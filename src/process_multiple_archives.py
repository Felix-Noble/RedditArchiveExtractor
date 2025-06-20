# process_multiple_archives.py

import os
import glob
import json
import logging
import argparse
import io
import zstandard as zst
import yaml
from tqdm import tqdm
import multiprocessing

# RE-INTRODUCED: The TqdmFileReader class to track byte-level progress.
class TqdmFileReader:
    """
    A wrapper for a file-like object that updates a tqdm progress bar
    as the file is read.
    """
    def __init__(self, file, pbar):
        self.file = file
        self.pbar = pbar

    def read(self, *args, **kwargs):
        chunk = self.file.read(*args, **kwargs)
        if chunk:
            self.pbar.update(len(chunk))
        return chunk

    def seekable(self):
        return self.file.seekable()

    def tell(self):
        return self.file.tell()

def setup_logging():
    """Sets up basic logging to the console."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - [%(levelname)s] - %(message)s",
    )

def load_config(config_path: str) -> dict:
    """Loads the YAML configuration file."""
    try:
        with open(config_path, "r") as f:
            config = yaml.safe_load(f)
        logging.info(f"Configuration loaded from {config_path}")
        return config
    except FileNotFoundError:
        logging.error(f"Configuration file not found at: {config_path}")
        exit(1)
    except yaml.YAMLError as e:
        logging.error(f"Error parsing YAML file: {e}")
        exit(1)

# def find_zst_files(directory: str) -> list[str]:
#     """Finds all .zst files in a directory, including subdirectories."""
#     if not os.path.isdir(directory):
#         logging.warning(f"Data directory not found: {directory}")
#         return []
    
#     search_path = os.path.join(directory, '**', '*.zst')
#     files = glob.glob(search_path, recursive=True)
#     logging.info(f"Found {len(files)} .zst files in {directory}")
#     return files


def find_zst_files(directory: str) -> list[str]:
    """
    Finds all .zst files in a directory, including subdirectories, recursively.

    Args:
        directory: The path to the directory to start searching from.

    Returns:
        A list of full paths to all found .zst files.
    """
    if not os.path.isdir(directory):
        logging.warning(f"Data directory not found: {directory}")
        return []

    # Construct the search path using '**' for recursive globbing
    # and '*.zst' to match files ending with .zst
    search_path = os.path.join(directory, '**', '*.zst')

    # Use glob.glob with recursive=True to find files in subdirectories
    files = glob.glob(search_path, recursive=True)

    logging.info(f"Found {len(files)} .zst files in {directory}")
    return files


def process_single_file(args):
    """
    Worker function to process one .zst file. It now creates its own
    progress bar in a managed position to show byte-level progress.
    """
    file_path, target_subreddits, batch_size, output_dir, worker_id = args
    
    base_name = os.path.basename(file_path)
    output_filename = os.path.splitext(base_name)[0] + '.jsonl'
    output_path = os.path.join(output_dir, output_filename)
    
    file_records_found = 0
    file_size_bytes = os.path.getsize(file_path)
    
    try:
        # Each worker creates its own progress bar, positioned by its ID.
        with tqdm(total=file_size_bytes, unit='B', unit_scale=True, desc=f"Worker {worker_id}: {base_name}", position=worker_id, leave=False) as pbar:
            with open(output_path, 'w', encoding='utf-8') as f_out:
                with open(file_path, 'rb') as f_in:
                    # Use the TqdmFileReader to link the file reading to the progress bar.
                    wrapped_reader = TqdmFileReader(f_in, pbar)
                    dctx = zst.ZstdDecompressor()
                    stream_reader = dctx.stream_reader(wrapped_reader)
                    text_stream = io.TextIOWrapper(stream_reader, encoding='utf-8')
                    
                    batch = []
                    for record_line in text_stream:
                        try:
                            record = json.loads(record_line)
                            if record.get('subreddit') in target_subreddits:
                                batch.append(json.dumps(record) + '\n')
                                file_records_found += 1
                                
                                if len(batch) >= batch_size:
                                    f_out.writelines(batch)
                                    batch.clear()
                        except (json.JSONDecodeError, AttributeError):
                            continue
                    
                    if batch:
                        f_out.writelines(batch)
    
    except (zst.ZstdError, IOError) as e:
        # On error, return the error message
        return None, f"Error processing {file_path}: {e}"

    return output_path, file_records_found


def main():
    """Main function to run the data processing pipeline."""
    parser = argparse.ArgumentParser(description="A simple pipeline to process Reddit .zst archives.")
    parser.add_argument("--config", type=str, default="config.yaml", help="Path to the YAML configuration file.")
    args = parser.parse_args()

    setup_logging()
    config = load_config(args.config)

    data_dir = config.get("paths", {}).get("data_directory")
    output_dir = config.get("paths", {}).get("output_directory")
    target_subreddits = set(config.get("filter", {}).get("target_subreddits", []))
    batch_size = config.get("processing", {}).get("batch_size", 1000)
    num_workers = config.get("processing", {}).get("num_workers", os.cpu_count() // 2)

    if not all([data_dir, output_dir]):
        logging.error("Config missing 'data_directory' or 'output_directory'.")
        return
    if not target_subreddits:
        logging.warning("No 'target_subreddits' defined in config. No data will be filtered.")
        return

    os.makedirs(output_dir, exist_ok=True)

    zst_files = find_zst_files(data_dir)
    if not zst_files:
        logging.warning("No .zst files to process. Exiting.")
        return
        
    if zst_files and len(zst_files) < num_workers:
        logging.info(f"Found {len(zst_files)} files, which is less than the configured {num_workers} workers. Adjusting worker count.")
        num_workers = len(zst_files)
        
    tasks = [(file_path, target_subreddits, batch_size, output_dir, i) for i, file_path in enumerate(zst_files)]

    logging.info(f"Starting processing pool with {num_workers} workers.")
    with multiprocessing.Pool(processes=num_workers) as pool:
        # The overall progress bar is now gone, but we can still track completion.
        # We wrap the pool.imap_unordered call in a simple tqdm to see files completing.
        results = list(tqdm(pool.imap_unordered(process_single_file, tasks), total=len(tasks), desc="Total Files Processed"))

    total_records_found = 0
    successful_files = 0
    for output_path, records_found in results:
        if output_path:
            # We don't need to log success here as the progress bar shows completion.
            total_records_found += records_found
            successful_files += 1
        else:
            error_message = records_found 
            logging.error(error_message)

    logging.info(f"\nProcessing complete. {successful_files}/{len(zst_files)} files processed successfully.")
    logging.info(f"Total matching records found across all files: {total_records_found}")

if __name__ == "__main__":
    main()
