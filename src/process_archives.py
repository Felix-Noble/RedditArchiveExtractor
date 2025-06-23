# process_archives.py

import os
import glob
import json
import logging
import argparse
import io
import zstandard as zst
import yaml
from tqdm import tqdm
from pathlib import Path

# NEW: A helper class to wrap a file object and update the progress bar on each read.
class TqdmFileReader:
    """
    A wrapper for a file-like object that updates a tqdm progress bar
    as the file is read.
    """
    def __init__(self, file, pbar):
        self.file = file
        self.pbar = pbar

    def read(self, *args, **kwargs):
        # Read a chunk of data from the underlying file
        chunk = self.file.read(*args, **kwargs)
        if chunk:
            # Update the progress bar by the number of bytes read
            self.pbar.update(len(chunk))
        return chunk

    # These methods are needed for compatibility with zstandard's stream reader
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

def find_zst_files(directory: str) -> list[str]:
    """Finds all .zst files in a directory, including subdirectories."""
    if not os.path.isdir(directory):
        logging.warning(f"Data directory not found: {directory}")
        return []
    
    search_path = os.path.join(directory, '**', '*.zst')
    files = glob.glob(search_path, recursive=True)
    logging.info(f"Found {len(files)} .zst files in {directory}")
    return files

# REMOVED: The stream_zst_records function is no longer needed, as its
# logic is now handled directly inside process_and_filter_files.

def process_and_filter_files(
    file_paths: list[str],
    target_subreddits: set[str],
    output_dir: Path,
    batch_size: int
):
    """
    Processes a list of .zst files, filters for target subreddits,
    and saves the matching records to a single output file.
    """
    
    for file in file_paths:
        base_name, _ = os.path.splitext(os.path.basename(file))
        output = output_dir / (base_name + ".jsonl")
        print(output)
        print("FFFFF")
        os.makedirs(os.path.dirname(output), exist_ok=True)
        total_records_found = 0

        with open(output, 'w', encoding='utf-8') as f_out:
            logging.info(f"Starting to process: {file}")
            
            # Get the compressed file size for the progress bar total
            file_size_bytes = os.path.getsize(file)
            file_records_found = 0
            
            # Set up the progress bar to track bytes
            with tqdm(total=file_size_bytes, unit='B', unit_scale=True, desc=f"Processing {os.path.basename(file)}") as pbar:
                try:
                    with open(file, 'rb') as f_in:
                        # Wrap the binary file reader with our TqdmFileReader
                        wrapped_reader = TqdmFileReader(f_in, pbar)
                        
                        # Decompress the stream from the wrapped reader
                        dctx = zst.ZstdDecompressor()
                        stream_reader = dctx.stream_reader(wrapped_reader)
                        text_stream = io.TextIOWrapper(stream_reader, encoding='utf-8')
                        
                        batch = []
                        # Iterate through the decompressed lines
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
                        
                        # Write any remaining records
                        if batch:
                            f_out.writelines(batch)
                            batch.clear()

                except (zst.ZstdError, IOError) as e:
                    logging.error(f"Error processing {file}: {e}")
                    continue # Skip to the next file

            total_records_found += file_records_found
            logging.info(f"Finished processing {file}. Found {file_records_found} matching records.")

    logging.info(f"All files processed. Total matching records found: {total_records_found}")
    logging.info(f"Filtered data saved to: {output}")


def main():
    """Main function to run the data processing pipeline."""
    parser = argparse.ArgumentParser(
        description="A simple pipeline to process Reddit .zst archives."
    )
    parser.add_argument(
        "--config",
        type=str,
        default="config.yaml",
        help="Path to the YAML configuration file.",
    )
    args = parser.parse_args()

    setup_logging()
    config = load_config(args.config)

    data_dir = config.get("paths", {}).get("data_directory")
    output_dir = Path(config.get("paths", {}).get("output_directory"))
    target_subreddits = set(config.get("filter", {}).get("target_subreddits", []))
    batch_size = config.get("processing", {}).get("batch_size", 1000)

    if not all([data_dir, output_dir]):
        logging.error("Config missing 'data_directory' or 'output_directory'.")
        return
    if not target_subreddits:
        logging.warning("No 'target_subreddits' defined in config. No data will be filtered.")
        return


    zst_files = find_zst_files(data_dir)

    if zst_files:
        process_and_filter_files(zst_files, target_subreddits, output_dir, batch_size)
    else:
        logging.warning("No .zst files to process. Exiting.")

if __name__ == "__main__":
    main()
