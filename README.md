# Reddit Archive Extractor
This tool offers fast, low-memory decompression of .zst archives with progress tracking bars for each file. 

Paralell option included (testing incomplete)
# Requirements
Python 3.7+ and the following libraries: zstandard, PyYAML (config), tqdm (progress bars)

# Setup
Install Libraries:
~~~
pip install zstandard pyyaml tqdm
~~~

# File Structure:
Create a project directory with the following structure./your_project_folder/
~~~
|-- process_archives.py     # The main script
|-- config.yaml             # The configuration file
|-- data/                   # A folder to hold your archives (can be external to project directory)
|  |-- RS_2022-01.zst
|  |-- RS_2022-02.zst
|   `-- ... (and so on)
|   `-- output/          # The script will save output here
~~~
(You can name the data, and output folders anything you like, as long as you update the paths in config.yaml.)

# Configuration
All script settings are controlled by the config.yaml file.# config.yaml
~~~
paths:
  # The directory where your .zst files are stored
  data_directory: ./data/raw
  
  # The directory where the filtered output file will be saved.
  output_directory: ./data/processed

filter:
  # A list of subreddit names to extract. 
  # This is case-sensitive.
  target_subreddits:
    - AskReddit
    - science
    - python

processing:
  # The number of records to hold in memory before writing to a file.
  batch_size: 1000
 ~~~
# Run from the Command Line
Once the config.yaml file is saved, open your terminal and run the following command:
~~~
python process_archives.py
~~~
The script will find all .zst files in your data_directory, process them in parallel, and save the filtered output as .jsonl files in your output_directory.

(Optional) Specify a different config fileIf your config file is not named config.yaml or is in another directory, use the --config flag to point to it:
~~~
python process_archives.py --config /path/to/your/settings.yaml
~~~

Or, to process several archives in paralell
~~~
python process_multiple_archives.py --config /path/to/your/settings.yaml
~~~
