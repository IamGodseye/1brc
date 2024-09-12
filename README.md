# Weather Station Data Processor

A Python script for processing large weather station data files using multiprocessing.

## Features

- Processes large CSV files
- Uses multiprocessing for parallel data processing
- Calculates min, max, and average temperatures for each station

## Usage

1. Ensure Python 3.6+ is installed
2. Place your input CSV file in the same directory as the script
3. Update the `file_path` variable in `main()` with your input file name
4. Run: `python script.py`
5. Find results in `sorted_data.txt`

## Benchmark

Results for a 130 MB file:

- File size: 131.56 MB
- Number of chunks: 132
- Total execution time: 142.37 seconds

Breakdown:
- Chunking time: 1.36 seconds
- Mapping time: 14.47 seconds
- Flattening time: 2.05 seconds
- Partitioning time: 21.68 seconds
- Reducing time: 102.72 seconds
- Merging time: 0.00 seconds
- Sorting time: 0.00 seconds
- Writing time: 0.09 seconds

## Performance Tuning

Adjust `chunk_size` and `num_partitions` for optimization on different hardware.
