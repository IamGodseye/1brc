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

We've tested two different approaches for processing the 130 MB file. Here are the results:

| Operation | Approach 1 (seconds) | Approach 2 (seconds) |
|-----------|----------------------|----------------------|
| Chunking | 1.36 | 1.44 |
| Mapping | 14.47 | 13.44 |
| Flattening | 2.05 | 1.41 |
| Partitioning | 21.68 | 25.35 |
| Reducing | 102.72 | 10.73 |
| Merging | 0.00 | 0.00 |
| Sorting | 0.00 | - |
| Writing | 0.09 | - |
| **Total execution time** | **142.37** | **52.78** |

File details:
- File size: 131.56 MB
- Number of chunks: 132

### Approach 1
This approach uses a more detailed breakdown of operations, including separate steps for sorting and writing results.

### Approach 2
This approach combines some operations and optimizes the reducing step, resulting in a significant performance improvement.

## Performance Tuning

Adjust `chunk_size` and `num_partitions` for optimization on different hardware. The second approach demonstrates that optimizing the reducing step can lead to substantial performance gains.
