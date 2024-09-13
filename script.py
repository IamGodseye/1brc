import itertools
import multiprocessing
from typing import List, Tuple, Dict
import os
import tempfile

def read_chunk(file_path: str, start: int, size: int) -> List[str]:
    with open(file_path, 'r') as f:
        f.seek(start)
        chunk = f.read(size).splitlines()
    return chunk

def map_function(chunk: List[str]) -> List[Tuple[str, float]]:
    result = []
    for line in chunk:
        if line.strip():  # Check if the line is not empty
            try:
                station, temp = line.split(';')
                result.append((station, float(temp)))
            except (ValueError, IndexError):
                # Skip lines that can't be properly parsed
                continue
    return result

def partition_function(mapped_data: List[Tuple[str, float]]) -> Dict[str, List[float]]:
    partitions = {}
    for station, temp in mapped_data:
        if station not in partitions:
            partitions[station] = []
        partitions[station].append(temp)
    return partitions

def reduce_function(station: str, temperatures: List[float]) -> Tuple[str, float, float, float]:
    min_temp = min(temperatures)
    max_temp = max(temperatures)
    avg_temp = sum(temperatures) / len(temperatures)

    return (station, min_temp, max_temp, avg_temp)

def hash_partition_function(mapped_data: List[Tuple[str, float]], num_partitions: int) -> List[List[Tuple[str, float]]]:
    partitions = [[] for _ in range(num_partitions)]
    for station, temp in mapped_data:
        partition_index = hash(station) % num_partitions
        partitions[partition_index].append((station, temp))
    return partitions

def write_partition_to_disk(partition, partition_id):
    with tempfile.NamedTemporaryFile(mode='w', delete=False, prefix=f'partition_{partition_id}_', suffix='.txt') as temp_file:
        for station, temp in partition:
            temp_file.write(f"{station};{temp}\n")
    return temp_file.name

def read_partition_from_disk(file_path):
    with open(file_path, 'r') as f:
        for line in f:
            station, temp = line.strip().split(';')
            yield station, float(temp)

def process_partition(partition_file):
    data = read_partition_from_disk(partition_file)
    results = {station: reduce_function(station, [temp for _, temp in group])
               for station, group in itertools.groupby(sorted(data), key=lambda x: x[0])}
    
    # Write results to a temporary file
    with tempfile.NamedTemporaryFile(mode='w', delete=False, prefix='result_', suffix='.txt') as temp_file:
        for station, (_, min_temp, max_temp, avg_temp) in results.items():
            temp_file.write(f"{station};{min_temp:.1f};{max_temp:.1f};{avg_temp:.1f}\n")
    
    return temp_file.name

import time

def main():
    file_path = 'long_data.csv'  # Replace with your actual file path
    chunk_size = 1024 * 1024  # 1 MB chunks

    start_time = time.time()

    with open(file_path, 'r') as f:
        file_size = os.path.getsize(file_path)
        num_chunks = (file_size + chunk_size - 1) // chunk_size
        print(f"File size: {file_size / (1024 * 1024):.2f} MB, Number of chunks: {num_chunks}")

        with multiprocessing.Pool() as pool:
            # Chunk data into 1 MB chunks
            chunk_start = time.time()
            chunks = [read_chunk(file_path, i * chunk_size, chunk_size) for i in range(num_chunks)]
            print(f"Chunking time: {time.time() - chunk_start:.2f} seconds")
            
            # Map each chunk to a list of (station, temperature) tuples
            map_start = time.time()
            mapped_data = pool.map(map_function, chunks)
            print(f"Mapping time: {time.time() - map_start:.2f} seconds")
            
            # Flatten the mapped data
            flatten_start = time.time()
            flattened_data = [item for sublist in mapped_data for item in sublist]
            print(f"Flattening time: {time.time() - flatten_start:.2f} seconds")
            
            # Use hash_partition_function to distribute data across partitions and write to disk
            partition_start = time.time()
            num_partitions = multiprocessing.cpu_count()
            partitioned_data = hash_partition_function(flattened_data, num_partitions)
            partition_files = [write_partition_to_disk(partition, i) for i, partition in enumerate(partitioned_data)]
            print(f"Partitioning and writing to disk time: {time.time() - partition_start:.2f} seconds")
            
            # Apply reduce_function to each partition file
            reduce_start = time.time()
            result_files = pool.map(process_partition, partition_files)
            print(f"Reducing time: {time.time() - reduce_start:.2f} seconds")
            
            # Merge the results from all partitions
            merge_start = time.time()
            with open('sorted_data.txt', 'w') as output_file:
                for result_file in result_files:
                    with open(result_file, 'r') as f:
                        output_file.write(f.read())
                    os.unlink(result_file)  # Delete the temporary file
            print(f"Merging time: {time.time() - merge_start:.2f} seconds")

            # Clean up partition files
            for file in partition_files:
                os.unlink(file)

    end_time = time.time()
    print(f"Total execution time: {end_time - start_time:.2f} seconds")

if __name__ == "__main__":
    main()