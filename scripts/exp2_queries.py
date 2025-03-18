#!/usr/bin/env python3

import subprocess
import multiprocessing
import time
import shlex

# List of query command files (one per process)
query_files = [
    "query_00.txt",
    "query_01.txt",
    "query_02.txt",
    "query_03.txt",
    "query_04.txt",
    "query_05.txt",
    "query_06.txt",
    "query_07.txt",
    "query_08.txt",
    "query_09.txt",
]

# Corresponding nodes (one per process)
nodes = [
    "10.0.39.177:5050", "10.0.39.155:5050", "10.0.39.191:5050",
    "10.0.39.19:5050", "10.0.39.87:5050", "10.0.39.177:5051",
    "10.0.39.155:5051",  "10.0.39.191:5051",  "10.0.39.19:5051",
    "10.0.39.87:5051"
]

def execute_queries_from_file(filename, node):
    """Reads a file and executes each query, measuring execution time and throughput."""
    try:
        with open("~/Chordify/queries/" + filename, 'r') as file:
            queries = [line.strip() for line in file if line.strip()]  # Read all non-empty queries

        total_queries = len(queries)
        if total_queries == 0:
            print(f"[{filename}] No queries found.")
            return filename, node, 0, 0  # Return zero throughput if no queries exist

        start_time = time.time()  # Start timing
        for query in queries:
            full_command = f'python ~/Chordify/src/cli.py query {shlex.quote(query)}'
            process = subprocess.run(full_command, shell=True, capture_output=True, text=True)
            print(full_command)
            if process.stdout:
                print(f"[{filename} -> {node}] Output:\n{process.stdout}")
            if process.stderr:
                print(f"[{filename} -> {node}] Error:\n{process.stderr}")

        end_time = time.time()  # End timing
        elapsed_time = end_time - start_time
        throughput = total_queries / elapsed_time if elapsed_time > 0 else 0  # Avoid division by zero

        print(f"[{filename} -> {node}] Completed in {elapsed_time:.2f} seconds, Throughput: {throughput:.2f} queries/sec")

        return filename, node, elapsed_time, throughput  # Return results

    except FileNotFoundError:
        print(f"[{filename}] Error: File not found.")
        return filename, node, 0, 0  # Return zero throughput in case of file errors
    except Exception as e:
        print(f"[{filename}] Unexpected error: {e}")
        return filename, node, 0, 0  # Return zero throughput in case of other errors

if __name__ == "__main__":
    with multiprocessing.Pool(processes=10) as pool:
        results = pool.starmap(execute_queries_from_file, zip(query_files, nodes))

    # Print summary results
    print("\n======== FINAL RESULTS ========")
    for filename, node, time_taken, throughput in results:
        print(f"[{filename} -- {node}] Time: {time_taken:.2f}s | Throughput: {throughput:.2f} queries/sec")
