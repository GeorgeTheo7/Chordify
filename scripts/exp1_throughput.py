import subprocess
import multiprocessing
import time
import os

# List of command files (one per process)
command_files = [
    "insert_00_part.txt",
    "insert_01_part.txt",
    "insert_02_part.txt",
    "insert_03_part.txt",
    "insert_04_part.txt",
    "insert_05_part.txt",
    "insert_06_part.txt",
    "insert_07_part.txt",
    "insert_08_part.txt",
    "insert_09_part.txt",
]

# Corresponding nodes (one per process)
nodes = [
    "10.0.39.177:5050", "10.0.39.155:5050", "10.0.39.191:5050",
    "10.0.39.19:5050", "10.0.39.87:5050", "10.0.39.177:5051",
    "10.0.39.155:5051",  "10.0.39.191:5051",  "10.0.39.19:5051",
    "10.0.39.87:5051"
]

# Base directory for command files
base_dir = os.path.expanduser("~/Chordify/insert")

def execute_commands_from_file(filename, node):
    """Reads a file and executes each command, measuring execution time and throughput."""
    file_path = os.path.join(base_dir, filename)
    try:
        with open(file_path, 'r') as file:
            # Read all non-empty lines
            commands = [line.strip() for line in file if line.strip()]

        total_commands = len(commands)
        if total_commands == 0:
            print(f"[{filename}] No commands found.")
            return filename, node, 0, 0  # Return zero throughput if no commands exist

        start_time = time.time()  # Start timing

        for command in commands:
            # Build the full command string.
            full_command = f'python ~/Chordify/src/cli.py insert "{command}" "{node}"'
            process = subprocess.run(full_command, shell=True, capture_output=True, text=True)
            print(full_command)
            if process.stdout:
                print(f"[{filename} -> {node}] Output:\n{process.stdout}")
            if process.stderr:
                print(f"[{filename} -> {node}] Error:\n{process.stderr}")

        end_time = time.time()  # End timing
        elapsed_time = end_time - start_time
        throughput = total_commands / elapsed_time if elapsed_time > 0 else 0  # Avoid division by zero

        print(f"[{filename} -> {node}] Completed in {elapsed_time:.2f} seconds, Throughput: {throughput:.2f} cmds/sec")

        return filename, node, elapsed_time, throughput  # Return results

    except FileNotFoundError:
        print(f"[{filename}] Error: File not found at {file_path}.")
        return filename, node, 0, 0  # Return zero throughput in case of file errors
    except Exception as e:
        print(f"[{filename}] Unexpected error: {e}")
        return filename, node, 0, 0  # Return zero throughput in case of other errors

if __name__ == "__main__":
    # Create a pool of 10 processes and run the commands concurrently.
    with multiprocessing.Pool(processes=10) as pool:
        results = pool.starmap(execute_commands_from_file, zip(command_files, nodes))

    # Print summary results
    print("\n======== FINAL RESULTS ========")
    for filename, node, time_taken, throughput in results:
        print(f"[{filename} -- {node}] Time: {time_taken:.2f}s | Throughput: {throughput:.2f} keys/sec")

