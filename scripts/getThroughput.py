import subprocess
import multiprocessing
import time

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
localhostNodes = [
    "127.0.0.1:5000", "127.0.0.1:5001", "127.0.0.1:5002",
    "127.0.0.1:5005", "127.0.0.1:5004", "127.0.0.1:5003",
    "127.0.0.1:5006", "127.0.0.1:5007", "127.0.0.1:5008",
    "127.0.0.1:5009"
]
nodes = ["10.0.39.177:5000", "10.0.39.177:5001", "10.0.39.155:5000", "10.0.39.155:5001", "10.0.39.191:5000", "10.0.39.191:5001", "10.0.39.19:5000", "10.0.39.19:5001" ,"10.0.39.87:5000", "10.0.39.87:5001"]
    


def execute_commands_from_file(filename, node):
    """Reads a file and executes each command, measuring execution time and throughput."""
    try:
        with open("inserts/" + filename, 'r') as file:
            commands = [line for line in file if line.strip()]  # Read all non-empty commands

        total_commands = len(commands)
        if total_commands == 0:
            print(f"[{filename}] No commands found.")
            return filename, node, 0, 0  # Return zero throughput if no commands exist

        start_time = time.time()  # Start timing

        for command in commands:
            full_command = f" python cli.py insert \"{command}\" value {node}"
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
        print(f"[{filename}] Error: File not found.")
        return filename, node, 0, 0  # Return zero throughput in case of file errors
    except Exception as e:
        print(f"[{filename}] Unexpected error: {e}")
        return filename, node, 0, 0  # Return zero throughput in case of other errors

if __name__ == "__main__":
    with multiprocessing.Pool(processes=10) as pool:
        results = pool.starmap(execute_commands_from_file, zip(command_files, nodes))

    # Print summary results
    print("\n===== Execution Summary =====")
    for filename, node, time_taken, throughput in results:
        print(f"[{filename} -> {node}] Time: {time_taken:.2f}s | Throughput: {throughput:.2f} cmds/sec")