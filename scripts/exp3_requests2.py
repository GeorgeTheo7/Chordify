import subprocess
import multiprocessing
import time
import shlex
import os

# List of request files (one per process)
request_files = [
    "requests_00.txt",
    "requests_01.txt",
    "requests_02.txt",
    "requests_03.txt",
    "requests_04.txt",
    "requests_05.txt",
    "requests_06.txt",
    "requests_07.txt",
    "requests_08.txt",
    "requests_09.txt",
]

# Corresponding nodes (one per process)
nodes = [
    "10.0.42.248:5000", "10.0.42.248:5001", "10.0.42.23:5000",
    "10.0.42.23:5001", "10.0.42.173:5000", "10.0.42.173:5001",
    "10.0.42.188:5000", "10.0.42.188:5001", "10.0.42.57:5000",
    "10.0.42.57:5001"
]

CLI_SCRIPT = "/home/ubuntu/Chordify/src/cli.py"  # Full path to cli.py
REQUESTS_DIR = "/home/ubuntu/Chordify/requests"  # Full path to the 'requests' directory

def execute_requests_from_file(filename, node):
    """Reads a request file and executes each query/insert sequentially, capturing outputs."""
    # Fix the file path to point to the 'requests' directory
    request_file_path = os.path.join(REQUESTS_DIR, filename)

    try:
        with open(request_file_path, 'r') as file:
            requests = [line.strip() for line in file if line.strip()]  # Read all non-empty lines

        if not requests:
            print(f"[{filename}] No requests found.")
            return

        print(f"[{filename} -> {node}] Processing {len(requests)} requests...")

        for request in requests:
            parts = request.split(", ")
            if len(parts) < 2:
                print(f"[{filename}] Invalid request format: {request}")
                continue

            action = parts[0].strip().lower()

            if action == "insert":
                if len(parts) < 3:
                    print(f"[{filename}] Insert detected with no value, defaulting value to 0.")
                    key = shlex.quote(parts[1])
                    value = '0'  # Default value if no value is provided
                else:
                    key = shlex.quote(parts[1])
                    value = shlex.quote(parts[2])

                command = f"python3 {CLI_SCRIPT} insert {key} {value}"

            elif action == "query":
                key = shlex.quote(parts[1])
                command = f"python3 {CLI_SCRIPT} query {key}"

            else:
                print(f"[{filename}] Unknown action: {action}")
                continue

            # Execute command and capture output
            process = subprocess.run(command, shell=True, capture_output=True, text=True)

            # Print the response for debugging
            print(f"[{filename} -> {node}] Command: {command}")
            if process.stdout.strip():
                print(f"[{filename} -> {node}] Output:\n{process.stdout.strip()}")
            if process.stderr.strip():
                print(f"[{filename} -> {node}] Error:\n{process.stderr.strip()}")

        print(f"[{filename} -> {node}] Completed processing requests.")

    except FileNotFoundError:
        print(f"[{filename}] Error: File not found at {request_file_path}")
    except Exception as e:
        print(f"[{filename}] Unexpected error: {e}")

if __name__ == "__main__":
    with multiprocessing.Pool(processes=10) as pool:
        pool.starmap(execute_requests_from_file, zip(request_files, nodes))

