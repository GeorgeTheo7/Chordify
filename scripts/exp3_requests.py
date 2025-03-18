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
    "10.0.39.177:5050", "10.0.39.155:5050", "10.0.39.191:5050",
    "10.0.39.19:5050", "10.0.39.87:5050", "10.0.39.177:5051",
    "10.0.39.155:5051",  "10.0.39.191:5051",  "10.0.39.19:5051",
    "10.0.39.87:5051"
]

def execute_requests_from_file(filename, node):
    """Reads a request file and executes each query/insert sequentially, capturing outputs."""
    file_path = os.path.join(os.path.expanduser("~/Chordify/requests"), filename)
    try:
        with open(file_path, 'r') as file:
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
                    print(f"[{filename}] Invalid insert format: {request}")
                    continue
                key = shlex.quote(parts[1])
                value = shlex.quote(parts[2])
                command = f'python ~/Chordify/src/cli.py insert "{command}" "{node}"'

            elif action == "query":
                key = shlex.quote(parts[1])
                command = f'python ~/Chordify/src/cli.py query {key}'

            else:
                print(f"[{filename}] Unknown action: {action}")
                continue

            # Execute command and capture output
            process = subprocess.run(command, shell=True, capture_output=True, text=True)

            # Print the response for debugging
            print(f"[{filename} -> {node}] Command: {command}")
            if process.stdout:
                print(f"[{filename} -> {node}] Output:\n{process.stdout.strip()}")
            if process.stderr:
                print(f"[{filename} -> {node}] Error:\n{process.stderr.strip()}")

        print(f"[{filename} -> {node}] Completed processing requests.")

    except FileNotFoundError:
        print(f"[{filename}] Error: File not found.")
    except Exception as e:
        print(f"[{filename}] Unexpected error: {e}")

if __name__ == "__main__":
    with multiprocessing.Pool(processes=10) as pool:
        pool.starmap(execute_requests_from_file, zip(request_files, nodes))
