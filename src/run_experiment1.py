import subprocess
import time
import re
import os
import signal
import argparse

def start_chord_node(k, consistency):
    cmd = ["python3", "chordify.py", str(k), consistency]
    proc = subprocess.Popen(
        cmd,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1,
        start_new_session=True
    )
    return proc

def read_until(proc, pattern, timeout=30):
    start_time = time.time()
    while True:
        if proc.stdout is None:
            break
        line = proc.stdout.readline()
        if line:
            print(f"[Chord node output] {line.strip()}")
            if pattern in line:
                return line
        if time.time() - start_time > timeout:
            break
    return None

def send_command(proc, command):
    if proc.stdin:
        proc.stdin.write(command)
        proc.stdin.flush()

def run_inserts(proc, insert_file, timeout=30):
    first_insertion_time = None
    last_insertion_time = None
    total_keys = 0
    try:
        with open(insert_file, 'r') as f:
            for line in f:
                key = line.strip()
                if not key:
                    continue
                # Record the start time at the first insertion.
                if first_insertion_time is None:
                    first_insertion_time = time.time()
                # Build and send the insert command.
                cmd = f"insert '{key}' '{key}'\n"
                send_command(proc, cmd)
                # Wait for the success message or error
                success = False
                start_wait = time.time()
                while not success and (time.time() - start_wait < timeout):
                    response_line = proc.stdout.readline().strip()
                    if response_line:
                        print(f"[Insert response] {response_line}")
                        if "Key inserted successfully." in response_line:
                            success = True
                            last_insertion_time = time.time()
                            total_keys += 1
                        elif "Error" in response_line:
                            print(f"Insert failed for key {key}")
                            break
                if not success:
                    print(f"Timeout waiting for insert confirmation for key {key}")
                    break
    except FileNotFoundError:
        print(f"Insert file {insert_file} not found!")
    return total_keys, first_insertion_time, last_insertion_time

def terminate_process(proc):
    try:
        os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
        proc.wait(timeout=5)
    except Exception as e:
        print(f"Error terminating process: {e}")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--node_id", type=int, required=True, help="Node identifier (0-9)")
    parser.add_argument("--k", type=int, required=True, help="Replication factor")
    parser.add_argument("--consistency", type=str, required=True, help="Consistency policy (chain-replication or eventual-consistency)")
    parser.add_argument("--bootstrap", action="store_true", help="Flag to indicate this is the bootstrap node")
    args = parser.parse_args()

    print(f"Starting run_experiment1.py for node {args.node_id} with k={args.k}, consistency={args.consistency}, bootstrap={args.bootstrap}")

    # Start chordify node.
    proc = start_chord_node(args.k, args.consistency)

    if args.bootstrap:
        line = read_until(proc, "Server is up and running in")
        if line:
            m = re.search(r"Server is up and running in ([\d\.]+):(\d+)", line)
            if m:
                bootstrap_ip = m.group(1)
                bootstrap_port = m.group(2)
                print(f"Bootstrap node info: {bootstrap_ip}:{bootstrap_port}")
                join_cmd = f"join -b {bootstrap_ip} {bootstrap_port}\n"
                send_command(proc, join_cmd)
            else:
                print("Failed to parse bootstrap node information.")
        else:
            print("Timeout waiting for bootstrap server info.")
    else:
        time.sleep(1)
        send_command(proc, "join\n")

    time.sleep(3)  # Allow time for the chord ring to stabilize.

    insert_file = os.path.join("..", "insert", f"insert_{args.node_id:02d}_part.txt")
    total_keys, first_time, last_time = run_inserts(proc, insert_file)
    
    if first_time is not None and last_time is not None:
        duration = last_time - first_time
        print(f"Node {args.node_id}: Inserted {total_keys} keys in {duration:.5f} seconds.")
        print(f"INSERTION_DURATION: {duration:.5f}")
        print(f"INSERTION_START: {first_time:.5f}")
        print(f"INSERTION_END: {last_time:.5f}")
    else:
        print("No keys were inserted.")

    terminate_process(proc)

if __name__ == "__main__":
    main()