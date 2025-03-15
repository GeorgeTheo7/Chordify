#!/usr/bin/env python3
"""
run_experiment1.py

This script is executed on a VM to run the experiment for a single chord node.
It performs the following steps:
  1. Starts the chordify node (by running chordify.py as a subprocess).
  2. For the bootstrap node (if run with --bootstrap) waits until the node prints its server info,
     then sends "join -b {ip} {port}". For other nodes, it sends "join" after a short delay.
  3. Waits for the chord network to stabilize.
  4. Reads its corresponding insert file (../insert/insert_XX_part.txt, with XX matching the node id)
     and issues "insert <key> <key>" commands.
  5. For each insert, waits until a response indicating a successful insertion is received before issuing the next.
  6. Records the time of the first and the last insertion command.
  7. Computes the duration between the 50th insertion and the 1st insertion.
  8. Prints a standardized insertion duration line ("INSERTION_DURATION: <value>") for parsing by the connector.
  9. Terminates the chordify node process.
"""

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

def run_inserts(proc, insert_file):
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
                cmd = f"insert {key} {key}\n"
                send_command(proc, cmd)
                # Wait for the response that confirms the insert happened.
                response_line = proc.stdout.readline().strip()
                print(f"[Insert response] {response_line}")
                total_keys += 1
                last_insertion_time = time.time()
    except FileNotFoundError:
        print(f"Insert file {insert_file} not found!")
    if first_insertion_time is None or last_insertion_time is None:
        duration = 0
    else:
        duration = last_insertion_time - first_insertion_time
    return total_keys, duration

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
    total_keys, duration = run_inserts(proc, insert_file)
    print(f"Node {args.node_id}: Inserted {total_keys} keys in {duration:.5f} seconds.")
    print(f"INSERTION_DURATION: {duration:.5f}")

    terminate_process(proc)

if __name__ == "__main__":
    main()
