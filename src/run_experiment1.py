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
  5. Measures and prints the throughput (keys per second) for the insert operations.
  6. Prints a standardized throughput line ("THROUGHPUT: <value>") for parsing by the connector.
  7. Terminates the chordify node process.
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
    start_time = time.time()
    total_keys = 0
    try:
        with open(insert_file, 'r') as f:
            for line in f:
                key = line.strip()
                if not key:
                    continue
                cmd = f"insert {key} {key}\n"
                send_command(proc, cmd)
                total_keys += 1
    except FileNotFoundError:
        print(f"Insert file {insert_file} not found!")
    end_time = time.time()
    duration = end_time - start_time
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
        # Wait until the bootstrap node prints its server info and then send join with -b.
        line = read_until(proc, "Server is up and running in")
        if line:
            m = re.search(r"Server is up and running in ([\\d\\.]+):(\\d+)", line)
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
        # For non-bootstrap nodes, wait a short delay then send join.
        time.sleep(1)
        send_command(proc, "join\n")

    # Allow time for the chord ring to stabilize.
    time.sleep(3)

    # Perform insert operations.
    insert_file = os.path.join("..", "insert", f"insert_{args.node_id:02d}_part.txt")
    total_keys, duration = run_inserts(proc, insert_file)
    if duration > 0:
        throughput = total_keys / duration
    else:
        throughput = float('inf')
    print(f"Node {args.node_id}: Inserted {total_keys} keys in {duration:.5f} seconds. Throughput: {throughput:.1f} keys/sec")
    
    # Print standardized throughput line for aggregation.
    print(f"THROUGHPUT: {throughput:.1f}")
    
    terminate_process(proc)

if __name__ == "__main__":
    main()
