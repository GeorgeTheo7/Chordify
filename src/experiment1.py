#!/usr/bin/env python3
"""
experiment.py

This script runs 6 experiments on a chord DHT:
  - Replication factors: k = 1 (no replication), 3, and 5.
  - Consistency policies: chain-replication (linearizability) and eventual-consistency.
  
For each experiment, it:
  1. Starts 10 nodes by launching chordify.py as separate processes.
     - The first node is the bootstrap node. After it starts,
       we wait until it prints its server info then send the command:
           join -b {bootstrap_ip} {bootstrap_port}
     - The other nodes join by sending "join" to their stdin.
  2. Waits a few seconds for the network to stabilize.
  3. From each node, concurrently reads its corresponding insert file (located in the "insert" folder)
     and issues “insert <key> <key>” commands (one per key).
  4. Measures the total time to insert all keys and calculates the average time per key.
  5. Terminates the chordify nodes before moving to the next experiment.

Note: This script assumes that:
   - chordify.py, server.py, cli.py, node.py, and experiment.py are in the src folder.
   - The insert files are in the subfolder "insert" and named "insert_00_part.txt", …, "insert_09_part.txt".
   - The chordify.py server prints a line like "Server is up and running in <ip>:<port> !" to stdout.
"""

import subprocess
import threading
import time
import re
import os
import signal
import sys

def start_chord_node(node_id, k, consistency):
    """
    Launch chordify.py as a separate process.
    For k==1 the consistency argument is ignored by chordify.py,
    but we still pass it for uniformity.
    """
    # Build command: for k==1 chordify.py expects one arg, but extra arg will be ignored.
    cmd = ["python", "chordify.py", str(k), consistency]
    proc = subprocess.Popen(
        cmd,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1,
        start_new_session=True
    )

    print(f"Node {node_id} started with command: {' '.join(cmd)}")
    return proc

def read_until(proc, pattern, timeout=30):
    """
    Reads lines from the process stdout until a line containing `pattern` is found
    or until timeout (in seconds) is reached.
    """
    start_time = time.time()
    while True:
        if proc.stdout is None:
            break
        line = proc.stdout.readline()
        if line:
            # Optionally, you can print or log the line.
            # print(line.strip())
            if pattern in line:
                return line
        if time.time() - start_time > timeout:
            break
    return None

def send_command(proc, command):
    """Send a command (with newline) to the process's stdin."""
    if proc.stdin:
        proc.stdin.write(command)
        proc.stdin.flush()

def run_inserts(proc, filename, results):
    """
    Opens the given insert file and for each nonempty line sends an
    "insert <key> <key>" command to the given chord node.
    Records the duration for sending all commands from that file.
    """
    start = time.time()
    try:
        with open(filename, 'r') as f:
            for line in f:
                key = line.strip()
                if not key:
                    continue
                # Insert command: using the song as both key and value.
                cmd = f"insert {key} {key}\n"
                send_command(proc, cmd)
                # Optionally, read the response from stdout if needed.
    except FileNotFoundError:
        print(f"File {filename} not found!")
    end = time.time()
    duration = end - start
    results.append((filename, duration))

def terminate_processes(procs):
    for p in procs:
        try:
            os.killpg(os.getpgid(p.pid), signal.SIGTERM)
            p.wait(timeout=5)
        except Exception as e:
            print(f"Error terminating process: {e}")

def main():
    # Define the experiments:
    replication_factors = [1, 3, 5]
    consistency_options = ["chain-replication", "eventual-consistency"]
    throughput_results = {}  # key: (k, consistency) -> avg time per key

    # Loop over each experiment configuration.
    for k in replication_factors:
        for cons in consistency_options:
            print("\n============================================")
            print(f"Running experiment with k = {k} and consistency = {cons}")
            procs = []

            # --- Start the bootstrap node (node 0) ---
            bootstrap_proc = start_chord_node(0, k, cons)
            procs.append(bootstrap_proc)

            # Wait until the bootstrap node prints its server info.
            bootstrap_line = read_until(bootstrap_proc, "Server is up and running in")
            if bootstrap_line is None:
                print("Timeout waiting for bootstrap node to start.")
                terminate_processes(procs)
                continue

            # Parse IP and port from the printed line.
            m = re.search(r"Server is up and running in ([\d\.]+):(\d+)", bootstrap_line)
            if m:
                bootstrap_ip = m.group(1)
                bootstrap_port = m.group(2)
                print(f"Bootstrap node info: {bootstrap_ip}:{bootstrap_port}")
                # The bootstrap node joins itself using the join command with -b.
                join_cmd = f"join -b {bootstrap_ip} {bootstrap_port}\n"
                send_command(bootstrap_proc, join_cmd)
            else:
                print("Could not parse bootstrap node information.")
                terminate_processes(procs)
                continue

            # --- Start the remaining 9 nodes ---
            for i in range(1, 10):
                proc = start_chord_node(i, k, cons)
                procs.append(proc)
                # Give a short delay to avoid race conditions.
                time.sleep(0.5)
                # The non-bootstrap nodes simply issue "join" to join the chord.
                send_command(proc, "join\n")

            # Allow time for the chord network to stabilize.
            time.sleep(3)

            # --- Concurrently perform insert operations ---
            threads = []
            insert_results = []  # List to record per-node insert durations.
            total_keys = 0

            # Count total keys from all files.
            for i in range(10):
                fname = os.path.join("../insert", f"insert_{i:02d}_part.txt")
                try:
                    with open(fname, 'r') as f:
                        keys = [line.strip() for line in f if line.strip()]
                        total_keys += len(keys)
                except FileNotFoundError:
                    print(f"Insert file {fname} not found!")
            
            print(f"Total keys to insert: {total_keys}")
            start_inserts = time.time()
            # Start a thread per node to run its inserts.
            for i, proc in enumerate(procs):
                fname = os.path.join("../insert", f"insert_{i:02d}_part.txt")
                t = threading.Thread(target=run_inserts, args=(proc, fname, insert_results))
                t.start()
                threads.append(t)
            
            # Wait for all insert threads to finish.
            for t in threads:
                t.join()
            end_inserts = time.time()
            total_time = end_inserts - start_inserts
            
            if total_keys > 0:
                avg_time_per_key = total_keys / total_time
            else:
                avg_time_per_key = float('inf')
            
            throughput_results[(k, cons)] = avg_time_per_key
            print(f"Insertion complete: {total_keys} keys in {total_time:.5f} seconds")
            print(f"Average number of keys per second: {avg_time_per_key:.1f} keys/sec")
            
            # Terminate all chord node processes.
            terminate_processes(procs)
            # Pause a bit before the next experiment.
            time.sleep(2)

    # --- Final results ---
    print("\n=========== Experiment Results ===========")
    for (k, cons), avg in throughput_results.items():
        print(f"Replication factor k={k}, consistency={cons}: {avg:.1f} key/sec")

if __name__ == "__main__":
    main()
