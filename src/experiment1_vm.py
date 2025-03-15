#!/usr/bin/env python3
"""
Modified experiment.py for distributed execution on remote VMs.

We now run chordify nodes on remote machines via ssh.
The available VMs are accessed using:
  team_32-vm1, team_32-vm2, team_32-vm3, team_32-vm4, team_32-vm5, team_32-vm6

Nodes (10 in total) are distributed round‑robin over these hosts.
"""

import subprocess
import threading
import time
import re
import os
import signal
import sys

# List of remote VM hostnames.
vm_hosts = [
    "team_32-vm1",  # Bootstrap node
    "team_32-vm2",
    "team_32-vm3",
    "team_32-vm4",
    "team_32-vm5",
    "team_32-vm6"
]

def start_chord_node(node_id, k, consistency, vm_host):
    """
    Launch chordify.py as a remote process using SSH.
    Assumes that chordify.py is available on the remote machine (in the expected location).
    """
    # Build an SSH command to run chordify.py remotely.
    cmd = ["ssh", vm_host, "python3", "chordify.py", str(k), consistency]
    proc = subprocess.Popen(
        cmd,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1,
        start_new_session=True
    )
    print(f"Node {node_id} started on {vm_host} with command: {' '.join(cmd)}")
    return proc

def read_until(proc, pattern, timeout=30):
    """
    Read lines from the process stdout until a line containing `pattern` is found,
    or until timeout (in seconds) is reached.
    """
    start_time = time.time()
    while True:
        if proc.stdout is None:
            break
        line = proc.stdout.readline()
        if line:
            print(f"[Node output]: {line.strip()}")
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
    Open the given insert file and for each nonempty line send an
    "insert <key> <key>" command to the given chord node.
    Record the duration for sending all commands from that file.
    """
    start = time.time()
    try:
        with open(filename, 'r') as f:
            for line in f:
                key = line.strip()
                if not key:
                    continue
                # Use the key as both key and value.
                cmd = f"insert {key} {key}\n"
                send_command(proc, cmd)
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
    # Define the experiments.
    replication_factors = [1, 3, 5]
    consistency_options = ["chain-replication", "eventual-consistency"]
    throughput_results = {}  # key: (k, consistency) -> avg keys/sec

    for k in replication_factors:
        for cons in consistency_options:
            print("\n============================================")
            print(f"Running experiment with k = {k} and consistency = {cons}")
            procs = []

            # --- Start the bootstrap node (node 0) on team_32-vm1 ---
            bootstrap_host = vm_hosts[0]
            bootstrap_proc = start_chord_node(0, k, cons, bootstrap_host)
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
                # Bootstrap node joins itself with the join -b command.
                join_cmd = f"join -b {bootstrap_ip} {bootstrap_port}\n"
                send_command(bootstrap_proc, join_cmd)
            else:
                print("Could not parse bootstrap node information.")
                terminate_processes(procs)
                continue

            # --- Start the remaining 9 nodes ---
            for i in range(1, 10):
                # Distribute nodes round-robin among the available hosts.
                vm_host = vm_hosts[i % len(vm_hosts)]
                proc = start_chord_node(i, k, cons, vm_host)
                procs.append(proc)
                time.sleep(0.5)
                send_command(proc, "join\n")

            # Allow time for the chord network to stabilize.
            time.sleep(3)

            # --- Concurrently perform insert operations ---
            threads = []
            insert_results = []
            total_keys = 0

            # Count total keys from all insert files.
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
            for i, proc in enumerate(procs):
                fname = os.path.join("../insert", f"insert_{i:02d}_part.txt")
                t = threading.Thread(target=run_inserts, args=(proc, fname, insert_results))
                t.start()
                threads.append(t)
            
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
            print(f"Average keys per second: {avg_time_per_key:.1f}")
            
            terminate_processes(procs)
            time.sleep(2)

    # --- Final results ---
    print("\n=========== Experiment Results ===========")
    for (k, cons), avg in throughput_results.items():
        print(f"Replication factor k={k}, consistency={cons}: {avg:.1f} key/sec")

if __name__ == "__main__":
    main()
