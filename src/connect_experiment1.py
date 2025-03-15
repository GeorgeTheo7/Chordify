#!/usr/bin/env python3
"""
connect_experiment1.py

This script runs (via SSH) the distributed experiment.
For each experiment configuration (replication factor k and consistency policy),
it connects to the six VMs (team_32-vm1 … team_32-vm6) in a round-robin fashion
to start 10 chord nodes. The bootstrap node (node 0) gets a flag so that it will
execute "join -b {ip} {port}", while the other nodes simply execute "join".
Once all nodes are started, each instance of run_experiment1.py will perform its insert operations.
"""

import subprocess
import time

# List of remote hostnames.
vm_hosts = [
    "team_32-vm1",  # VM1 (bootstrap will be here)
    "team_32-vm2",
    "team_32-vm3",
    "team_32-vm4",
    "team_32-vm5",
    "team_32-vm6"
]

# Experiment configurations.
replication_factors = [1, 3, 5]
consistency_options = ["chain-replication", "eventual-consistency"]

def run_experiment(k, consistency):
    processes = []
    print(f"\n=== Starting experiment with k={k} and consistency={consistency} ===")
    # Launch 10 nodes (node IDs 0 to 9) in round-robin over the available VMs.
    for node_id in range(10):
        vm = vm_hosts[node_id % len(vm_hosts)]
        # Build the remote command.
        # It changes directory into Chordify/src and runs run_experiment1.py with appropriate arguments.
        cmd = f"cd Chordify/src && python3 run_experiment1.py --node_id {node_id} --k {k} --consistency {consistency}"
        if node_id == 0:
            cmd += " --bootstrap"
        full_cmd = ["ssh", vm, cmd]
        print(f"Starting node {node_id} on {vm} with command:\n  {' '.join(full_cmd)}")
        # Start the remote process.
        proc = subprocess.Popen(full_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        processes.append((node_id, vm, proc))
        time.sleep(0.5)  # slight delay between starting nodes

    # Wait for all nodes to finish and collect outputs.
    for node_id, vm, proc in processes:
        stdout, stderr = proc.communicate()
        print(f"\n--- Output from node {node_id} on {vm} ---\n{stdout}")
        if stderr:
            print(f"--- Error output from node {node_id} on {vm} ---\n{stderr}")

def main():
    for k in replication_factors:
        for cons in consistency_options:
            run_experiment(k, cons)
            # Pause a bit between experiments.
            time.sleep(2)

if __name__ == "__main__":
    main()
