#!/usr/bin/env python3
"""
connect_experiment1.py

This script, running on your MacBook, connects to the 6 VMs (team_32-vm1 through team_32-vm6)
in a round-robin fashion to start 10 chord nodes. Each node runs run_experiment1.py remotely.
Each node prints its throughput in the standardized format "THROUGHPUT: <value>".
After all nodes finish, this script aggregates the throughput (summing over all 10 nodes)
and prints a summary for each experiment configuration.
"""

import subprocess
import time
import re

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

def get_vm_ip(vm):
    """Retrieve the IP address of a given VM via SSH."""
    try:
        # This command gets the first IP address returned by 'hostname -I'
        cmd = f"ssh {vm} hostname -I | awk '{{print $1}}'"
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        return result.stdout.strip() or "Unknown"
    except Exception as e:
        return f"Unknown ({e})"

def run_experiment(k, consistency):
    processes = []
    throughput_sum = 0.0
    vm_info = {vm: get_vm_ip(vm) for vm in vm_hosts}
    print(f"\n=== Starting experiment with k={k} and consistency={consistency} ===")
    
    # Launch 10 nodes (node IDs 0 to 9) in round-robin over the available VMs.
    for node_id in range(10):
        vm = vm_hosts[node_id % len(vm_hosts)]
        vm_ip = vm_info.get(vm, "Unknown")
        
        # Build the remote command.
        # It changes directory into Chordify/src and runs run_experiment1.py with appropriate arguments.
        cmd = f"cd Chordify/src && python3 run_experiment1.py --node_id {node_id} --k {k} --consistency {consistency}"
        if node_id == 0:
            cmd += " --bootstrap"
        full_cmd = ["ssh", vm, cmd]
        print(f"\n--- Starting node {node_id} on {vm} ({vm_ip}) ---")
        proc = subprocess.Popen(full_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        processes.append((node_id, vm, vm_ip, proc))
        time.sleep(0.5)  # slight delay between launching nodes
    
    # Wait for all nodes to finish and collect outputs.
    for node_id, vm, vm_ip, proc in processes:
        stdout, stderr = proc.communicate()
        print(f"\n--- Output from node {node_id} on {vm} ({vm_ip}) ---\n{stdout}")
        if stderr:
            print(f"--- Error output from node {node_id} on {vm} ---\n{stderr}")
        # Look for a line that begins with "THROUGHPUT:" to parse the throughput value.
        match = re.search(r"THROUGHPUT:\s*([0-9.]+)", stdout)
        if match:
            try:
                node_tp = float(match.group(1))
                throughput_sum += node_tp
            except ValueError:
                print(f"Could not parse throughput from node {node_id}")
        else:
            print(f"Throughput value not found in node {node_id}'s output.")
    
    print(f"\n>>> Experiment result: Replication factor k={k}, consistency={consistency}: {throughput_sum:.1f} key/sec")
    return (k, consistency, throughput_sum)

def main():
    results = []
    for k in replication_factors:
        for cons in consistency_options:
            result = run_experiment(k, cons)
            results.append(result)
            time.sleep(2)
    
    print("\n=========== Experiment Results ===========")
    for k, cons, tp in results:
        print(f"Replication factor k={k}, consistency={cons}: {tp:.1f} key/sec")

if __name__ == "__main__":
    main()
