#!/usr/bin/env python3
import subprocess
import time
import re
import threading

vm_hosts = ["team_32-vm1", "team_32-vm2", "team_32-vm3", "team_32-vm4", "team_32-vm5"]
replication_factors = [1, 3, 5]
consistency_options = ["chain-replication", "eventual-consistency"]

def get_vm_ip(vm):
    cmd = f"ssh {vm} hostname -I | awk '{{print $1}}'"
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    return result.stdout.strip() or "Unknown"

def run_experiment(k, consistency):
    processes = {}
    durations = {}
    vm_info = {vm: get_vm_ip(vm) for vm in vm_hosts}
    
    print(f"\n=== Starting experiment with k={k} and consistency={consistency} ===")
    
    # Start bootstrap node first (node 0)
    bootstrap_node_id = 0
    vm_bootstrap = vm_hosts[bootstrap_node_id % len(vm_hosts)]
    cmd_bootstrap = f"python ~/Chordify/src/run_experiment1.py --node_id {bootstrap_node_id} --k {k} --consistency {consistency} --bootstrap"
    proc_bootstrap = subprocess.Popen(
        ["ssh", vm_bootstrap, cmd_bootstrap],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )
    processes[bootstrap_node_id] = (vm_bootstrap, proc_bootstrap)
    
    # Allow ample time for the bootstrap node to initialize (critical!)
    print("Waiting for bootstrap node to initialize...")
    time.sleep(25)  # Adjust based on observed startup time
    
    # Start non-bootstrap nodes sequentially with staggered delays
    for node_id in range(1, 10):
        vm = vm_hosts[node_id % len(vm_hosts)]
        cmd = f"python ~/Chordify/src/run_experiment1.py --node_id {node_id} --k {k} --consistency {consistency}"
        full_cmd = ["ssh", vm, cmd]
        
        proc = subprocess.Popen(
            full_cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        processes[node_id] = (vm, proc)
        time.sleep(2)  # Stagger node startups to reduce contention
    
    # Monitor output and collect durations
    def monitor_output(node_id, vm, proc):
        stdout, stderr = proc.communicate()
        print(f"\n--- Output from node {node_id} on {vm} ---\n{stdout}")
        if stderr:
            print(f"--- Error output from node {node_id} ---\n{stderr}")
        match = re.search(r"INSERTION_DURATION:\s*([0-9.]+)", stdout)
        if match:
            durations[node_id] = float(match.group(1))
    
    threads = []
    for node_id, (vm, proc) in processes.items():
        thread = threading.Thread(target=monitor_output, args=(node_id, vm, proc))
        thread.start()
        threads.append(thread)
    
    for thread in threads:
        thread.join()
    
    # Calculate throughput
    total_insertions = sum(50 for _ in durations)
    max_duration = max(durations.values(), default=0)
    overall_throughput = total_insertions / max_duration if max_duration > 0 else 0
    
    print(f"\n>>> Experiment result: k={k}, consistency={consistency}: {overall_throughput:.1f} keys/sec (Max duration: {max_duration:.5f} sec)")
    return (k, consistency, overall_throughput)

def main():
    results = []
    for k in replication_factors:
        for cons in consistency_options:
            results.append(run_experiment(k, cons))
            time.sleep(10)  # Cool-down between experiments
    
    print("\n=========== Experiment Results ===========")
    for k, cons, tp in results:
        print(f"Replication factor k={k}, consistency={cons}: {tp:.1f} keys/sec")

if __name__ == "__main__":
    main()