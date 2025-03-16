#!/usr/bin/env python3
import subprocess
import time
import re
import threading
import signal
import sys
from subprocess import TimeoutExpired

vm_hosts = ["team_32-vm1", "team_32-vm2", "team_32-vm3", "team_32-vm4", "team_32-vm5"]
replication_factors = [1, 3, 5]
consistency_options = ["chain-replication", "eventual-consistency"]

# Global flag for graceful shutdown
shutdown_flag = False

def signal_handler(sig, frame):
    global shutdown_flag
    print("\nReceived shutdown signal, terminating processes...")
    shutdown_flag = True
    sys.exit(1)

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

def get_vm_ip(vm):
    try:
        cmd = f"ssh {vm} hostname -I | awk '{{print $1}}'"
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=5)
        return result.stdout.strip() or "Unknown"
    except subprocess.TimeoutExpired:
        print(f"Timeout getting IP for {vm}")
        return "Unknown"

def run_experiment(k, consistency):
    processes = {}
    durations = {}
    vm_info = {vm: get_vm_ip(vm) for vm in vm_hosts}
    
    print(f"\n=== Starting experiment with k={k} and consistency={consistency} ===")
    
    try:
        # Start bootstrap node first with longer timeout
        bootstrap_node_id = 0
        vm_bootstrap = vm_hosts[bootstrap_node_id % len(vm_hosts)]
        cmd_bootstrap = f"python ~/Chordify/src/run_experiment1.py --node_id {bootstrap_node_id} --k {k} --consistency {consistency} --bootstrap"
        
        try:
            proc_bootstrap = subprocess.Popen(
                ["ssh", vm_bootstrap, cmd_bootstrap],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            processes[bootstrap_node_id] = (vm_bootstrap, proc_bootstrap)
            print(f"Started bootstrap node {bootstrap_node_id} on {vm_bootstrap}")
        except Exception as e:
            print(f"Failed to start bootstrap node: {str(e)}")
            return (k, consistency, 0)

        # Wait for bootstrap node to initialize
        print("Waiting for bootstrap node to initialize (25s)...")
        time.sleep(25)

        # Start other nodes with staggered delays
        for node_id in range(1, 10):
            if shutdown_flag:
                break
            vm = vm_hosts[node_id % len(vm_hosts)]
            cmd = f"python ~/Chordify/src/run_experiment1.py --node_id {node_id} --k {k} --consistency {consistency}"
            
            try:
                proc = subprocess.Popen(
                    ["ssh", vm, cmd],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True
                )
                processes[node_id] = (vm, proc)
                print(f"Started node {node_id} on {vm}")
                time.sleep(2)  # Stagger node startups
            except Exception as e:
                print(f"Failed to start node {node_id}: {str(e)}")
                continue

        def monitor_output(node_id, vm, proc):
            try:
                try:
                    stdout, stderr = proc.communicate(timeout=300)  # 5-minute timeout
                except TimeoutExpired:
                    print(f"Timeout on node {node_id}, terminating...")
                    proc.kill()
                    stdout, stderr = proc.communicate()
                
                if proc.returncode != 0:
                    print(f"Node {node_id} exited with code {proc.returncode}")
                    
                print(f"\n--- Output from node {node_id} on {vm} ---\n{stdout}")
                if stderr:
                    print(f"--- Error output from node {node_id} ---\n{stderr}")
                
                match = re.search(r"INSERTION_DURATION:\s*([0-9.]+)", stdout)
                if match:
                    durations[node_id] = float(match.group(1))
                    
            except BrokenPipeError:
                print(f"Broken pipe error occurred with node {node_id}")
            except Exception as e:
                print(f"Error processing output for node {node_id}: {str(e)}")

        threads = []
        for node_id, (vm, proc) in processes.items():
            if shutdown_flag:
                break
            thread = threading.Thread(target=monitor_output, args=(node_id, vm, proc))
            thread.start()
            threads.append(thread)

        for thread in threads:
            thread.join(timeout=310)  # Slightly longer than process timeout

    finally:
        # Cleanup any remaining processes
        for node_id, (vm, proc) in processes.items():
            if proc.poll() is None:
                print(f"Terminating node {node_id} on {vm}")
                proc.terminate()
                try:
                    proc.wait(timeout=5)
                except TimeoutExpired:
                    proc.kill()

    total_insertions = sum(50 for _ in durations)
    max_duration = max(durations.values(), default=0)
    overall_throughput = total_insertions / max_duration if max_duration > 0 else 0
    
    print(f"\n>>> Experiment result: k={k}, consistency={consistency}: {overall_throughput:.1f} keys/sec (Max duration: {max_duration:.5f} sec)")
    return (k, consistency, overall_throughput)

def main():
    results = []
    try:
        for k in replication_factors:
            for cons in consistency_options:
                if shutdown_flag:
                    break
                results.append(run_experiment(k, cons))
                time.sleep(10)  # Longer cooldown between experiments
    except KeyboardInterrupt:
        print("\nExperiment sequence interrupted")
    
    print("\n=========== Experiment Results ===========")
    for k, cons, tp in results:
        print(f"Replication factor k={k}, consistency={cons}: {tp:.1f} keys/sec")

if __name__ == "__main__":
    main()