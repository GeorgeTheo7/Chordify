import subprocess
import time
import re

def get_vm_ip(vm):
    """Retrieve the IP address of a given VM via SSH."""
    try:
        cmd = f"ssh {vm} hostname -I | awk '{{print $1}}'"
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        return result.stdout.strip()
    except Exception as e:
        return f"Unknown ({e})"

vm_hosts = [
    "team_32-vm1",  # VM1 (bootstrap)
    "team_32-vm2",
    "team_32-vm3",
    "team_32-vm4",
    "team_32-vm5",
    "team_32-vm6"
]

replication_factors = [1, 3, 5]
consistency_options = ["chain-replication", "eventual-consistency"]

def run_experiment(k, consistency):
    processes = []
    vm_info = {vm: get_vm_ip(vm) for vm in vm_hosts}  # Fetch VM IPs
    print(f"\n=== Starting experiment with k={k} and consistency={consistency} ===")
    
    for node_id in range(10):
        vm = vm_hosts[node_id % len(vm_hosts)]
        vm_ip = vm_info.get(vm, "Unknown")
        
        cmd = f"cd Chordify/src && python3 run_experiment1.py --node_id {node_id} --k {k} --consistency {consistency}"
        if node_id == 0:
            cmd += " --bootstrap"
        full_cmd = ["ssh", vm, cmd]
        
        print(f"\n--- Output from node {node_id} on {vm} ({vm_ip}) ---")
        proc = subprocess.Popen(full_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        processes.append((node_id, vm, vm_ip, proc))
        time.sleep(0.5)
    
    for node_id, vm, vm_ip, proc in processes:
        stdout, stderr = proc.communicate()
        print(f"\n--- Output from node {node_id} on {vm} ({vm_ip}) ---\n{stdout}")
        if stderr:
            print(f"--- Error output from node {node_id} on {vm} ---\n{stderr}")

def main():
    for k in replication_factors:
        for cons in consistency_options:
            run_experiment(k, cons)
            time.sleep(2)

if __name__ == "__main__":
    main()
