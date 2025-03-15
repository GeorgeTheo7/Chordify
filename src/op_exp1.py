import requests
import time
import json
import threading

# Assume each node is running a server on a fixed port (e.g., 5000)
DEFAULT_PORT = 5000

def join_node(node_ip, node_port, is_bootstrap=False, bootstrap_ip=None, bootstrap_port=None):
    """
    Send a join request to the node. For the bootstrap node, start without joining.
    For other nodes, join the bootstrap node.
    """
    url = f"http://{node_ip}:{node_port}/join"
    if is_bootstrap:
        # Bootstrap node starts its chord without joining another node.
        payload = {"action": "bootstrap"}
    else:
        # Other nodes join via the bootstrap node.
        payload = {"action": "join", "bootstrap_ip": bootstrap_ip, "bootstrap_port": bootstrap_port}
    try:
        r = requests.post(url, params=payload)
        if r.status_code == 200:
            print(f"Node at {node_ip}:{node_port} successfully joined.")
        else:
            print(f"Node at {node_ip}:{node_port} join failed: {r.text}")
    except Exception as e:
        print(f"Error joining node at {node_ip}:{node_port}: {e}")

def run_inserts(node_ip, node_port, insert_file):
    """
    For a given node, sequentially read keys from insert_file and insert them.
    Each insert is sent to the /insert endpoint, and the next key is only sent if
    the previous insertion was successful (HTTP status 200).
    """
    total_keys = 0
    first_insertion_time = None
    last_insertion_time = None

    try:
        with open(insert_file, 'r') as f:
            for line in f:
                key = line.strip()
                if not key:
                    continue

                if first_insertion_time is None:
                    first_insertion_time = time.time()

                url = f"http://{node_ip}:{node_port}/insert"
                # Here we assume the value is the same as the key; adjust as needed.
                r = requests.post(url, params={"key": key, "value": key})
                if r.status_code == 200:
                    total_keys += 1
                    last_insertion_time = time.time()
                    print(f"[{node_ip}:{node_port}] Inserted key: {key}")
                else:
                    print(f"[{node_ip}:{node_port}] Failed to insert key {key}: {r.text}")
                    # Optionally, you can retry or break the loop if insertion fails.
                    break
    except FileNotFoundError:
        print(f"Insert file {insert_file} not found for node {node_ip}:{node_port}!")

    duration = (last_insertion_time - first_insertion_time) if (first_insertion_time and last_insertion_time) else 0
    print(f"[{node_ip}:{node_port}] Finished inserting {total_keys} keys in {duration:.5f} sec")
    return total_keys, duration

def run_experiment(k, consistency, node_configs):
    """
    node_configs: List of dictionaries each with:
        - 'vm': the VM hostname (for logging)
        - 'ip': the node's IP address
        - 'port': the node's server port (e.g., 5000)
        - 'insert_file': the file containing keys for that node.
    The first node in the list is considered the bootstrap.
    """
    threads = []
    durations = []
    
    # Start nodes by sending join requests. Node 0 bootstraps.
    bootstrap = node_configs[0]
    join_node(bootstrap['ip'], bootstrap['port'], is_bootstrap=True)
    for node in node_configs[1:]:
        join_node(node['ip'], node['port'],
                  is_bootstrap=False,
                  bootstrap_ip=bootstrap['ip'],
                  bootstrap_port=bootstrap['port'])
    
    # Allow some time for the chord network to stabilize after joining.
    time.sleep(2)
    
    # Start insertions concurrently on each node.
    def insert_worker(config):
        _, duration = run_inserts(config['ip'], config['port'], config['insert_file'])
        durations.append(duration)
    
    for config in node_configs:
        t = threading.Thread(target=insert_worker, args=(config,))
        t.start()
        threads.append(t)
    
    # Wait for all nodes to finish inserting.
    for t in threads:
        t.join()

    if durations:
        max_duration = max(durations)
        overall_throughput = 500 / max_duration if max_duration > 0 else float('inf')
    else:
        max_duration = 0
        overall_throughput = 0

    print(f"\n>>> Experiment result (k={k}, consistency={consistency}): " \
          f"{overall_throughput:.1f} key/sec (Max insertion duration: {max_duration:.5f} sec)")
    return (k, consistency, overall_throughput)

if __name__ == "__main__":
    # Example configuration for 10 nodes.
    # Here we assume you know the IP addresses and each node's insert file name.
    # For instance, node 0 uses "insert_0.txt", node 1 uses "insert_1.txt", etc.
    node_configs = [
        {"vm": "team_32-vm1", "ip": "10.0.0.1", "port": DEFAULT_PORT, "insert_file": "insert_0.txt"},
        {"vm": "team_32-vm2", "ip": "10.0.0.2", "port": DEFAULT_PORT, "insert_file": "insert_1.txt"},
        {"vm": "team_32-vm3", "ip": "10.0.0.3", "port": DEFAULT_PORT, "insert_file": "insert_2.txt"},
        {"vm": "team_32-vm4", "ip": "10.0.0.4", "port": DEFAULT_PORT, "insert_file": "insert_3.txt"},
        {"vm": "team_32-vm5", "ip": "10.0.0.5", "port": DEFAULT_PORT, "insert_file": "insert_4.txt"},
        {"vm": "team_32-vm1", "ip": "10.0.0.1", "port": DEFAULT_PORT, "insert_file": "insert_5.txt"},
        {"vm": "team_32-vm2", "ip": "10.0.0.2", "port": DEFAULT_PORT, "insert_file": "insert_6.txt"},
        {"vm": "team_32-vm3", "ip": "10.0.0.3", "port": DEFAULT_PORT, "insert_file": "insert_7.txt"},
        {"vm": "team_32-vm4", "ip": "10.0.0.4", "port": DEFAULT_PORT, "insert_file": "insert_8.txt"},
        {"vm": "team_32-vm5", "ip": "10.0.0.5", "port": DEFAULT_PORT, "insert_file": "insert_9.txt"}
    ]

    # Run experiments for different configurations.
    replication_factors = [1, 3, 5]
    consistency_options = ["linearizability", "eventual-consistency"]

    results = []
    for k in replication_factors:
        for cons in consistency_options:
            print(f"\n=== Starting experiment with k={k} and consistency={cons} ===")
            # In this simplified example, we assume that k and consistency are configured via other means
            # (e.g., when the node starts) so that the /insert endpoint uses them.
            result = run_experiment(k, cons, node_configs)
            results.append(result)
            time.sleep(2)

    print("\n=========== Experiment Results ===========")
    for k, cons, tp in results:
        print(f"Replication factor k={k}, consistency={cons}: {tp:.1f} key/sec")
