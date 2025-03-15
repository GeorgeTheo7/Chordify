#!/usr/bin/env python3
import argparse
import requests
import time
import json

DEFAULT_PORT = 5000

def join_node(is_bootstrap, bootstrap_ip=None, bootstrap_port=None):
    url = f"http://localhost:{DEFAULT_PORT}/join"
    if is_bootstrap:
        payload = {"action": "bootstrap"}
        print(f"[DEBUG] Bootstrapping node at localhost:{DEFAULT_PORT}")
    else:
        payload = {"action": "join", "bootstrap_ip": bootstrap_ip, "bootstrap_port": bootstrap_port}
        print(f"[DEBUG] Joining node at localhost:{DEFAULT_PORT} using bootstrap {bootstrap_ip}:{bootstrap_port}")
    try:
        r = requests.post(url, params=payload)
        if r.status_code == 200:
            print(f"[DEBUG] Successfully joined node at localhost:{DEFAULT_PORT}")
        else:
            print(f"[ERROR] Failed to join node at localhost:{DEFAULT_PORT}: {r.text}")
    except Exception as e:
        print(f"[ERROR] Exception during join: {e}")

def run_inserts(insert_file):
    total_keys = 0
    first_insertion_time = None
    last_insertion_time = None

    print(f"[DEBUG] Starting key insertions using file '{insert_file}'")
    try:
        with open(insert_file, 'r') as f:
            for line in f:
                key = line.strip()
                if not key:
                    continue
                if first_insertion_time is None:
                    first_insertion_time = time.time()
                url = f"http://localhost:{DEFAULT_PORT}/insert"
                print(f"[DEBUG] Inserting key '{key}' at localhost:{DEFAULT_PORT}")
                r = requests.post(url, params={"key": key, "value": key})
                if r.status_code == 200:
                    total_keys += 1
                    last_insertion_time = time.time()
                    print(f"[DEBUG] Successfully inserted key '{key}'")
                else:
                    print(f"[ERROR] Failed to insert key '{key}': {r.text}")
                    break  # Stop on failure; optionally add retry logic.
    except FileNotFoundError:
        print(f"[ERROR] Insert file '{insert_file}' not found!")
    
    duration = (last_insertion_time - first_insertion_time) if (first_insertion_time and last_insertion_time) else 0
    print(f"INSERTION_DURATION: {duration}")
    return total_keys, duration

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--node_id", type=int, required=True)
    parser.add_argument("--k", type=int, required=True)
    parser.add_argument("--consistency", type=str, required=True)
    parser.add_argument("--bootstrap", action="store_true")
    args = parser.parse_args()

    print(f"[INFO] Starting remote experiment on node {args.node_id} with k={args.k} and consistency={args.consistency}")

    if args.bootstrap:
        join_node(is_bootstrap=True)
    else:
        # For simplicity, assume bootstrap is at localhost; if needed, pass actual bootstrap info.
        join_node(is_bootstrap=False, bootstrap_ip="localhost", bootstrap_port=DEFAULT_PORT)

    # Allow time for the join to complete.
    time.sleep(2)

    insert_file = f"insert_{args.node_id}.txt"
    run_inserts(insert_file)

if __name__ == "__main__":
    main()
