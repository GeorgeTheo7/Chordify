import subprocess
import time
import re
import os
import signal
import argparse
import threading

def start_chord_node(k, consistency):
    cmd = ["python", "/home/panosb/Chordify/src/chordify.py", str(k), consistency]
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
    buffer = ""
    while time.time() - start_time < timeout:
        line = proc.stdout.readline()
        if line:
            buffer += line
            print(f"[Chord node] {line.strip()}")
            if re.search(pattern, line):
                return buffer
        else:
            time.sleep(0.1)
    return None

def send_command(proc, command):
    try:
        proc.stdin.write(command)
        proc.stdin.flush()
        return True
    except BrokenPipeError:
        print("Connection to node lost!")
        return False
    except Exception as e:
        print(f"Error sending command: {str(e)}")
        return False


def run_inserts(proc, node_id):
    insert_dir = "/home/panosb/Chordify/insert"
    insert_path = f"{insert_dir}/insert_{node_id:02d}_part.txt"
    
    if not os.path.exists(insert_path):
        print(f"Error: {insert_path} not found.")
        return 0, None, None

    with open(insert_path, 'r') as f:
        keys = [line.strip() for line in f if line.strip()]

    print(f"Node {node_id}: Starting insertion of {len(keys)} keys...")
    start_time = time.time()
    successful_inserts = 0

    for key in keys:
        cmd = f'insert "{key}" "{node_id}"\n'
        if not send_command(proc, cmd):
            break
        
        # Wait for confirmation
        response = read_until(proc, r"(Key inserted successfully.|error)", timeout=5)
        if response and "Key inserted successfully" in response:
            successful_inserts += 1
        else:
            print(f"Failed to insert key: {key}")
        
        time.sleep(0.1)  # Rate limiting

    duration = time.time() - start_time
    return successful_inserts, start_time, duration

def terminate_process(proc):
    try:
        if proc.poll() is None:
            os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
            proc.wait(timeout=5)
    except Exception as e:
        print(f"Error terminating process: {e}")

def drain_stderr(proc):
    for line in proc.stderr:
        print(f"[Chord node STDERR] {line.strip()}")


def main():
    parser = argparse.ArgumentParser(description="Chord Experiment Runner")
    parser.add_argument('--node_id', type=int, required=True)
    parser.add_argument('--k', type=int, required=True)
    parser.add_argument('--consistency', type=str, required=True)
    parser.add_argument('--bootstrap', action='store_true')
    args = parser.parse_args()

    proc = start_chord_node(args.k, args.consistency)

    # In your main() function after starting the process:
    stderr_thread = threading.Thread(target=drain_stderr, args=(proc,))
    stderr_thread.daemon = True
    stderr_thread.start()

    try:
        # Wait for node initialization
        init_msg = read_until(proc, r"Server is up and running in", timeout=10)
        if not init_msg:
            print("Node failed to initialize")
            return

        # Handle bootstrap join
        if args.bootstrap:
            match = re.search(r"Server is up and running in ([\d\.]+):(\d+)", init_msg)
            if match:
                ip, port = match.groups()
                send_command(proc, f"join -b {ip} {port}\n")
        else:
            send_command(proc, "join\n")

        # Wait for join confirmation
        if not read_until(proc, r"New node added successfully!|New chord created.", timeout=15):
            print("Join operation timed out")
            return

        # Perform insertions
        inserted_count, start_time, duration = run_inserts(proc, args.node_id)
        
        if inserted_count > 0:
            print(f"Node {args.node_id}: Successfully inserted {inserted_count} keys in {duration:.2f}s")
            print(f"INSERTION_START: {start_time:.5f}")
            print(f"INSERTION_END: {start_time + duration:.5f}")
            print(f"INSERTION_DURATION: {duration:.5f}")
        else:
            print("No keys were inserted")

    finally:
        terminate_process(proc)

if __name__ == "__main__":
    main()