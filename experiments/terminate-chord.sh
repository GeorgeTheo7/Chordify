#!/bin/bash

# Terminate 10 nodes
terminate() {

    conda_path=$(which conda)
    path=${conda_path%/*}
    # Operations to be send
    find_ip='IPs=$(hostname -I) && arrIP=(${IPs// / })'
    set_ip='export CHORDIFYSERVER_IP=${arrIP[0]}'
    exit="$path/python ./src/cli.py exit"
    
    find_ip_main='IPs=$(hostname -I) && arrIP=(${IPs// / })'
    set_ip_main='export CHORDIFYSERVER_IP=${arrIP[1]}'
    exit_main="$path/python ./src/cli.py exit"

    # Start nodes in main VM
    for n in 1 2
        do
            set_port="export CHORDIFYSERVER_PORT=500$((n - 1))"
            ssh user@main-node "$find_ip_main && $set_ip_main && $set_port && $exit_main"
        done

    # Start nodes in workers
    for i in 0 1 2 3
    do
        for n in 1 2
        do
            set_port="export CHORDIFYSERVER_PORT=500$((n - 1))"
            ssh user@node$i "$find_ip && $set_ip && $set_port && $exit"
        done
    done
        
}

echo "Terminating 10 nodes..."
terminate
echo "All nodes were terminated"