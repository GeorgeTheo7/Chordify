#!/bin/bash

# Starts a chord from 5 VMs
# $1: k factor
# $2: consistency policy
start() {

    # Find IPs of vm
    IPs=$(hostname -I)
    conda_path=$(which conda)
    path=${conda_path%/*}
    # Operations to be send
    # VMs
    chordify="python ~/Chordify/src/server.py"
    find_ip='IPs=$(hostname -I) && arrIP=(${IPs// / })'
    set_ip='export CHORDIFYSERVER_IP=${arrIP[0]}' 
    join="python ~/Chordify/src/cli.py join"

    # Main-node
    find_ip_main='IPs=$(hostname -I) && arrIP=(${IPs// / })'
    set_ip_main='export CHORDIFYSERVER_IP=${arrIP[0]}'
    chordify_main="python ~/Chordify/src/server.py"
    join_main="python ~/Chordify/src/cli.py join"

    # Start nodes in main VM
    for n in {1..2}
        do
            set_port="export CHORDIFYSERVER_PORT=500$((n - 1))"
            ssh ubuntu@main-node "sh -c 'nohup $chordify_main 500$((n - 1)) $1 $2 > /dev/null 2>&1 &'"
            ssh ubuntu@main-node "$find_ip_main && $set_ip_main && $set_port && $join_main"
        done

    # Start nodes in workers
    for i in 0 1 2 3
    do
        for n in {1..2}
        do
            set_port="export CHORDIFYSERVER_PORT=500$((n - 1))"
            ssh ubuntu@node$i "sh -c 'nohup $chordify 500$((n - 1)) $1 $2 > /dev/null 2>&1 &'"
            ssh ubuntu@node$i "$find_ip && $set_ip && $set_port && $join"
        done
    done
        
}

if [ $# -eq 0 -o $# -eq 1 ]
then
    echo "Please provide <k factor> <consistency type>"
    exit
fi

echo "Starting 10 nodes..."
start $1 $2
echo "New chord created"