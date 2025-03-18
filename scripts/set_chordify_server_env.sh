#!/bin/bash

# List of IPs and Ports
IP_PORTS=(
    "10.0.39.177:5050"
    "10.0.39.155:5050"
    "10.0.39.191:5050"
    "10.0.39.19:5050"
    "10.0.39.87:5050"
    "10.0.39.177:5051"
    "10.0.39.155:5051"
    "10.0.39.191:5051"
    "10.0.39.19:5051"
    "10.0.39.87:5051"
)

# Loop through the list of IP:Port and set environment variables
for ip_port in "${IP_PORTS[@]}"; do
    ip=$(echo $ip_port | cut -d ':' -f 1)
    port=$(echo $ip_port | cut -d ':' -f 2)

    export CHORDIFYSERVER_IP="$ip"
    export CHORDIFYSERVER_PORT="$port"

    # Print the values to confirm they are set
    echo "Setting environment variables:"
    echo "CHORDIFYSERVER_IP=$CHORDIFYSERVER_IP"
    echo "CHORDIFYSERVER_PORT=$CHORDIFYSERVER_PORT"
done

