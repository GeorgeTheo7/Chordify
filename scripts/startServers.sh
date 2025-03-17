#!/bin/bash

# Start the bootstrap node and capture its port
output=$(expect ./join_node.exp team_32-vm1 bootstrap)
bootstrap_port=$(echo "$output" | grep "BOOTSTRAP_PORT=" | cut -d'=' -f2)

if [ -z "$bootstrap_port" ]; then
  echo "Failed to capture bootstrap port"
  exit 1
fi

echo "Bootstrap node started on port $bootstrap_port"
sleep 5

# Start other nodes in new terminals
gnome-terminal -- expect ./join_node.exp team_32-vm2 join $bootstrap_port
sleep 3
gnome-terminal -- expect ./join_node.exp team_32-vm3 join $bootstrap_port
sleep 3
gnome-terminal -- expect ./join_node.exp team_32-vm4 join $bootstrap_port
sleep 3
gnome-terminal -- expect ./join_node.exp team_32-vm5 join $bootstrap_port
sleep 3

echo "All nodes started in separate terminals!"