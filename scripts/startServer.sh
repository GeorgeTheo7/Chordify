#!/bin/bash

ssh team_32-vm1 pkill -9 python
ssh team_32-vm2 pkill -9 python
ssh team_32-vm3 pkill -9 python
ssh team_32-vm4 pkill -9 python
ssh team_32-vm5 pkill -9 python

# Start the bootstrap node and capture its port
output=$(expect ./join_node.exp team_32-vm1 bootstrap)
bootstrap_port=$(echo "$output" | grep "BOOTSTRAP_PORT=" | cut -d'=' -f2)

if [ -z "$bootstrap_port" ]; then
  echo "Failed to capture bootstrap port"
  exit 1
fi

echo "Bootstrap node started on port $bootstrap_port"
sleep 5

#gnome-terminal -- bash -c "expect ./join_node.exp team_32-vm1 join"
#sleep 3
gnome-terminal -- bash -c "expect ./join_node.exp team_32-vm2 join"
sleep 3
gnome-terminal -- bash -c "expect ./join_node.exp team_32-vm3 join"
sleep 3
gnome-terminal -- bash -c "expect ./join_node.exp team_32-vm4 join"
sleep 3
gnome-terminal -- bash -c "expect ./join_node.exp team_32-vm5 join"
sleep 3
gnome-terminal -- bash -c "expect ./join_node.exp team_32-vm1 join"
sleep 3
gnome-terminal -- bash -c "expect ./join_node.exp team_32-vm2 join"
sleep 3
gnome-terminal -- bash -c "expect ./join_node.exp team_32-vm3 join"
sleep 3
gnome-terminal -- bash -c "expect ./join_node.exp team_32-vm4 join"
sleep 3
gnome-terminal -- bash -c "expect ./join_node.exp team_32-vm5 join; exec bash"
sleep 3

echo "All nodes started in separate terminals!"

