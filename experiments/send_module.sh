#!/bin/bash

# Zip source code and scripts folder, excluding unnecessary files
tar --exclude="../src/__pycache__" -czvf ../code.tar.gz . ../src ../scripts

# List of VM aliases defined in your local SSH config
vms=("main-node" "node0" "node1" "node2" "node3")

# Send tarball to each VM and extract it
for vm in "${vms[@]}"; do
    echo "Distributing to $vm..."
    scp ../code.tar.gz ubuntu@$vm:.
    ssh ubuntu@$vm 'tar -xzf ./code.tar.gz && rm ./code.tar.gz'
done

echo "Distribution complete."