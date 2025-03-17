#!/bin/bash

# Files 
nodes="../nodes.txt"
keys="../insert/insert_00_part.txt"

# Operations
conda_path=$(which conda)
path=${conda_path%/*}
# Operations to be send
find_ip='IPs=$(hostname -I) && arrIP=(${IPs// / })'
set_ip='export CHORDIFYSERVER_IP=${arrIP[0]}'
insert="$path/python ./src/cli.py insert"

find_ip_main='IPs=$(hostname -I) && arrIP=(${IPs// / })'
set_ip_main='export CHORDIFYSERVER_IP=${arrIP[1]}'
insert_main="$path/python ./src/cli.py insert"

start=`date +%s`
while IFS= read -r -u 4 node_port && IFS= read -r -u 5 key_pair;
do
	# Read key pair
	IFS=','
	read -a arr <<< "$key_pair"
	key=${arr[0]}
	value=$((${arr[1]}))

	# Read node & port
	IFS=' '
	read -a arr <<< "$node_port"
	node=${arr[0]}
	port=$((${arr[1]}))

	set_port="export CHORDIFYSERVER_PORT=$port"
	
	if [ "$node" = "main-node" ]
	then
		ssh ubuntu@$node "$find_ip_main && $set_ip_main && $set_port && $insert_main \"$key\" $value"
	else
		ssh ubuntu@$node "$find_ip && $set_ip && $set_port && $insert \"$key\" $value"
	fi

done 4<$nodes 5<$keys
end=`date +%s`

runtime=$((end-start))
echo $runtime >> ../insert/insert_times.txt