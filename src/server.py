#!/usr/bin/env python

from flask import Flask
from flask import request
import requests
from requests.adapters import HTTPAdapter
import multiprocessing
import logging
import socket
import sys
import json
from node import *

# Initialize the Flask application
app = Flask(__name__)
log = logging.getLogger('werkzeug')
# Hide logs of HTTP requests
log.disabled = True

def shutdown_server():
    func = request.environ.get('werkzeug.server.shutdown')
    if func is None:
        raise RuntimeError('Not running with the Werkzeug Server')
    func()

@app.route('/')
def health_check():
    global ip, port
    return "\nServer is up and running in {}:{} !".format(ip,port)

@app.route('/join', methods=['PUT'])
def join():
    global node, ip, port, kappa, consistency
    bnode_ip = request.args.get("ip")
    bnode_port = int(request.args.get("port")) # port number was given as string, so we have to cast to (int)

    if not node is None:
        return "You're already part of chord.", 403

    if ip == bnode_ip and port == bnode_port:
        node = BootstrapNode(ip, port, kappa, consistency)
        return "New chord created.", 200
    else:
        
        node = Node(ip, port, (bnode_ip, bnode_port), kappa, consistency)
        
        # Communicate with bootstrap node
        url = "http://{}:{}/addNode".format(bnode_ip,bnode_port)
        r = requests.put(url, params={"ip":node.ip,"port":node.port})
        
        if r.status_code == 200:

            data = r.json()
            node.previous_node = RefNode(data["previous"]["ip"],int(data["previous"]["port"]))
            node.next_node = RefNode(data["next"]["ip"],int(data["next"]["port"]))
            
            # Inform neighboors
            # Receive keys from next
            url = "http://{}:{}/transferKeys".format(node.next_node.ip,node.next_node.port)
            r1 = requests.get(url, params={"keynode":node.key})

            # Download Primary Keys
            data = r1.json()
            node.data = {d["key_hash"]:(d["key"],d["value"]) for d in data["keys"]}

            if node.kappa > 1:

                if node.consistency_type == "chain-replication" or node.consistency_type == "eventual-consistency":
                    
                    node.replicas = {d["key_hash"]:(d["key"],d["value"],d["replica_number"]) for d in data["replicas"]}
                    # Initiate fix replicas operation
                    s = requests.Session()
                    s.mount('http://', HTTPAdapter(max_retries=0))
                    url = "http://{}:{}/initfixReplicas".format(node.next_node.ip,node.next_node.port)
                    s.get(url)

            # Inform previous
            url = "http://{}:{}/changeNext".format(node.previous_node.ip,node.previous_node.port)
            r2 = requests.put(url, params={"ip":node.ip,"port":node.port})
            
            # Inform next
            url = "http://{}:{}/changePrevious".format(node.next_node.ip,node.next_node.port)
            r3 = requests.put(url, params={"ip":node.ip,"port":node.port})

            if node.kappa == 1:
                # Tell next to delete unnecessary keys
                url = "http://{}:{}/deleteKeys".format(node.next_node.ip,node.next_node.port)
                r4 = requests.delete(url, params={"keynode":node.key})

            # Edge case:
            elif node.kappa > 1:
                
                data = {"existing":list(node.replicas.keys()) + list(node.data.keys())}
                url = "http://{}:{}/generateReplicas".format(node.previous_node.ip,node.previous_node.port)
                r5 = requests.get(url,json=json.dumps(data))

                data = r5.json()["keys"]

                for d in data:
                    node.add_replica(d["key"],d["value"],d["replica_number"])
            
            return "New node added successfully!", 200

        else:
            return r.text, r.status_code

@app.route('/changeNext',methods=['PUT'])
def change_next():
    new_ip = request.args.get("ip")
    new_port = int(request.args.get("port"))
    if new_ip == node.ip and node.port == new_port:
        node.next_node = None
    else:
        node.next_node = RefNode(new_ip,new_port)
    return "Changed next node.", 200

@app.route('/changePrevious',methods=['PUT'])
def change_previous():
    new_ip = request.args.get("ip")
    new_port = int(request.args.get("port"))
    if new_ip == node.ip and node.port == new_port:
        node.previous_node = None
    else:
        node.previous_node = RefNode(new_ip,new_port)
    return "Changed previous node.", 200

@app.route('/addNode', methods=['PUT'])
def add_node():
    global node
    if node.is_bootstrap():
        ip = request.args.get("ip")
        port = int(request.args.get("port"))
        keynode = node.add_node(ip, port)
        if keynode == -1:
            return "Node is already inside chord.", 405
        else:
            prev_node, next_node = node.find_neighboors(keynode)
            data = {"previous":{"ip":prev_node[0],"port":prev_node[1]}, "next":{"ip":next_node[0],"port":next_node[1]}}
            response = app.response_class(
                response=json.dumps(data),
                status=200,
                mimetype='application/json'
            )
            return response
    else:
        return "I'm not the bootstrap server. Please contact {}:{}".format(node.bnode.ip,node.bnode.port), 301

@app.route('/depart', methods=['DELETE'])
def depart():
    global node

    if node is None:
        return "You have to join first.", 403

    if node.is_bootstrap():
        return "Bootstrap node is not allowed to depart!", 403
    else:
        # Send keys to next node
        if not node.data == {}:
            data_list = [{"key_hash":k,"key":v[0],"value":v[1]} for (k,v) in node.data.items()]
            data = {"keys":data_list}
            r = requests.post("http://{}:{}/send".format(node.next_node.ip,node.next_node.port), json=json.dumps(data))

            # In case of replication, my replicas sould shift
            if node.kappa > 1:
                
                if node.consistency_type == "chain-replication" or node.consistency_type == "eventual-consistency":
                    
                    s = requests.Session()
                    s.mount('http://', HTTPAdapter(max_retries=0))
                    r = s.post("http://{}:{}/shiftReplicas".format(node.next_node.ip,node.next_node.port))

        # Send replicas
        if not node.replicas == {}:

            if node.consistency_type == "chain-replication" or node.consistency_type == "eventual-consistency":            
                
                s = requests.Session()
                s.mount('http://', HTTPAdapter(max_retries=0))
                        
                for (k,v) in node.replicas.items():
                    r = s.post("http://{}:{}/insertReplicas".format(node.next_node.ip,node.next_node.port),params={"key":v[0],"value":v[1],"replica_number":v[2]})

        # Communicate with bootstrap node
        url = "http://{}:{}/removeNode".format(node.bnode.ip,node.bnode.port)
        r = requests.delete(url, params={"keynode":node.key})
        
        # Inform neighboors
        # Inform Previous
        url = "http://{}:{}/changeNext".format(node.previous_node.ip,node.previous_node.port)
        requests.put(url, params={"ip":node.next_node.ip,"port":node.next_node.port})
        
        # Inform Next
        url = "http://{}:{}/changePrevious".format(node.next_node.ip,node.next_node.port)
        requests.put(url, params={"ip":node.previous_node.ip,"port":node.previous_node.port})
        
        node = None
        
        return r.text

@app.route('/shiftReplicas',methods=['POST'])
def shift_replicas():
    global node
    
    deletion_keys = set()
    s = requests.Session()
    s.mount('http://', HTTPAdapter(max_retries=0))
    
    for (k,(key,value,replica_num)) in node.replicas.items():
        if replica_num == 1:
            
            r = s.post("http://{}:{}/insertReplicas".format(node.next_node.ip,node.next_node.port),params={"key":key,"value":value,"replica_number":1})
            
            deletion_keys.add(k)

    # Delete unnecessary keys
    for k in deletion_keys:
        del node.replicas[k]

    return "Replicas of previous node shifted.", 200

@app.route('/kickout', methods=['DELETE'])
def kickout():
    global node
    if node.is_bootstrap():
        return "Bootstrap node is not allowed to be excluded!", 200
    else:
        # Communicate with bootstrap node
        node = None
        return "Node object deleted!", 200

@app.route('/removeNode', methods=['DELETE'])
def remove_node():
    global node
    if node.is_bootstrap():
        keynode = int(request.args.get("keynode"))
        if keynode == node.key:
            return "Bootstrap node is not allowed to depart!"
        res = node.delete_node(keynode)
        if res == -1:
            return "Node is not part of chord!", 403
        else:
            return "Node deleted succesfully!", 200
    else:
        return "I'm not the bootstrap server. Please contact {}:{}".format(node.bnode.ip,node.bnode.port), 301

@app.route('/query')
def query():
    global node
    key_value = request.args.get("key")

    if node is None:
        return "You have to join first.", 403
    
    if key_value == "*":
        
        data_list = [{
                        "node":{"hash":node.key, "ip":node.ip, "port":node.port}, 
                        "keys":[{"hash":k, "key":v[0], "value":v[1]} for k,v in node.data.items()], 
                        "replicas":[{"hash":k, "key":v[0], "value":v[1],"replica_number":v[2]} for k,v in node.replicas.items()],
                    }]

        response = app.response_class(
            response=json.dumps(data_list),
            status=200,
            mimetype='application/json'
        )
        
        return response

    else:

        key = hash_key(key_value)
        successor = node.successor(key_value)
        
        if successor.key == node.key:

            if key in node.data:

                data = {
                        "hash": key,
                        "key": node.data[key][0],
                        "value": node.data[key][1],
                        "replica_number": "original",
                        "node_ip": node.ip,
                        "node_port": node.port,
                    }
                my_response = app.response_class(
                                response=json.dumps(data),
                                status=200,
                                mimetype='application/json'
                            )
                
                if node.kappa == 1:

                    return my_response

                elif node.consistency_type == "chain-replication":
                    
                    s = requests.Session()
                    s.mount('http://', HTTPAdapter(max_retries=0))
                    url = "http://{}:{}/queryReplicas".format(node.next_node.ip,node.next_node.port)
                    r = s.get(url,params={"key":key_value})
                    
                    if r.status_code == 200:
                        return  app.response_class(
                            response=json.dumps(r.json()),
                            status=r.status_code,
                            mimetype='application/json'
                        )
                    else:
                        return r.text, r.status_code
                
                elif node.consistency_type == "eventual-consistency":
                    
                    return  my_response  
            else:
                return "Key not found.",404
        else:

            s = requests.Session()
            s.mount('http://', HTTPAdapter(max_retries=0))

            if node.kappa > 1 and key in node.replicas:

                data = {
                    "hash": key,
                    "key": node.replicas[key][0],
                    "value": node.replicas[key][1],
                    "replica_number": node.replicas[key][2],
                    "node_ip": node.ip,
                    "node_port": node.port,
                }

                my_response = app.response_class(
                                response=json.dumps(data),
                                status=200,
                                mimetype='application/json'
                            )
                
                if node.consistency_type == "eventual-consistency":
                    
                    return my_response

                elif node.consistency_type == "chain-replication":
                    
                    if node.replicas[key][2] == node.kappa - 1 or node.next_node.key == successor.key: 

                        return my_response

                    else:
                        url = "http://{}:{}/query".format(node.next_node.ip,node.next_node.port)
                        r = s.get(url,params={"key":key_value})

                        if r.status_code == 200:
                            return  app.response_class(
                                response=json.dumps(r.json()),
                                status=r.status_code,
                                mimetype='application/json'
                            )
                        else:
                            return r.text, r.status_code
                    
            # Send key to successor
            url = "http://{}:{}/query".format(successor.ip,successor.port)
            r = s.get(url,params={"key":key_value})
            if r.status_code == 200:
                return  app.response_class(
                    response=json.dumps(r.json()),
                    status=r.status_code,
                    mimetype='application/json'
                )
            else:
                return r.text, r.status_code

@app.route('/queryReplicas')
def query_replicas():
    global node

    key_value = request.args.get("key")
    k, v, replica_number = node.replicas[hash_key(key_value)]

    if not node.key == node.successor(key_value).key:

        data = {
            "hash": k,
            "key": key_value,
            "value": v,
            "replica_number": replica_number,
            "node_ip": node.ip,
            "node_port": node.port,
        }
        my_response = app.response_class(
                                response=json.dumps(data),
                                status=200,
                                mimetype='application/json'
                            )
    
        if replica_number == node.kappa - 1:
            return my_response
        else:
                
            s = requests.Session()
            s.mount('http://', HTTPAdapter(max_retries=0))
            url = "http://{}:{}/queryReplicas".format(node.next_node.ip,node.next_node.port)
            r = s.get(url,params={"key":key_value})

            if r.status_code == 200:
                return app.response_class(
                            response=json.dumps(r.json()),
                            status=200,
                            mimetype='application/json'
                        )
            elif r.status_code == 204:
                return my_response
    else:
        return "Replica manager only have original data.", 204


@app.route('/nextNode')
def next_node():
    global node
    if node == None or node.next_node == None:
        response = app.response_class(
            response=json.dumps({}),
            status=204,
            mimetype='application/json'
        )
    else:
        response = app.response_class(
            response=json.dumps({"ip":node.next_node.ip,"port":node.next_node.port}),
            status=200,
            mimetype='application/json'
        )
    return response

@app.route('/queryAll')
def query_all():
    global node

    if node is None:
        return "You have to join first.", 403

    data_list = [{
                    "node":{"hash":node.key,"ip":node.ip,"port":node.port}, 
                    "keys":[{"hash":k,"key":v[0], "value":v[1]} for k,v in node.data.items()], 
                    "replicas":[{"hash":k,"key":v[0], "value":v[1],"replica_number":v[2]} for k,v in node.replicas.items()],
                }]
    
    next_node = node.next_node

    if not next_node == None:

        while not next_node.key == node.key:
            # Find next node for query
            url = "http://{}:{}/nextNode".format(next_node.ip,next_node.port)
            r1 = requests.get(url)
            
            # Receive keys for next node
            url = "http://{}:{}/query".format(next_node.ip,next_node.port)
            r2 = requests.get(url, params={"key":"*"})

            # Update data_list
            if r2.status_code == 200:
                data_list = data_list + r2.json()

            # Update next node
            data = r1.json()
            next_node = RefNode(data["ip"],data["port"])        

    response = app.response_class(
        response=json.dumps(data_list),
        status=200,
        mimetype='application/json'
    )
    return response

@app.route('/insert',methods=['POST'])
def insert():
    global node    
    key_value = request.args.get("key")
    value = request.args.get("value")

    if node is None:
        return "You have to join first.", 403
    
    successor = node.successor(key_value)

    if successor.key == node.key:
        
        # Add key here
        key = node.add_key(key_value,value)

        data = {
            "hash": key,
            "key": key_value,
            "value": value,
            "node_ip": node.ip,
            "node_port": node.port,
        }
        insert_response = app.response_class(
                response=json.dumps(data),
                status=200,
                mimetype='application/json'
            )

        if node.kappa > 1:

            url = "http://{}:{}/insertReplicas".format(node.next_node.ip,node.next_node.port)
            params = {"key":key_value,"value":value,"replica_number":1}

            if node.consistency_type == "chain-replication":
                
                s = requests.Session()
                s.mount('http://', HTTPAdapter(max_retries=0))
                r = s.post(url,params=params)
            
            elif node.consistency_type == "eventual-consistency":
                
                # Asychrnous call of insertReplicas
                p1 = multiprocessing.Process(target=async_post, args=(url,params,{}))
                # starting process 1
                p1.start()
            
        return insert_response
    
    else:
        
        # Send key to successor
        s = requests.Session()
        s.mount('http://', HTTPAdapter(max_retries=0))
        url = "http://{}:{}/insert".format(successor.ip,successor.port)
        r = s.post(url,params={"key":key_value,"value":value})
        
        if r.status_code == 200:
            return  app.response_class(
                response=json.dumps(r.json()),
                status=r.status_code,
                mimetype='application/json'
            )
        else:
            return r.text, r.status_code

@app.route('/insertReplicas',methods=['POST'])
def insert_replicas():
    global node

    key_value = request.args.get("key")
    value = request.args.get("value")
    replica_number = int(request.args.get("replica_number"))
    
    # Check if already have this key in data
    # Only edge case if kappa >= number on nodes
    if not node.key == node.successor(key_value).key:

        # Update replica key
        node.add_replica(key_value, value, replica_number)

        if replica_number < node.kappa - 1:
            s = requests.Session()
            s.mount('http://', HTTPAdapter(max_retries=0))
            url = "http://{}:{}/insertReplicas".format(node.next_node.ip,node.next_node.port)
            r = s.post(url,params={"key":key_value,"value":value,"replica_number":replica_number + 1})
            
            return r.text

    return "Key {} & its replicas added successfully".format(key_value), 200

@app.route('/fixReplicas',methods=['PUT'])
def fix_replicas():
    global node

    initial_node = int(request.args.get("keynode"))
    hop = int(request.args.get("hop"))

    json_data = request.get_json()
    keys_of_initial_node = set(json.loads(json_data)["keys"])
    
    # Only edge case if kappa >= number on nodes
    if not node.key == initial_node:

        # Fix your replicas
        deletion_replicas = set()
        for (k,(key, value, replica_number)) in node.replicas.items():
            if replica_number > hop or (replica_number == hop and not k in keys_of_initial_node):
                if replica_number < node.kappa - 1:
                    node.add_replica(key,value,replica_number + 1)
                else:
                    deletion_replicas.add(k)
        
        for k in deletion_replicas:
            del node.replicas[k]

        if hop < node.kappa - 1:
            s = requests.Session()
            s.mount('http://', HTTPAdapter(max_retries=0))
            url = "http://{}:{}/fixReplicas".format(node.next_node.ip,node.next_node.port)
            r = s.put(url,params={"keynode":initial_node,"hop": hop + 1},json=json_data)
            
            return r.text

    return "Replication number updated.", 200

@app.route('/initfixReplicas')
def init_fix_replicas():
    global node

    if not node.next_node == None:

        # Send a list of your primary keys
        # , that weren't send to new node
        data = {"keys":list(node.data.keys())}

        s = requests.Session()
        s.mount('http://', HTTPAdapter(max_retries=0))
        url = "http://{}:{}/fixReplicas".format(node.next_node.ip,node.next_node.port)
        s.put(url,params={"keynode":node.key,"hop": 1},json=json.dumps(data))

        return "Fix Replicas Operation ended.", 200
    else:
        return "No need for Fix Replicas Sequence", 200

@app.route('/generateReplicas')
def generate_replicas():
    global node

    # Read set of existing keys
    existing = set(json.loads(request.get_json())["existing"])
    # Check primary keys
    data = {}
    for (k,(key,value)) in node.data.items():
        if k not in existing:
            data[k] = (key,value,1)
    # Check replicas
    for (k,(key,value,replica_number)) in node.replicas.items():
        if replica_number < node.kappa - 1 and k not in existing:
            data[k] = (key,value,replica_number + 1)
    
    data = {"keys":[{"key":v[0],"value":v[1],"replica_number":v[2]} for (k,v) in data.items()]}
    response = app.response_class(
        response=json.dumps(data),
        status=200,
        mimetype='application/json'
    )
    return response

@app.route('/send',methods=['POST'])
def send():
    global node        
    new_keys = json.loads(request.get_json())["keys"]
    for d in new_keys:
        node.data[d["key_hash"]] = (d["key"],d["value"])
    return "Keys transfered!", 200

@app.route('/transferKeys')
def transfer_keys():
    global node        
    keynode = int(request.args.get("keynode"))

    if node.kappa == 1:
        
        data_list = [{"key_hash":k,"key":v[0],"value":v[1]} for (k,v) in node.data.items() if k <= keynode or k > node.key]
        data = {"keys":data_list}
        
    elif node.consistency_type == "chain-replication" or node.consistency_type == "eventual-consistency":
        
        primary_keys = {k:v for (k,v) in node.data.items() if k <= keynode or k > node.key}
        replicas_keys = [{"key_hash":k,"key":v[0],"value":v[1],"replica_number":v[2]} for (k,v) in node.replicas.items()]

        # Increase replication number on 
        # your own replication dictionairy
        deletion_replicas = set()
        
        for (k,(key,value,replica_number)) in node.replicas.items():
            if replica_number < node.kappa - 1:
                node.add_replica(key, value, replica_number + 1)
            else:
                deletion_replicas.add(k)

        for k in deletion_replicas:
            del node.replicas[k]

        # Each primary key of node, that will be send
        # to new node, must be added as a replica
        for (k,(key,value)) in primary_keys.items():
            node.add_replica(key,value,1)

        # Uneccessary keys must be now deleted
        for k in primary_keys.keys():
            del node.data[k]
        
        # Format json output
        primary_keys = [{"key_hash":k,"key":v[0],"value":v[1]} for (k,v) in primary_keys.items()]
        data = {"keys":primary_keys,"replicas":replicas_keys}

    response = app.response_class(
        response=json.dumps(data),
        status=200,
        mimetype='application/json'
    )
    return response

@app.route('/deleteKeys',methods=['DELETE'])
def delete_keys():

    global node        
    keynode = request.args.get("keynode")

    if not keynode == None:
        keynode = int(keynode)
        node.data = {k:v for (k,v) in node.data.items() if not(k <= keynode or k > node.key)}

    return "Keys deleted.", 200

@app.route('/delete', methods=['DELETE'])
def delete():
    global node
    key_value = request.args.get("key")
    key = hash_key(key_value)

    if node is None:
        return "You have to join first.", 403
    
    successor = node.successor(key_value)
    if successor.key == node.key:
        if key in node.data:

            data = {
                "hash": key,
                "key": node.data[key][0],
                "value": node.data[key][1],
                "node_ip": node.ip,
                "node_port": node.port,
            }
            delete_response = response = app.response_class(
                response=json.dumps(data),
                status=200,
                mimetype='application/json'
            )
        
            del node.data[key]

            if node.kappa == 1:
                return delete_response
            else:

                url = "http://{}:{}/deleteReplicas".format(node.next_node.ip,node.next_node.port)
                params = {"key":key_value,"replica_number":1}

                if node.consistency_type == "chain-replication":
                    
                    s = requests.Session()
                    s.mount('http://', HTTPAdapter(max_retries=0))
                    r = s.delete(url,params=params)
                    
                    if r.status_code == 200:
                        return delete_response
                    else:
                        return r.text, r.status_code

                elif node.consistency_type == "eventual-consistency":
                    async_delete(url,params,{})
                    return delete_response
        else:
            return "Key not found.",404
    else:
        # Send key to successor
        s = requests.Session()
        s.mount('http://', HTTPAdapter(max_retries=0))
        url = "http://{}:{}/delete".format(successor.ip,successor.port)
        r = s.delete(url,params={"key":key_value})
        if r.status_code == 200:
            return app.response_class(
                response=json.dumps(r.json()),
                status=200,
                mimetype='application/json'
            )
        else:
            return r.text, r.status_code

@app.route('/deleteReplicas',methods=['DELETE'])
def delete_replicas():
    global node

    key_value = request.args.get("key")
    replica_number = int(request.args.get("replica_number"))
    

    # Edge case for kappa >= number of nodes
    if not node.key == node.successor(key_value).key:
        
        # Delete replica key
        del node.replicas[hash_key(key_value)]

        if replica_number < node.kappa - 1:
                
            s = requests.Session()
            s.mount('http://', HTTPAdapter(max_retries=0))
            url = "http://{}:{}/deleteReplicas".format(node.next_node.ip,node.next_node.port)
            r = s.delete(url,params={"key":key_value,"replica_number":replica_number + 1})
            
            return r.text, r.status_code
    
    return "Key '{}' & its replicas deleted.".format(key_value), 200

@app.route('/overlay')
def overlay():
    global node

    if node is None:
        return "You have to join first.", 403
    
    if node.is_bootstrap():
        data = {"nodes":[{"node_key":key,"ip":ip_port[0],"port":ip_port[1]} for key,ip_port in node.nodes.items()]}
        return app.response_class(
            response=json.dumps(data),
            status=200,
            mimetype='application/json'
        )
    else:
        url = "http://{}:{}/overlay".format(node.bnode.ip,node.bnode.port)
        r = requests.get(url)
        if r.status_code == 200:
            return app.response_class(
                response=json.dumps(r.json()),
                status=200,
                mimetype='application/json'
            )
        else:
            return r.text, r.status_code

@app.route('/info')
def info():
    global node

    if node is None:
        return "You have to join first.", 403

    if node.previous_node == None:
        p_hash, p_ip, p_port = "none", "none", "none"

    data = {
        "keys": [{"key_hash":k,"key":v[0],"value":v[1]} for (k,v) in node.data.items()],
        "replicas": [{"key_hash":k,"key":v[0],"value":v[1],"replica_num":v[2]} for (k,v) in node.replicas.items()],
        "previous": {},
        "next": {},
    }
    if node.previous_node != None:
        data["previous"] = {
            "hash": node.previous_node.key,
            "ip": node.previous_node.ip,
            "port": node.previous_node.port,
        }
        
    if node.next_node != None:
        data["next"] = { 
            "hash": node.next_node.key,
            "ip": node.next_node.ip,
            "port": node.next_node.port,
        }

    response = app.response_class(
        response=json.dumps(data),
        status=200,
        mimetype='application/json'
    )
    return response

@app.route('/shutdown', methods=['POST'])
def shutdown():
    # Notify bootstrap node
    global node
    if node == None:
        shutdown_server()
    else:
        if node.is_bootstrap():
            if node.number_of_nodes == 1:
                shutdown_server()
            else:
                for n in node.nodes:
                    url = "http://{}:{}/kickout".format(node.nodes[n][0],node.nodes[n][1])
                    requests.delete(url)
                    shutdown_server()
        else:
            url = "http://{}:{}/depart".format(node.ip,node.port)
            r = requests.delete(url)
            shutdown_server()
    return 'Server shutting down...'

def async_get(url, params, data):
    s = requests.Session()
    s.mount('http://', HTTPAdapter(max_retries=0))
    s.get(url,params=params,json = json.dumps(data))

def async_put(url, params, data):
    s = requests.Session()
    s.mount('http://', HTTPAdapter(max_retries=0))
    s.put(url,params=params,json = json.dumps(data))

def async_post(url, params, data):
    s = requests.Session()
    s.mount('http://', HTTPAdapter(max_retries=0))
    s.post(url,params=params,json = json.dumps(data))

def async_delete(url, params, data):
    s = requests.Session()
    s.mount('http://', HTTPAdapter(max_retries=0))
    s.delete(url,params=params,json = json.dumps(data))        

if __name__ == "__main__":

    if not len(sys.argv) == 4:
        print("Please provide available port number, replication factor & consistency type")
        exit()

    ip = socket.gethostbyname(socket.gethostname())
    port = int(sys.argv[1])
    kappa = int(sys.argv[2])
    consistency = sys.argv[3]
    node = None

    try:
        app.run(host=ip, port=port, threaded=True)
    except socket.error:
        print("Port {} is not available".format(sys.argv[1]))
        exit()

