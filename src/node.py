#!/usr/bin/env python

import hashlib

def modulo(x, y):
    # when y is power of 2
    return x & (y - 1)

def hash_key(s):
    return modulo(int(hashlib.sha1(str.encode(s)).hexdigest(),16), 1 << 160)

class ReferenceNode():
    def __init__(self,ip,port):
        self.ip = ip
        self.port = port
        self.key = hash_key("{}:{}".format(ip, port))

class Node():
    data = {}
    replicas = {}
    next_node = None
    previous_node = None

    def __init__(self, ip, port, bnode, kfactor = 1, consistency_type = "eventually"):
        self.ip = ip
        self.port = port
        self.key = hash_key("{}:{}".format(ip, port))
        self.bnode = ReferenceNode(bnode[0],bnode[1])
        self.kfactor = kfactor
        self.consistency_type = consistency_type

    def is_bootstrap(self):
        return self.key == self.bnode.key

    def add_key(self, key, value):

        key_hash = hash_key(key)
        self.data[key_hash] = (key,value)

        return key_hash

    def add_replica(self, key, value, replica_number):

        key_hash = hash_key(key)
        self.replicas[key_hash] = (key,value, replica_number)

        return key_hash

    def successor(self,key_value):

        key = hash_key(key_value)

        if self.next_node == None or self.previous_node == None:
            return ReferenceNode(self.ip, self.port)

        if self.previous_node.key < self.next_node.key:
            # Inner node
            if key <= self.previous_node.key:
                return self.previous_node
            elif key <= self.key:
                return ReferenceNode(self.ip, self.port)
            else:
                return self.next_node
        else:
            # Edge node
            if self.key > self.previous_node.key:
                # Greates edge node
                if key > self.key or key <= self.next_node.key:
                    return self.next_node
                elif key <= self.previous_node.key:
                    return self.previous_node
                else:
                    return ReferenceNode(self.ip, self.port)
            else:
                # Lowest Edge node
                if key <= self.key or key > self.previous_node.key:
                    return ReferenceNode(self.ip, self.port)
                else:
                    return self.next_node

class BootstrapNode(Node):

    nodes = {}
    number_of_nodes = 0

    def __init__(self, ip, port, kfactor = 1, consistency_type = "chain-replication"):
        super().__init__(ip, port, (ip,port), kfactor, consistency_type)
        self.nodes[self.key] = (ip, port)
        self.number_of_nodes += 1
        
    def add_node(self, ip, port):
        keynode = hash_key("{}:{}".format(ip, port))
        if not keynode in self.nodes:
            self.nodes[keynode] = (ip, port)
            self.number_of_nodes += 1
            return keynode
        else:
            return -1

    def next_index(self, index):
        return (index + 1) % self.number_of_nodes
    def previous_index(self, index):
        return (index - 1) % self.number_of_nodes

    def find_neighboors(self, keynode):
        temp = list(self.nodes.keys())
        temp.sort()
        for index, key in enumerate(temp):
            if key == keynode:
                return self.nodes[temp[self.previous_index(index)]], self.nodes[temp[self.next_index(index)]]

    def delete_node(self, keynode):
        if keynode in self.nodes:
            self.number_of_nodes -= 1
            del self.nodes[keynode]
            return keynode
        else:
            return -1
            
