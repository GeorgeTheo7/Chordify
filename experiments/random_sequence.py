#!/usr/bin/env python

import random

with open("nodes.txt","w") as f:
    for _ in range(500):
        x = random.randrange(0, 5)
        port = random.randrange(5000,5002)
        if x == 4:
            f.write("main-node {}\n".format(port))
        else:
            f.write("node{} {}\n".format(x,port))