#!/bin/bash

for i in 0 1 2 3
do
	rsync -av -e ssh --exclude='~/src/__pycache__/*' ~/src/ ubuntu@node$i:.
done