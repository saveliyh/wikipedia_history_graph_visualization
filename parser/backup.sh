#!/bin/bash

[ 0$1 -gt 0 ] || exit

while true; do
	rm -r ./backup
	skyd backup --to ./backup --type direct --allow-dirty
	sleep $1
done	
