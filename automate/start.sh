#!/bin/bash

#read in command line arguments
MACHINE_FILE=""
NUM_NODES=25
while getopts 'hf:n:' flag; do
	case "${flag}" in

		f)
			MACHINE_FILE="${OPTARG}"
			;;
		h)
			echo "Usage: $0 -f machine-file -n num-nodes"
			exit 0
			;;
		n)
			NUM_NODES="${OPTARG}"
			;;
		\?)
			echo "Invalid option: -$OPTARG" >&2
			exit 1
			;;
		:)
			echo "Option -$OPTARG requires an argument." >&2
			exit 1
			;;
	esac
done

if [[ -z "$MACHINE_FILE" ]]
then
	echo "No machine file supplied. Please provide one with -f option."
	exit 1
fi

#TODO startup discovery node on remove machine

for line in `cat $MACHINE_FILE`
do
	if [ "$NUM_NODES" -le "0" ]
	then
		break
	fi

	#TODO start pastry node on remote machine
	echo $line

	let NUM_NODES-=1
done
