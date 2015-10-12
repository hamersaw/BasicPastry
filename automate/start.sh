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

let NUM_NODES+=1

#start discovery node
DNODE_INDEX=$(( ( RANDOM % $NUM_NODES ) ))
INDEX=0
DNODE_MACHINE=""
for MACHINE in `cat $MACHINE_FILE`
do
	if [ "$INDEX" -eq "$DNODE_INDEX" ]
	then
		urxvt -e "ssh rammerd@madison.cs.colostate.edu && java -cp Desktop/BasicPastry.jar com.hamersaw.basic_pastry.DiscoveryNode 15605"
		#ssh -f rammerd@madison.cs.colostate.edu "nohup java -cp Desktop/BasicPastry.jar com.hamersaw.basic_pastry.DiscoveryNode 15605 > Documents/DiscoveryNode$MACHINE.txt &"
		#ssh -f rammerd@madison.cs.colostate.edu "echo $!" > "processes/$MACHINE.txt"

		echo "started discovery node on machine $MACHINE"
		DNODE_MACHINE=$MACHINE
		break
	fi

	let INDEX+=1
done

#start pastry nodes
let INDEX=0
for MACHINE in `cat $MACHINE_FILE`
do
	if [ "$INDEX" -eq "$NUM_NODES" ]
	then
		break
	elif [ "$INDEX" -ne "$DNODE_INDEX" ]
	then
		#TODO start pastry node on remote machine
		echo "TODO start pastry node on machine $MACHINE"

		#write pid to file

		sleep 5
	fi

	let INDEX+=1
done
