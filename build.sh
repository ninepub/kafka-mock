#!/bin/bash

if [ "$1" = "" ]; then
	echo "Version is empty"
	exit 0
fi

docker build -t "kevin-monteiro/kafka-mock:$1" .