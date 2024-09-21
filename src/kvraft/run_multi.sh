#/bin/bash


order=$1
times=$2

for ((i = 0; i < $times; i++))
do
	$order
done

