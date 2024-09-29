#/bin/bash


order=$1
times=$2
output=$3

for ((i = 0; i < $times; i++))
do
	if [[ -n "$output" ]]; then 
		fail=$($order | tee $output | sed -n '/FAIL/p')
		if [[ -n "$fail" ]];then
			echo "FAIL"
			exit
		fi
	else 
		$order
	fi
done