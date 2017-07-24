#!/usr/bin/env bash

if [ $# -lt 2 ]; then
	echo "Usage: `basename $0` directory regionId"
	exit 1
fi

dir=$1
regionId=$2

last=`cat $dir/last.txt`

flowVersion="0"

while [ $flowVersion -le $last ]; do
    summary=`cat ${dir}/flow${flowVersion}_summary.txt`

    if [ $? = '0' ]; then
    	thr=`cat ${dir}/flow${flowVersion}_p${regionId}_0_throughput_0.txt`

        echo $summary
        echo $thr
    fi

	flowVersion=`expr $flowVersion + 1`
done

