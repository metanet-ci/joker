#!/usr/bin/env bash


if [ $# -lt 6 ]; then
	echo "Usage: `basename $0` cpuCount cpuLoad cpuLoadSlice timeout minDelay maxDelay?"
	exit 1
fi

cpuCount=$1
cpuLoad=$2
cpuLoadSlice=$3
timeout=$4
minDelay=$5
maxDelay=$6

randomDelayRange=$((maxDelay - minDelay))

while :
do
	delay=$(( ( RANDOM % $randomDelayRange )  + $minDelay ))
    echo $delay

    stress-ng --cpu ${cpuCount} --cpu-load ${cpuLoad} --cpu-load-slice ${cpuLoadSlice} --timeout ${timeout}s --verbose

    sleep $delay
done
