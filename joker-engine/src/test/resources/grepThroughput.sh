#!/bin/bash

if [[ $# -lt 3 ]]; then
	echo "Usage: `basename $0` inputFile pipelineId outputFile"
	exit 1
fi

inputFile=$1
pipelineId=$2
outputFile=$3

grep -F 'MetricManagerImpl$CollectPipelineMetrics - '${pipelineId} ${inputFile} | awk '{split($0,a," "); print a[18]}' | cut -d "[" -f 2 | cut -d "]" -f 1 > ${outputFile}

