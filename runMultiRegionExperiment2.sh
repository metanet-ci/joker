#!/bin/bash

if [ $# -lt 5 ]; then
	echo "Usage: `basename $0` directory operatorCostFile pipelineSplitEnabled? regionRebalanceEnabled? pipelineSplitFirst?"
	exit 1
fi

dir=$1
operatorCostFile=$2
pipelineSplitEnabled=$3
regionRebalanceEnabled=$4
pipelineSplitFirst=$5

cat $operatorCostFile | while read line ; do
    upstreamCosts=`echo $line | cut -f1 -d ' '`
    downstreamCosts=`echo $line | cut -f2 -d ' '`

    echo $upstreamCosts
    echo $downstreamCosts

    outputDir=$dir"/"$upstreamCosts"__"$downstreamCosts
	mkdir $outputDir

	if [ $? != '0' ]; then
	    echo "$dir does not exist!!!"
	    exit 1
    fi

    echo $line

	J="_"
	java -Xmx4G -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps -XX:+PrintGCDetails -Xloggc:$outputDir"/gc.log" -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=$outputDir"/heap.hprof" -cp joker-experiments-0.1.jar -DflowFactory=cs.bilkent.joker.experiment.MultiRegionFlowDefFactory -DtuplesPerKey=16 -Djoker.engine.tupleQueueManager.maxDrainableKeyCount=4096 -Djoker.engine.tupleQueueManager.partitionedTupleQueueDrainHint=256 -DvizPath=joker-engine/viz.py -DreportDir=$outputDir -DoperatorCosts1=$upstreamCosts -DoperatorCosts2=$downstreamCosts -Djoker.engine.adaptation.pipelineSplitEnabled=$pipelineSplitEnabled -Djoker.engine.adaptation.regionRebalanceEnabled=$regionRebalanceEnabled -Djoker.engine.adaptation.pipelineSplitFirst=$pipelineSplitFirst cs.bilkent.joker.experiment.ExperimentRunner

	if [ $? != '0' ]; then
		echo "JAVA command failed!!!"
		exit 1
	fi

done
