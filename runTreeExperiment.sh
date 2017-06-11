#!/bin/bash

if [ $# -lt 6 ]; then
	echo "Usage: `basename $0` directory maxOperatorCost operatorCount pipelineSplitEnabled? regionRebalanceEnabled? pipelineSplitFirst?"
	exit 1
fi

dir=$1
maxOperatorCost=$2
operatorCount=$3
pipelineSplitEnabled=$4
regionRebalanceEnabled=$5
pipelineSplitFirst=$6

operatorCost="1"

while [ $operatorCost -le $maxOperatorCost ]; do

    outputDir=$dir"/"$operatorCost
	mkdir $outputDir

	if [ $? != '0' ]; then
	    echo "$dir does not exist!!!"
	    exit 1
    fi

	operatorCostsStr=$operatorCost
	L=`expr $operatorCount - 1`
	echo $L
	for i in $(seq 1 $L);
    do
        operatorCostsStr=$operatorCostsStr"_"$operatorCost
    done

    echo $operatorCostsStr

	J="_" 
	java -Xmx4G -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps -XX:+PrintGCDetails -Xloggc:$outputDir"/gc.log" -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=$outputDir"/heap.hprof" -cp joker-experiments/target/joker-experiments-0.1.jar -DflowFactory=cs.bilkent.joker.experiment.TreeFlowDefFactory -DtuplesPerKey=16 -Djoker.engine.tupleQueueManager.maxDrainableKeyCount=4096 -Djoker.engine.tupleQueueManager.partitionedTupleQueueDrainHint=256 -DvizPath=joker-engine/viz.py -DreportDir=$outputDir -DoperatorCostsDown1=$operatorCostsStr -DoperatorCostsDown2=$operatorCostsStr -DoperatorCostsUp=$operatorCostsStr -Djoker.engine.adaptation.pipelineSplitEnabled=$pipelineSplitEnabled -Djoker.engine.adaptation.regionRebalanceEnabled=$regionRebalanceEnabled -Djoker.engine.adaptation.pipelineSplitFirst=$pipelineSplitFirst cs.bilkent.joker.experiment.ExperimentRunner

	if [ $? != '0' ]; then
		echo "JAVA command failed!!!"
		exit 1
	fi

	operatorCost=`expr $operatorCost \* 2`

done
