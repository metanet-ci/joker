#!/usr/bin/env bash

if [ $# -lt 9 ]; then
	echo "Usage: `basename $0` directory operatorCost operatorCount pipelineSplitEnabled? regionRebalanceEnabled? pipelineSplitFirst? metricsReadingPeriod warmUpPeriods historySize"
	exit 1
fi

outputDir=$1
operatorCost=$2
operatorCount=$3
pipelineSplitEnabled=$4
regionRebalanceEnabled=$5
pipelineSplitFirst=$6
metricsReadingPeriod=$7
warmUpPeriods=$8
historySize=$9

ls $outputDir

if [ $? != '0' ]; then
    exit 1
fi

operatorCostsStr=$operatorCost
	L=`expr $operatorCount - 1`

	for i in $(seq 1 $L);
    do
        operatorCostsStr=$operatorCostsStr"_"$operatorCost
    done

java -Xmx4G -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps -XX:+PrintGCDetails -Xloggc:$outputDir"/gc.log" -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=$outputDir"/heap.hprof" -cp joker-experiments-0.1.jar -DflowFactory=cs.bilkent.joker.experiment.SingleRegionFlowDefFactory -DtuplesPerKey=16 -Djoker.engine.tupleQueueManager.maxDrainableKeyCount=4096 -Djoker.engine.tupleQueueManager.partitionedTupleQueueDrainHint=256 -DvizPath=viz.py -DreportDir=$outputDir -DoperatorCosts=$operatorCostsStr -Djoker.engine.adaptation.pipelineSplitEnabled=$pipelineSplitEnabled -Djoker.engine.adaptation.regionRebalanceEnabled=$regionRebalanceEnabled -Djoker.engine.adaptation.pipelineSplitFirst=$pipelineSplitFirst -Djoker.engine.metricManager.pipelineMetricsScanningPeriodInMillis=$metricsReadingPeriod -Djoker.engine.metricManager.warmupIterations=$warmUpPeriods -Djoker.engine.metricManager.historySize=$historySize  cs.bilkent.joker.experiment.ExperimentRunner
