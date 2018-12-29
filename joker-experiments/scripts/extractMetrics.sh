#!/bin/bash

if [ $# -lt 5 ]; then
	echo "Usage: `basename $0` directory maxProducedTupleCountPerSourceInvocation maxMultiplicationCount maxBatchSize repeat"
	exit 1
fi

dir=$1
maxProducedTupleCountPerSourceInv=$2
maxMultiplicationCount=$3
maxBatchSize=$4
repeat=$5

producedTupleCountPerSourceInv="1"

while [ $producedTupleCountPerSourceInv -le $maxProducedTupleCountPerSourceInv ]; do

    echo "source: $producedTupleCountPerSourceInv"

    multCount="0"

    while [ $multCount -le $maxMultiplicationCount ]; do

        echo "mult: $multCount"

        batchSize="1"

        while [ $batchSize -le $maxBatchSize ]; do

            echo "batch: $batchSize"

            rep="1"

            while [ $rep -le $repeat ]; do

                echo "rep: $rep"

                subDir=$dir"/source"$producedTupleCountPerSourceInv"_mult"$multCount"_batch"$batchSize"_rep"$rep

                file=$subDir"/log.txt"

                grep "TUPLE LATENCIES FOR SINK" $file | awk '{split($0,a," "); print a[18]}' > $subDir"/latency_sink_current.txt"

                grep "TUPLE LATENCIES FOR SINK" $file | awk '{split($0,a," "); print a[34]}' > $subDir"/latency_sink_historical.txt"

                grep "Queue Waiting Time: multiplier" $file | awk '{split($0,a," "); print a[18]}' > $subDir"/queue_waiting_time_current.txt"

                grep "Queue Waiting Time: multiplier" $file | awk '{split($0,a," "); print a[34]}' > $subDir"/queue_waiting_time_historical.txt"

                grep "Service Time: multiplier" $file | awk '{split($0,a," "); print a[18]}' > $subDir"/service_time_current.txt"

                grep "Service Time: multiplier" $file | awk '{split($0,a," "); print a[34]}' > $subDir"/service_time_historical.txt"

                grep -F 'MetricManagerImpl$CollectPipelineMetrics - P[1][0][0]' $file | awk '{split($0,a," "); print a[13]}' > $subDir"/thread_utilization.txt"

                grep -F 'MetricManagerImpl$CollectPipelineMetrics - P[1][0][0]' $file | awk '{split($0,a," "); print a[18]}' | cut -d "[" -f 2 | cut --d "]" -f 1 > $subDir"/throughput.txt"

                grep -F 'MetricManagerImpl$CollectPipelineMetrics - P[1][0][0]' $file | awk '{split($0,a," "); print a[24]}' | cut -d "[" -f 2 | cut --d "]" -f 1 > $subDir"/operator_cost.txt"

                grep -F 'PipelineReplicaMeter - P[1][0][0] => INPUT TUPLE COUNTS' $file |  awk '{split($0,a," "); print a[15]}' > $subDir"/average_invocation_tuple_count.txt"

                grep -F 'PipelineReplica - P[1][0][0] => CURRENT QUEUE SIZE' $file |  awk '{split($0,a," "); print a[13]}' > $subDir"/average_queue_size.txt"

                rep=`expr $rep \+ 1`
            done

            batchSize=`expr $batchSize \* 2`

        done

        if [ $multCount = "0" ]; then
            multCount="1"
        else
            multCount=`expr $multCount \* 2`
        fi

    done

    producedTupleCountPerSourceInv=`expr $producedTupleCountPerSourceInv \* 2`

done
