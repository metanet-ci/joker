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

                outputDir=$dir"/source"$producedTupleCountPerSourceInv"_mult"$multCount"_batch"$batchSize"_rep"$rep
                mkdir $outputDir

                if [ $? != '0' ]; then
                    echo "output dir: $outputDir not created"
                    exit 1
                fi

                java -Xmx4G -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps -XX:+PrintGCDetails -Xloggc:$outputDir"/gc.log" -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=$outputDir"/heap.hprof" -cp joker-experiments-0.1.jar  -DproducedTupleCountPerSourceInvocation=$producedTupleCountPerSourceInv -DmultiplicationCount=$multCount -DmapperOperatorBatchSize=$batchSize -DenforceThreadAffinity cs.bilkent.joker.experiment.LatencyTestMain >> $outputDir"/log.txt"

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
