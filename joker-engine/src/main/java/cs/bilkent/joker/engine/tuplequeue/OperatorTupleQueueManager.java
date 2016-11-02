package cs.bilkent.joker.engine.tuplequeue;

import cs.bilkent.joker.engine.config.ThreadingPreference;
import cs.bilkent.joker.engine.partition.PartitionDistribution;
import cs.bilkent.joker.operator.OperatorDef;


public interface OperatorTupleQueueManager
{

    OperatorTupleQueue createDefaultOperatorTupleQueue ( int regionId,
                                                         int replicaIndex,
                                                         OperatorDef operatorDef,
                                                         ThreadingPreference threadingPreference );

    default OperatorTupleQueue[] createPartitionedOperatorTupleQueues ( int regionId,
                                                                        OperatorDef operatorDef,
                                                                        PartitionDistribution partitionDistribution )
    {
        return createPartitionedOperatorTupleQueues( regionId,
                                                     operatorDef,
                                                     partitionDistribution,
                                                     operatorDef.partitionFieldNames().size() );
    }

    OperatorTupleQueue[] createPartitionedOperatorTupleQueues ( int regionId,
                                                                OperatorDef operatorDef,
                                                                PartitionDistribution partitionDistribution,
                                                                int forwardKeyLimit );

    OperatorTupleQueue[] rebalancePartitionedOperatorTupleQueues ( int regionId,
                                                                   OperatorDef operatorDef,
                                                                   PartitionDistribution currentPartitionDistribution,
                                                                   PartitionDistribution newPartitionDistribution );

    void releaseDefaultOperatorTupleQueue ( int regionId, int replicaIndex, String operatorId );

    void releasePartitionedOperatorTupleQueues ( int regionId, String operatorId );

    OperatorTupleQueue switchThreadingPreference ( int regionId, int replicaIndex, String operatorId );
}
