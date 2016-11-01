package cs.bilkent.joker.engine.tuplequeue;

import cs.bilkent.joker.engine.config.ThreadingPreference;
import cs.bilkent.joker.engine.partition.PartitionDistribution;
import cs.bilkent.joker.engine.tuplequeue.impl.operator.PartitionedOperatorTupleQueue;
import cs.bilkent.joker.operator.OperatorDef;


public interface OperatorTupleQueueManager
{

    OperatorTupleQueue createDefaultOperatorTupleQueue ( int regionId,
                                                         int replicaIndex,
                                                         OperatorDef operatorDef,
                                                         ThreadingPreference threadingPreference );

    default PartitionedOperatorTupleQueue[] createPartitionedOperatorTupleQueue ( int regionId,
                                                                                  OperatorDef operatorDef,
                                                                                  PartitionDistribution partitionDistribution )
    {
        return createPartitionedOperatorTupleQueue( regionId,
                                                    partitionDistribution,
                                                    operatorDef,
                                                    operatorDef.partitionFieldNames().size() );
    }

    PartitionedOperatorTupleQueue[] createPartitionedOperatorTupleQueue ( int regionId, PartitionDistribution partitionDistribution,
                                                                          OperatorDef operatorDef,
                                                                          int forwardKeyLimit );

    void releaseDefaultOperatorTupleQueue ( int regionId, int replicaIndex, String operatorId );

    void releasePartitionedOperatorTupleQueue ( int regionId, String operatorId );

    OperatorTupleQueue switchThreadingPreference ( int regionId, int replicaIndex, String operatorId );
}
