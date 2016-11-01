package cs.bilkent.joker.engine.tuplequeue;

import cs.bilkent.joker.engine.config.ThreadingPreference;
import cs.bilkent.joker.engine.tuplequeue.impl.operator.PartitionedOperatorTupleQueue;
import cs.bilkent.joker.operator.OperatorDef;


public interface OperatorTupleQueueManager
{

    OperatorTupleQueue createDefaultOperatorTupleQueue ( int regionId,
                                                         int replicaIndex,
                                                         OperatorDef operatorDef,
                                                         ThreadingPreference threadingPreference );

    default PartitionedOperatorTupleQueue[] createPartitionedOperatorTupleQueue ( int regionId, int replicaCount, OperatorDef operatorDef )
    {
        return createPartitionedOperatorTupleQueue( regionId, replicaCount, operatorDef, operatorDef.partitionFieldNames().size() );
    }

    PartitionedOperatorTupleQueue[] createPartitionedOperatorTupleQueue ( int regionId,
                                                                          int replicaCount,
                                                                          OperatorDef operatorDef,
                                                                          int forwardKeyLimit );

    boolean releaseDefaultOperatorTupleQueue ( int regionId, int replicaIndex, String operatorId );

    boolean releasePartitionedOperatorTupleQueue ( int regionId, String operatorId );

    OperatorTupleQueue switchThreadingPreference ( int regionId, int replicaIndex, String operatorId );
}
