package cs.bilkent.joker.engine.tuplequeue;

import cs.bilkent.joker.engine.config.ThreadingPreference;
import cs.bilkent.joker.engine.tuplequeue.impl.context.PartitionedTupleQueueContext;
import cs.bilkent.joker.operator.OperatorDef;


public interface TupleQueueContextManager
{

    TupleQueueContext createDefaultTupleQueueContext ( int regionId,
                                                       int replicaIndex,
                                                       OperatorDef operatorDef,
                                                       ThreadingPreference threadingPreference );

    default PartitionedTupleQueueContext[] createPartitionedTupleQueueContext ( int regionId, int replicaCount, OperatorDef operatorDef )
    {
        return createPartitionedTupleQueueContext( regionId, replicaCount, operatorDef, operatorDef.partitionFieldNames().size() );
    }

    PartitionedTupleQueueContext[] createPartitionedTupleQueueContext ( int regionId,
                                                                        int replicaCount,
                                                                        OperatorDef operatorDef,
                                                                        int forwardKeyLimit );

    boolean releaseDefaultTupleQueueContext ( int regionId, int replicaIndex, String operatorId );

    boolean releasePartitionedTupleQueueContexts ( int regionId, String operatorId );

    TupleQueueContext switchThreadingPreference ( int regionId, int replicaIndex, String operatorId );
}
