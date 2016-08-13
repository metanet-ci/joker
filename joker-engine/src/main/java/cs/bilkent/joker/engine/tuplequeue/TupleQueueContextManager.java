package cs.bilkent.joker.engine.tuplequeue;

import cs.bilkent.joker.engine.config.ThreadingPreference;
import cs.bilkent.joker.engine.tuplequeue.impl.context.PartitionedTupleQueueContext;
import cs.bilkent.joker.flow.OperatorDef;


public interface TupleQueueContextManager
{

    TupleQueueContext createDefaultTupleQueueContext ( int regionId,
                                                       int replicaIndex,
                                                       OperatorDef operatorDef,
                                                       ThreadingPreference threadingPreference );

    PartitionedTupleQueueContext[] createPartitionedTupleQueueContext ( int regionId, int replicaCount, OperatorDef operatorDef );

    boolean releaseDefaultTupleQueueContext ( int regionId, int replicaIndex, String operatorId );

    boolean releasePartitionedTupleQueueContexts ( int regionId, String operatorId );

}
