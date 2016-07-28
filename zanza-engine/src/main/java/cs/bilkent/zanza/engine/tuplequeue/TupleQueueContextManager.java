package cs.bilkent.zanza.engine.tuplequeue;

import cs.bilkent.zanza.engine.config.ThreadingPreference;
import cs.bilkent.zanza.engine.tuplequeue.impl.context.PartitionedTupleQueueContext;
import cs.bilkent.zanza.flow.OperatorDef;


public interface TupleQueueContextManager
{

    TupleQueueContext createDefaultTupleQueueContext ( int regionId, int replicaIndex, OperatorDef operatorDef,
                                                       ThreadingPreference threadingPreference );

    PartitionedTupleQueueContext[] createPartitionedTupleQueueContext ( int regionId, int replicaCount, OperatorDef operatorDef );

    boolean releaseDefaultTupleQueueContext ( int regionId, int replicaIndex, String operatorId );

    boolean releasePartitionedTupleQueueContexts ( int regionId, String operatorId );

}
