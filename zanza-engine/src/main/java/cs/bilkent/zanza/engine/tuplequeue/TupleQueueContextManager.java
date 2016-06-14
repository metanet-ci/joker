package cs.bilkent.zanza.engine.tuplequeue;

import cs.bilkent.zanza.engine.config.ThreadingPreference;
import cs.bilkent.zanza.engine.config.ZanzaConfig;
import cs.bilkent.zanza.engine.tuplequeue.impl.context.PartitionedTupleQueueContext;
import cs.bilkent.zanza.flow.OperatorDefinition;


public interface TupleQueueContextManager
{

    void init ( ZanzaConfig config );

    TupleQueueContext createDefaultTupleQueueContext ( int regionId,
                                                       int replicaIndex,
                                                       OperatorDefinition operatorDefinition,
                                                       ThreadingPreference threadingPreference );

    PartitionedTupleQueueContext[] createPartitionedTupleQueueContext ( int regionId,
                                                                        int replicaCount, OperatorDefinition operatorDefinition );

    boolean releaseDefaultTupleQueueContext ( int regionId, int replicaIndex, String operatorId );

    boolean releasePartitionedTupleQueueContexts ( int regionId, String operatorId );

}
