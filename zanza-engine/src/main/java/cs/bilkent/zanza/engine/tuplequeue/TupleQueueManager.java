package cs.bilkent.zanza.engine.tuplequeue;

import cs.bilkent.zanza.engine.config.ThreadingPreference;
import cs.bilkent.zanza.flow.OperatorDefinition;


public interface TupleQueueManager
{

    TupleQueueContext createTupleQueueContext ( OperatorDefinition operatorDefinition,
                                                ThreadingPreference threadingPreference,
                                                int replicaIndex );

    boolean releaseTupleQueueContext ( String operatorId, int replicaIndex );

}
