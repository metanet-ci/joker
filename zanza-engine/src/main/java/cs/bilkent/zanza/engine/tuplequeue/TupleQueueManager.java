package cs.bilkent.zanza.engine.tuplequeue;

import cs.bilkent.zanza.engine.config.ThreadingOption;
import cs.bilkent.zanza.flow.OperatorDefinition;


public interface TupleQueueManager
{

    TupleQueueContext createTupleQueueContext ( OperatorDefinition operatorDefinition, ThreadingOption threadingOption, int replicaIndex );

    boolean releaseTupleQueueContext ( String operatorId, int replicaIndex );

}
