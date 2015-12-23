package cs.bilkent.zanza.engine.tuplequeue;

import cs.bilkent.zanza.operator.PartitionKeyExtractor;
import cs.bilkent.zanza.operator.spec.OperatorType;

public interface TupleQueueManager
{

    enum TupleQueueThreading
    {
        SINGLE_THREADED,
        MULTI_THREADED
    }

    TupleQueueContext createTupleQueueContext ( String operatorId,
                                                int inputPortCount,
                                                OperatorType operatorType,
                                                PartitionKeyExtractor partitionKeyExtractor,
                                                TupleQueueThreading tupleQueueThreading,
                                                int initialQueueCapacity );

    boolean releaseTupleQueueContext ( String operatorId );

}
