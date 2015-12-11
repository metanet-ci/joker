package cs.bilkent.zanza.engine.tuplequeue;

import cs.bilkent.zanza.operator.OperatorType;
import cs.bilkent.zanza.operator.PartitionKeyExtractor;

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
