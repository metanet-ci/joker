package cs.bilkent.zanza.engine.tuplequeue;

import java.util.List;

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
                                                OperatorType operatorType, List<String> partitionFieldNames,
                                                TupleQueueThreading tupleQueueThreading,
                                                int initialQueueCapacity );

    boolean releaseTupleQueueContext ( String operatorId );

}
