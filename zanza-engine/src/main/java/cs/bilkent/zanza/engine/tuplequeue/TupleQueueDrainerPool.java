package cs.bilkent.zanza.engine.tuplequeue;

import cs.bilkent.zanza.operator.scheduling.SchedulingStrategy;

public interface TupleQueueDrainerPool
{

    TupleQueueDrainer acquire ( SchedulingStrategy strategy );

    void release ( TupleQueueDrainer drainer );

}
