package cs.bilkent.joker.engine.tuplequeue;

import cs.bilkent.joker.operator.scheduling.SchedulingStrategy;

public interface TupleQueueDrainerPool
{

    TupleQueueDrainer acquire ( SchedulingStrategy strategy );

    void release ( TupleQueueDrainer drainer );

}
