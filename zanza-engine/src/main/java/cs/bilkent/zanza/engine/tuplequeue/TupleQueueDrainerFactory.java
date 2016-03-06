package cs.bilkent.zanza.engine.tuplequeue;

import cs.bilkent.zanza.operator.scheduling.SchedulingStrategy;

public interface TupleQueueDrainerFactory
{

    TupleQueueDrainer create ( SchedulingStrategy strategy );

}
