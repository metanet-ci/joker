package cs.bilkent.zanza.engine.tuplequeue;

import cs.bilkent.zanza.scheduling.SchedulingStrategy;

public interface TupleQueueDrainerFactory
{

    TupleQueueDrainer create ( SchedulingStrategy strategy );

}
