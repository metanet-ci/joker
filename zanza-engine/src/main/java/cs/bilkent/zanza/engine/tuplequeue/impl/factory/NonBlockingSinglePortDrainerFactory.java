package cs.bilkent.zanza.engine.tuplequeue.impl.factory;

import cs.bilkent.zanza.engine.tuplequeue.TupleQueueDrainer;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueueDrainerFactory;
import cs.bilkent.zanza.engine.tuplequeue.impl.drainer.GreedyDrainer;
import cs.bilkent.zanza.engine.tuplequeue.impl.drainer.NonBlockingSinglePortDrainer;
import cs.bilkent.zanza.scheduling.ScheduleWhenAvailable;
import cs.bilkent.zanza.scheduling.ScheduleWhenTuplesAvailable;
import cs.bilkent.zanza.scheduling.SchedulingStrategy;

public class NonBlockingSinglePortDrainerFactory implements TupleQueueDrainerFactory
{

    @Override
    public TupleQueueDrainer create ( final SchedulingStrategy input )
    {
        if ( input instanceof ScheduleWhenTuplesAvailable )
        {
            final ScheduleWhenTuplesAvailable strategy = (ScheduleWhenTuplesAvailable) input;
            return new NonBlockingSinglePortDrainer( strategy.getTupleCount( 0 ), strategy.getTupleAvailabilityByCount() );
        }
        else if ( input instanceof ScheduleWhenAvailable )
        {
            return new GreedyDrainer();
        }

        throw new IllegalArgumentException( input.getClass() + " is not supported yet!" );
    }

}
