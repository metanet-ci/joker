package cs.bilkent.zanza.engine.tuplequeue.impl.factory;

import static com.google.common.base.Preconditions.checkArgument;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueueDrainer;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueueDrainerFactory;
import cs.bilkent.zanza.engine.tuplequeue.impl.drainer.BlockingMultiPortConjunctiveDrainer;
import cs.bilkent.zanza.engine.tuplequeue.impl.drainer.BlockingMultiPortDisjunctiveDrainer;
import cs.bilkent.zanza.engine.tuplequeue.impl.drainer.GreedyDrainer;
import cs.bilkent.zanza.scheduling.ScheduleWhenAvailable;
import cs.bilkent.zanza.scheduling.ScheduleWhenTuplesAvailable;
import static cs.bilkent.zanza.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByCount.AT_LEAST_BUT_SAME_ON_ALL_PORTS;
import static cs.bilkent.zanza.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByPort.AVAILABLE_ON_ALL_PORTS;
import static cs.bilkent.zanza.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByPort.AVAILABLE_ON_ANY_PORT;
import cs.bilkent.zanza.scheduling.SchedulingStrategy;

public class BlockingMultiPortDrainerFactory implements TupleQueueDrainerFactory
{

    private final int timeoutInMillis;

    public BlockingMultiPortDrainerFactory ( final int timeoutInMillis )
    {
        this.timeoutInMillis = timeoutInMillis;
    }

    @Override
    public TupleQueueDrainer create ( final SchedulingStrategy input )
    {
        if ( input instanceof ScheduleWhenTuplesAvailable )
        {
            ScheduleWhenTuplesAvailable strategy = (ScheduleWhenTuplesAvailable) input;
            checkArgument( !( strategy.getTupleAvailabilityByPort() == AVAILABLE_ON_ANY_PORT
                              && strategy.getTupleAvailabilityByCount() == AT_LEAST_BUT_SAME_ON_ALL_PORTS ) );
            if ( strategy.getTupleAvailabilityByPort() == AVAILABLE_ON_ALL_PORTS )
            {
                return new BlockingMultiPortConjunctiveDrainer( strategy.getTupleAvailabilityByCount(),
                                                                strategy.getTupleCountByPortIndex(),
                                                                timeoutInMillis );
            }
            else
            {
                return new BlockingMultiPortDisjunctiveDrainer( strategy.getTupleAvailabilityByCount(),
                                                                strategy.getTupleCountByPortIndex(),
                                                                timeoutInMillis );
            }
        }
        else if ( input instanceof ScheduleWhenAvailable )
        {
            return new GreedyDrainer();
        }

        throw new IllegalArgumentException( input.getClass() + " is not supported yet!" );
    }

}
