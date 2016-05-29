package cs.bilkent.zanza.engine.tuplequeue.impl.drainer.pool;

import javax.annotation.concurrent.NotThreadSafe;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import cs.bilkent.zanza.engine.config.ZanzaConfig;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueueDrainer;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueueDrainerPool;
import cs.bilkent.zanza.engine.tuplequeue.impl.drainer.GreedyDrainer;
import cs.bilkent.zanza.engine.tuplequeue.impl.drainer.NonBlockingMultiPortConjunctiveDrainer;
import cs.bilkent.zanza.engine.tuplequeue.impl.drainer.NonBlockingMultiPortDisjunctiveDrainer;
import cs.bilkent.zanza.engine.tuplequeue.impl.drainer.NonBlockingSinglePortDrainer;
import cs.bilkent.zanza.flow.OperatorDefinition;
import static cs.bilkent.zanza.flow.Port.DEFAULT_PORT_INDEX;
import cs.bilkent.zanza.operator.scheduling.ScheduleWhenAvailable;
import cs.bilkent.zanza.operator.scheduling.ScheduleWhenTuplesAvailable;
import static cs.bilkent.zanza.operator.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByCount.AT_LEAST_BUT_SAME_ON_ALL_PORTS;
import static cs.bilkent.zanza.operator.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByPort.AVAILABLE_ON_ALL_PORTS;
import static cs.bilkent.zanza.operator.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByPort.AVAILABLE_ON_ANY_PORT;
import cs.bilkent.zanza.operator.scheduling.SchedulingStrategy;

@NotThreadSafe
public class NonBlockingTupleQueueDrainerPool implements TupleQueueDrainerPool
{

    private final int inputPortCount;

    private NonBlockingSinglePortDrainer singlePortDrainer;

    private NonBlockingMultiPortConjunctiveDrainer multiPortConjunctiveDrainer;

    private NonBlockingMultiPortDisjunctiveDrainer multiPortDisjunctiveDrainer;

    private GreedyDrainer greedyDrainer;

    private TupleQueueDrainer active;

    public NonBlockingTupleQueueDrainerPool ( final OperatorDefinition operatorDefinition )
    {
        inputPortCount = operatorDefinition.inputPortCount();
    }

    @Override
    public void init ( final ZanzaConfig config )
    {
        final int maxBatchSize = config.getTupleQueueDrainerConfig().getMaxBatchSize();
        singlePortDrainer = new NonBlockingSinglePortDrainer( maxBatchSize );
        multiPortConjunctiveDrainer = new NonBlockingMultiPortConjunctiveDrainer( inputPortCount, maxBatchSize );
        multiPortDisjunctiveDrainer = new NonBlockingMultiPortDisjunctiveDrainer( inputPortCount, maxBatchSize );
        greedyDrainer = new GreedyDrainer( inputPortCount );
    }

    @Override
    public TupleQueueDrainer acquire ( final SchedulingStrategy input )
    {
        checkState( active == null );

        if ( input instanceof ScheduleWhenAvailable )
        {
            active = greedyDrainer;
        }
        else if ( input instanceof ScheduleWhenTuplesAvailable )
        {
            final ScheduleWhenTuplesAvailable strategy = (ScheduleWhenTuplesAvailable) input;

            if ( inputPortCount == 1 )
            {
                active = singlePortDrainer;
                singlePortDrainer.setParameters( strategy.getTupleAvailabilityByCount(), strategy.getTupleCount( DEFAULT_PORT_INDEX ) );
            }
            else
            {
                checkArgument( !( strategy.getTupleAvailabilityByPort() == AVAILABLE_ON_ANY_PORT
                                  && strategy.getTupleAvailabilityByCount() == AT_LEAST_BUT_SAME_ON_ALL_PORTS ) );
                if ( strategy.getTupleAvailabilityByPort() == AVAILABLE_ON_ALL_PORTS )
                {
                    active = multiPortConjunctiveDrainer;
                    multiPortConjunctiveDrainer.setParameters( strategy.getTupleAvailabilityByCount(), strategy.getTupleCounts() );
                }
                else
                {
                    active = multiPortDisjunctiveDrainer;
                    multiPortDisjunctiveDrainer.setParameters( strategy.getTupleAvailabilityByCount(), strategy.getTupleCounts() );
                }
            }
        }
        else
        {
            throw new IllegalArgumentException( input.getClass() + " is not supported yet!" );
        }

        return active;
    }

    @Override
    public void release ( final TupleQueueDrainer drainer )
    {
        checkArgument( active == drainer, "active: " + active + " drainer: " + drainer );
        active.reset();
        active = null;
    }
}
