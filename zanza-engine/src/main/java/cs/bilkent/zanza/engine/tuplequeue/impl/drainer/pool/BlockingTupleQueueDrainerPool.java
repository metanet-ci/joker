package cs.bilkent.zanza.engine.tuplequeue.impl.drainer.pool;

import javax.annotation.concurrent.NotThreadSafe;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import cs.bilkent.zanza.engine.config.ZanzaConfig;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueueDrainer;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueueDrainerPool;
import cs.bilkent.zanza.engine.tuplequeue.impl.drainer.BlockingMultiPortConjunctiveDrainer;
import cs.bilkent.zanza.engine.tuplequeue.impl.drainer.BlockingMultiPortDisjunctiveDrainer;
import cs.bilkent.zanza.engine.tuplequeue.impl.drainer.BlockingSinglePortDrainer;
import cs.bilkent.zanza.engine.tuplequeue.impl.drainer.GreedyDrainer;
import cs.bilkent.zanza.flow.OperatorDef;
import static cs.bilkent.zanza.flow.Port.DEFAULT_PORT_INDEX;
import cs.bilkent.zanza.operator.scheduling.ScheduleWhenAvailable;
import cs.bilkent.zanza.operator.scheduling.ScheduleWhenTuplesAvailable;
import static cs.bilkent.zanza.operator.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByCount.AT_LEAST_BUT_SAME_ON_ALL_PORTS;
import static cs.bilkent.zanza.operator.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByPort.ALL_PORTS;
import static cs.bilkent.zanza.operator.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByPort.ANY_PORT;
import cs.bilkent.zanza.operator.scheduling.SchedulingStrategy;

@NotThreadSafe
public class BlockingTupleQueueDrainerPool implements TupleQueueDrainerPool
{

    private final int inputPortCount;

    private BlockingSinglePortDrainer singlePortDrainer;

    private BlockingMultiPortConjunctiveDrainer multiPortConjunctiveDrainer;

    private BlockingMultiPortDisjunctiveDrainer multiPortDisjunctiveDrainer;

    private GreedyDrainer greedyDrainer;

    private TupleQueueDrainer active;

    public BlockingTupleQueueDrainerPool ( final ZanzaConfig config, final OperatorDef operatorDef )
    {
        this.inputPortCount = operatorDef.inputPortCount();

        final int maxBatchSize = config.getTupleQueueDrainerConfig().getMaxBatchSize();
        final long timeoutInMillis = config.getTupleQueueDrainerConfig().getDrainTimeoutInMillis();

        if ( inputPortCount == 1 )
        {
            this.singlePortDrainer = new BlockingSinglePortDrainer( maxBatchSize, timeoutInMillis );
        }
        else if ( inputPortCount > 1 )
        {
            this.multiPortConjunctiveDrainer = new BlockingMultiPortConjunctiveDrainer( inputPortCount, maxBatchSize, timeoutInMillis );
            this.multiPortDisjunctiveDrainer = new BlockingMultiPortDisjunctiveDrainer( inputPortCount, maxBatchSize, timeoutInMillis );
        }

        this.greedyDrainer = new GreedyDrainer( inputPortCount );
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
                singlePortDrainer.setParameters( strategy.getTupleAvailabilityByCount(), strategy.getTupleCount( DEFAULT_PORT_INDEX ) );
                active = singlePortDrainer;
            }
            else
            {
                checkArgument( !( strategy.getTupleAvailabilityByPort() == ANY_PORT
                                  && strategy.getTupleAvailabilityByCount() == AT_LEAST_BUT_SAME_ON_ALL_PORTS ) );
                final int[] inputPorts = new int[ inputPortCount ];
                for ( int i = 0; i < inputPortCount; i++ )
                {
                    inputPorts[ i ] = i;
                }
                if ( strategy.getTupleAvailabilityByPort() == ALL_PORTS )
                {
                    multiPortConjunctiveDrainer.setParameters( strategy.getTupleAvailabilityByCount(),
                                                               inputPorts,
                                                               strategy.getTupleCounts() );
                    active = multiPortConjunctiveDrainer;
                }
                else
                {
                    multiPortDisjunctiveDrainer.setParameters( strategy.getTupleAvailabilityByCount(),
                                                               inputPorts,
                                                               strategy.getTupleCounts() );
                    active = multiPortDisjunctiveDrainer;
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
        checkArgument( active == drainer );
        active.reset();
        active = null;
    }

}
