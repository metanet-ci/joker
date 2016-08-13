package cs.bilkent.joker.engine.tuplequeue.impl.drainer.pool;

import javax.annotation.concurrent.NotThreadSafe;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import cs.bilkent.joker.engine.config.JokerConfig;
import cs.bilkent.joker.engine.tuplequeue.TupleQueueDrainer;
import cs.bilkent.joker.engine.tuplequeue.TupleQueueDrainerPool;
import cs.bilkent.joker.engine.tuplequeue.impl.drainer.BlockingMultiPortConjunctiveDrainer;
import cs.bilkent.joker.engine.tuplequeue.impl.drainer.BlockingMultiPortDisjunctiveDrainer;
import cs.bilkent.joker.engine.tuplequeue.impl.drainer.BlockingSinglePortDrainer;
import cs.bilkent.joker.engine.tuplequeue.impl.drainer.GreedyDrainer;
import static cs.bilkent.joker.flow.Port.DEFAULT_PORT_INDEX;
import cs.bilkent.joker.operator.OperatorDef;
import cs.bilkent.joker.operator.scheduling.ScheduleWhenAvailable;
import cs.bilkent.joker.operator.scheduling.ScheduleWhenTuplesAvailable;
import static cs.bilkent.joker.operator.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByCount.AT_LEAST_BUT_SAME_ON_ALL_PORTS;
import static cs.bilkent.joker.operator.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByPort.ALL_PORTS;
import static cs.bilkent.joker.operator.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByPort.ANY_PORT;
import cs.bilkent.joker.operator.scheduling.SchedulingStrategy;

@NotThreadSafe
public class BlockingTupleQueueDrainerPool implements TupleQueueDrainerPool
{

    private final String operatorId;

    private final int inputPortCount;

    private BlockingSinglePortDrainer singlePortDrainer;

    private BlockingMultiPortConjunctiveDrainer multiPortConjunctiveDrainer;

    private BlockingMultiPortDisjunctiveDrainer multiPortDisjunctiveDrainer;

    private GreedyDrainer greedyDrainer;

    private TupleQueueDrainer active;

    public BlockingTupleQueueDrainerPool ( final JokerConfig config, final OperatorDef operatorDef )
    {
        this.operatorId = operatorDef.id();
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
        checkState( active == null, "cannot acquire new drainer when there is an active one for operator %s", operatorId );

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
                                  && strategy.getTupleAvailabilityByCount() == AT_LEAST_BUT_SAME_ON_ALL_PORTS ), "invalid %s", strategy );
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
        checkArgument( active == drainer, "cannot release active drainer %s by drainer %s", active, drainer );
        active.reset();
        active = null;
    }

}
