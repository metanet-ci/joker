package cs.bilkent.joker.engine.tuplequeue.impl.drainer.pool;

import javax.annotation.concurrent.NotThreadSafe;

import static com.google.common.base.Preconditions.checkArgument;
import cs.bilkent.joker.engine.config.JokerConfig;
import cs.bilkent.joker.engine.tuplequeue.TupleQueueDrainer;
import cs.bilkent.joker.engine.tuplequeue.TupleQueueDrainerPool;
import cs.bilkent.joker.engine.tuplequeue.impl.drainer.BlockingMultiPortConjunctiveDrainer;
import cs.bilkent.joker.engine.tuplequeue.impl.drainer.BlockingMultiPortDisjunctiveDrainer;
import cs.bilkent.joker.engine.tuplequeue.impl.drainer.BlockingSinglePortDrainer;
import cs.bilkent.joker.engine.tuplequeue.impl.drainer.EmptyDrainer;
import cs.bilkent.joker.engine.tuplequeue.impl.drainer.MultiPortDrainer;
import cs.bilkent.joker.engine.tuplequeue.impl.drainer.SinglePortDrainer;
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

    private final int maxBatchSize;


    public BlockingTupleQueueDrainerPool ( final JokerConfig config, final OperatorDef operatorDef )
    {
        this.operatorId = operatorDef.getId();
        this.inputPortCount = operatorDef.getInputPortCount();
        this.maxBatchSize = config.getTupleQueueDrainerConfig().getMaxBatchSize();
    }

    @Override
    public TupleQueueDrainer acquire ( final SchedulingStrategy input )
    {
        if ( input instanceof ScheduleWhenAvailable )
        {
            return new EmptyDrainer();
        }

        if ( input instanceof ScheduleWhenTuplesAvailable )
        {
            final ScheduleWhenTuplesAvailable strategy = (ScheduleWhenTuplesAvailable) input;
            if ( inputPortCount == 1 )
            {
                final SinglePortDrainer singlePortDrainer = new BlockingSinglePortDrainer( operatorId, maxBatchSize );
                singlePortDrainer.setParameters( strategy.getTupleAvailabilityByCount(), strategy.getTupleCount( DEFAULT_PORT_INDEX ) );
                return singlePortDrainer;
            }

            checkArgument( !( strategy.getTupleAvailabilityByPort() == ANY_PORT
                              && strategy.getTupleAvailabilityByCount() == AT_LEAST_BUT_SAME_ON_ALL_PORTS ), "invalid %s", strategy );
            final int[] inputPorts = new int[ inputPortCount ];
            for ( int i = 0; i < inputPortCount; i++ )
            {
                inputPorts[ i ] = i;
            }

            final MultiPortDrainer multiPortDrainer;

            if ( strategy.getTupleAvailabilityByPort() == ALL_PORTS )
            {
                multiPortDrainer = new BlockingMultiPortConjunctiveDrainer( operatorId, inputPortCount, maxBatchSize );
            }
            else
            {
                multiPortDrainer = new BlockingMultiPortDisjunctiveDrainer( operatorId, inputPortCount, maxBatchSize );
            }

            multiPortDrainer.setParameters( strategy.getTupleAvailabilityByCount(), inputPorts, strategy.getTupleCounts() );

            return multiPortDrainer;
        }

        throw new IllegalArgumentException( input.getClass() + " is not supported yet!" );
    }

}
