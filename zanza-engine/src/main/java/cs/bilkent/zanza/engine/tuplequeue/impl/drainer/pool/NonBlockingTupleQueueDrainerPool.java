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
import cs.bilkent.zanza.flow.OperatorDef;
import static cs.bilkent.zanza.flow.Port.DEFAULT_PORT_INDEX;
import cs.bilkent.zanza.operator.scheduling.ScheduleWhenAvailable;
import cs.bilkent.zanza.operator.scheduling.ScheduleWhenTuplesAvailable;
import static cs.bilkent.zanza.operator.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByCount.AT_LEAST_BUT_SAME_ON_ALL_PORTS;
import static cs.bilkent.zanza.operator.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByPort.ALL_PORTS;
import static cs.bilkent.zanza.operator.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByPort.ANY_PORT;
import cs.bilkent.zanza.operator.scheduling.SchedulingStrategy;
import cs.bilkent.zanza.operator.spec.OperatorType;
import static cs.bilkent.zanza.operator.spec.OperatorType.PARTITIONED_STATEFUL;

@NotThreadSafe
public class NonBlockingTupleQueueDrainerPool implements TupleQueueDrainerPool
{

    private final String operatorId;

    private final int inputPortCount;

    private final OperatorType operatorType;

    private NonBlockingSinglePortDrainer singlePortDrainer;

    private NonBlockingMultiPortConjunctiveDrainer multiPortConjunctiveDrainer;

    private NonBlockingMultiPortDisjunctiveDrainer multiPortDisjunctiveDrainer;

    private GreedyDrainer greedyDrainer;

    private TupleQueueDrainer active;

    public NonBlockingTupleQueueDrainerPool ( final ZanzaConfig config, final OperatorDef operatorDef )
    {
        this.operatorId = operatorDef.id();
        this.inputPortCount = operatorDef.inputPortCount();
        this.operatorType = operatorDef.operatorType();

        final int maxBatchSize = config.getTupleQueueDrainerConfig().getMaxBatchSize();

        if ( inputPortCount == 1 )
        {
            this.singlePortDrainer = new NonBlockingSinglePortDrainer( maxBatchSize );
        }
        else if ( inputPortCount > 1 )
        {
            this.multiPortConjunctiveDrainer = new NonBlockingMultiPortConjunctiveDrainer( inputPortCount, maxBatchSize );
            this.multiPortDisjunctiveDrainer = new NonBlockingMultiPortDisjunctiveDrainer( inputPortCount, maxBatchSize );
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
                                  && strategy.getTupleAvailabilityByCount() == AT_LEAST_BUT_SAME_ON_ALL_PORTS ) );
                final int[] inputPorts = new int[ inputPortCount ];
                for ( int i = 0; i < inputPortCount; i++ )
                {
                    inputPorts[ i ] = i;
                }
                if ( strategy.getTupleAvailabilityByPort() == ALL_PORTS && operatorType != PARTITIONED_STATEFUL )
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
        checkArgument( active == drainer, "active: " + active + " drainer: " + drainer );
        active.reset();
        active = null;
    }
}
