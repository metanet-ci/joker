package cs.bilkent.joker.engine.tuplequeue.impl.drainer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkArgument;
import cs.bilkent.joker.engine.tuplequeue.TupleQueueDrainer;
import cs.bilkent.joker.operator.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByCount;
import static cs.bilkent.joker.operator.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByCount.EXACT;
import static java.lang.Math.max;

public abstract class SinglePortDrainer implements TupleQueueDrainer
{

    private static final Logger LOGGER = LoggerFactory.getLogger( SinglePortDrainer.class );

    protected final String operatorId;

    protected final int maxBatchSize;

    int tupleCountToCheck;

    int tupleCountToPoll;

    SinglePortDrainer ( final String operatorId, final int maxBatchSize )
    {
        this.operatorId = operatorId;
        this.maxBatchSize = maxBatchSize;
    }

    public final void setParameters ( final TupleAvailabilityByCount tupleAvailabilityByCount, final int tupleCount )
    {
        checkArgument( tupleAvailabilityByCount != null );
        checkArgument( tupleCount > 0, "invalid tuple count %s", tupleCount );
        tupleCountToCheck = tupleCount;
        tupleCountToPoll = tupleAvailabilityByCount == EXACT ? tupleCount : max( tupleCount, maxBatchSize );
        LOGGER.info( "Operator: {} -> tuple count to check: {} tuple count to poll: {}", operatorId, tupleCountToCheck, tupleCountToPoll );
    }

}
