package cs.bilkent.joker.engine.tuplequeue.impl.drainer;

import static com.google.common.base.Preconditions.checkArgument;
import cs.bilkent.joker.engine.tuplequeue.TupleQueueDrainer;
import cs.bilkent.joker.operator.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByCount;
import static cs.bilkent.joker.operator.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByCount.EXACT;
import static java.lang.Math.max;

public abstract class SinglePortDrainer implements TupleQueueDrainer
{

    protected final int maxBatchSize;

    int tupleCountToCheck;

    int tupleCountToPoll;

    SinglePortDrainer ( final int maxBatchSize )
    {
        this.maxBatchSize = maxBatchSize;
    }

    public final void setParameters ( final TupleAvailabilityByCount tupleAvailabilityByCount, final int tupleCount )
    {
        checkArgument( tupleAvailabilityByCount != null );
        checkArgument( tupleCount > 0, "invalid tuple count %s", tupleCount );
        this.tupleCountToCheck = tupleCount;
        this.tupleCountToPoll = tupleAvailabilityByCount == EXACT ? tupleCount : max( tupleCount, maxBatchSize );
    }

}
