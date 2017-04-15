package cs.bilkent.joker.engine.tuplequeue.impl.drainer;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import cs.bilkent.joker.engine.partition.PartitionKey;
import cs.bilkent.joker.engine.tuplequeue.TupleQueueDrainer;
import static cs.bilkent.joker.flow.Port.DEFAULT_PORT_INDEX;
import cs.bilkent.joker.operator.Tuple;
import cs.bilkent.joker.operator.impl.TuplesImpl;
import cs.bilkent.joker.operator.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByCount;
import static cs.bilkent.joker.operator.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByCount.EXACT;
import static java.lang.Math.max;

public abstract class SinglePortDrainer implements TupleQueueDrainer
{

    protected final int maxBatchSize;

    final TuplesImpl buffer = new TuplesImpl( 1 );

    protected final List<Tuple> tuples = buffer.getTuplesModifiable( DEFAULT_PORT_INDEX );

    int tupleCountToCheck;

    int tupleCountToPoll;

    protected TuplesImpl result;

    protected PartitionKey key;

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

    @Override
    public final TuplesImpl getResult ()
    {
        return result;
    }

    @Override
    public final PartitionKey getKey ()
    {
        return key;
    }

    @Override
    public void reset ()
    {
        tuples.clear();
        result = null;
        key = null;
    }

}
