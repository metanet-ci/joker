package cs.bilkent.zanza.engine.tuplequeue.impl.drainer;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueue;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueueDrainer;
import cs.bilkent.zanza.operator.Tuple;
import cs.bilkent.zanza.operator.impl.TuplesImpl;
import cs.bilkent.zanza.operator.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByCount;
import static cs.bilkent.zanza.operator.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByCount.AT_LEAST_BUT_SAME_ON_ALL_PORTS;
import static cs.bilkent.zanza.operator.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByCount.EXACT;
import static java.lang.Math.max;

abstract class MultiPortDrainer implements TupleQueueDrainer
{

    static final int NO_TUPLES_AVAILABLE = -1;

    int[] tupleCounts;

    protected final int inputPortCount;

    private final int maxBatchSize;

    private TupleAvailabilityByCount tupleAvailabilityByCount;

    private final TuplesImpl buffer;

    private TuplesImpl result;

    private Object key;

    MultiPortDrainer ( final int inputPortCount, final int maxBatchSize )
    {
        this.inputPortCount = inputPortCount;
        this.maxBatchSize = maxBatchSize;
        this.buffer = new TuplesImpl( inputPortCount );
    }

    public void setParameters ( final TupleAvailabilityByCount tupleAvailabilityByCount, final int[] tupleCounts )
    {
        this.tupleAvailabilityByCount = tupleAvailabilityByCount;
        this.tupleCounts = tupleCounts;
    }

    @Override
    public void drain ( final Object key, final TupleQueue[] tupleQueues )
    {
        checkArgument( tupleQueues != null && tupleQueues.length == inputPortCount );

        boolean success = false;

        final int[] tupleCounts = checkQueueSizes( tupleQueues );
        if ( tupleCounts != null )
        {
            for ( int portIndex = 0; portIndex < inputPortCount; portIndex++ )
            {
                final int tupleCount = tupleCounts[ portIndex ];
                if ( tupleCount == NO_TUPLES_AVAILABLE )
                {
                    continue;
                }

                final TupleQueue tupleQueue = tupleQueues[ portIndex ];
                final boolean pollWithExactCount =
                        tupleAvailabilityByCount == EXACT || tupleAvailabilityByCount == AT_LEAST_BUT_SAME_ON_ALL_PORTS;

                final List<Tuple> tuples = buffer.getTuplesModifiable( portIndex );

                if ( pollWithExactCount )
                {
                    tupleQueue.pollTuples( tupleCount, tuples );
                }
                else
                {
                    tupleQueue.pollTuplesAtLeast( tupleCount, max( tupleCount, maxBatchSize ), tuples );
                }

                success |= !tuples.isEmpty();
            }
        }

        if ( success )
        {
            this.result = buffer;
            this.key = key;
        }
    }

    @Override
    public TuplesImpl getResult ()
    {
        return result;
    }

    @Override
    public Object getKey ()
    {
        return key;
    }

    @Override
    public void reset ()
    {
        buffer.clear();
        result = null;
        key = null;
    }

    protected abstract int[] checkQueueSizes ( final TupleQueue[] tupleQueues );

}
