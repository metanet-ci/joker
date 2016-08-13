package cs.bilkent.joker.engine.tuplequeue.impl.drainer;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import cs.bilkent.joker.engine.tuplequeue.TupleQueue;
import cs.bilkent.joker.engine.tuplequeue.TupleQueueDrainer;
import cs.bilkent.joker.operator.Tuple;
import cs.bilkent.joker.operator.impl.TuplesImpl;
import cs.bilkent.joker.operator.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByCount;
import static cs.bilkent.joker.operator.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByCount.AT_LEAST_BUT_SAME_ON_ALL_PORTS;
import static cs.bilkent.joker.operator.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByCount.EXACT;
import static java.lang.Math.max;

public abstract class MultiPortDrainer implements TupleQueueDrainer
{

    static final int NO_TUPLES_AVAILABLE = -1;

    protected final int[] tupleCounts;

    protected final int limit;

    protected final int inputPortCount;

    private final int maxBatchSize;

    private TupleAvailabilityByCount tupleAvailabilityByCount;

    private final TuplesImpl buffer;

    private TuplesImpl result;

    private Object key;

    MultiPortDrainer ( final int inputPortCount, final int maxBatchSize )
    {
        checkArgument( inputPortCount > 1, "invalid input port count %s", inputPortCount );
        this.inputPortCount = inputPortCount;
        this.maxBatchSize = maxBatchSize;
        this.buffer = new TuplesImpl( inputPortCount );
        this.tupleCounts = new int[ inputPortCount * 2 ];
        this.limit = this.tupleCounts.length - 1;
    }

    public void setParameters ( final TupleAvailabilityByCount tupleAvailabilityByCount, final int[] inputPorts, final int[] tupleCounts )
    {
        this.tupleAvailabilityByCount = tupleAvailabilityByCount;
        for ( int i = 0; i < inputPortCount; i++ )
        {
            this.tupleCounts[ i * 2 ] = inputPorts[ i ];
            this.tupleCounts[ i * 2 + 1 ] = tupleCounts[ i ];
        }
    }

    @Override
    public void drain ( final Object key, final TupleQueue[] tupleQueues )
    {
        checkArgument( tupleQueues != null );
        checkArgument( tupleQueues.length == inputPortCount );

        boolean success = false;

        final int[] tupleCounts = checkQueueSizes( tupleQueues );
        if ( tupleCounts != null )
        {
            for ( int i = 0; i < limit; i += 2 )
            {
                final int portIndex = tupleCounts[ i ];
                final int tupleCount = tupleCounts[ i + 1 ];
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
