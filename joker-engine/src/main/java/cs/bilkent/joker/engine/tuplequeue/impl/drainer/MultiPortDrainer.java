package cs.bilkent.joker.engine.tuplequeue.impl.drainer;

import static com.google.common.base.Preconditions.checkArgument;
import cs.bilkent.joker.engine.partition.PartitionKey;
import cs.bilkent.joker.engine.tuplequeue.TupleQueue;
import cs.bilkent.joker.engine.tuplequeue.TupleQueueDrainer;
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

    protected final int[] tupleCountsBuffer;

    private final int maxBatchSize;

    private final TuplesImpl buffer;

    private boolean pollWithExactCount;

    private TuplesImpl result;

    private PartitionKey key;

    MultiPortDrainer ( final int inputPortCount, final int maxBatchSize )
    {
        this.inputPortCount = inputPortCount;
        this.maxBatchSize = maxBatchSize;
        this.buffer = new TuplesImpl( inputPortCount );
        this.tupleCounts = new int[ inputPortCount * 2 ];
        this.tupleCountsBuffer = new int[ inputPortCount * 2 ];
        this.limit = this.tupleCounts.length - 1;
    }

    public final void setParameters ( final TupleAvailabilityByCount tupleAvailabilityByCount,
                                      final int[] inputPorts,
                                      final int[] tupleCounts )
    {
        this.pollWithExactCount = tupleAvailabilityByCount == EXACT || tupleAvailabilityByCount == AT_LEAST_BUT_SAME_ON_ALL_PORTS;
        for ( int i = 0; i < inputPortCount; i++ )
        {
            final int portIndex = i * 2;
            this.tupleCounts[ portIndex ] = inputPorts[ i ];
            this.tupleCountsBuffer[ portIndex ] = inputPorts[ i ];
            int tupleCount = tupleCounts[ i ];
            tupleCount = tupleCount > 0 ? tupleCount : NO_TUPLES_AVAILABLE;
            this.tupleCounts[ portIndex + 1 ] = tupleCount;
            this.tupleCountsBuffer[ portIndex + 1 ] = tupleCount;
        }
        reset();
    }

    @Override
    public void drain ( final boolean maySkipBlocking, final PartitionKey key, final TupleQueue[] tupleQueues )
    {
        checkArgument( tupleQueues != null );
        checkArgument( tupleQueues.length == inputPortCount );

        final int[] tupleCounts = checkQueueSizes( maySkipBlocking, tupleQueues );

        if ( tupleCounts == null )
        {
            return;
        }

        for ( int i = 0; i < limit; i += 2 )
        {
            int tupleCount = tupleCounts[ i + 1 ];
            if ( tupleCount == NO_TUPLES_AVAILABLE )
            {
                continue;
            }

            tupleCount = pollWithExactCount ? tupleCount : max( tupleCount, maxBatchSize );

            final int portIndex = tupleCounts[ i ];
            final TupleQueue tupleQueue = tupleQueues[ portIndex ];
            tupleQueue.poll( tupleCount, buffer.getTuplesModifiable( portIndex ) );
        }

        this.result = buffer;
        this.key = key;
    }

    @Override
    public TuplesImpl getResult ()
    {
        return result;
    }

    @Override
    public PartitionKey getKey ()
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

    protected abstract int[] checkQueueSizes ( boolean maySkipBlocking, TupleQueue[] tupleQueues );

}
