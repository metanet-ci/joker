package cs.bilkent.joker.engine.tuplequeue.impl.drainer;

import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import cs.bilkent.joker.engine.tuplequeue.TupleQueue;
import cs.bilkent.joker.engine.tuplequeue.TupleQueueDrainer;
import cs.bilkent.joker.operator.impl.TuplesImpl;
import cs.bilkent.joker.operator.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByCount;
import static cs.bilkent.joker.operator.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByCount.AT_LEAST_BUT_SAME_ON_ALL_PORTS;
import static cs.bilkent.joker.operator.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByCount.EXACT;
import cs.bilkent.joker.partition.impl.PartitionKey;
import static java.lang.Math.max;

public abstract class MultiPortDrainer implements TupleQueueDrainer
{

    static final int NO_TUPLES_AVAILABLE = -1;

    private final QueueWaitingTimeRecorder queueWaitingTimeRecorder;

    protected final int[] tupleCounts;

    protected final int limit;

    protected final int inputPortCount;

    protected final int[] tupleCountsBuffer;

    private final int maxBatchSize;

    private boolean pollWithExactCount;

    MultiPortDrainer ( final String operatorId, final int inputPortCount, final int maxBatchSize )
    {
        this.queueWaitingTimeRecorder = new QueueWaitingTimeRecorder( operatorId );
        this.inputPortCount = inputPortCount;
        this.maxBatchSize = maxBatchSize;
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
    }

    @Override
    public boolean drain ( final PartitionKey key, final TupleQueue[] queues,
                           final Function<PartitionKey, TuplesImpl> tuplesSupplier )
    {
        checkArgument( queues != null );
        checkArgument( queues.length == inputPortCount );
        checkArgument( tuplesSupplier != null );

        final int[] tupleCounts = checkQueueSizes( queues );
        if ( tupleCounts == null )
        {
            return false;
        }

        final long now = System.nanoTime();

        final TuplesImpl tuples = tuplesSupplier.apply( key );

        for ( int i = 0; i < limit; i += 2 )
        {
            int tupleCount = tupleCounts[ i + 1 ];
            if ( tupleCount == NO_TUPLES_AVAILABLE )
            {
                continue;
            }

            tupleCount = pollWithExactCount ? tupleCount : max( tupleCount, maxBatchSize );

            final int portIndex = tupleCounts[ i ];
            queueWaitingTimeRecorder.setParameters( now, tuples.getTuplesModifiable( portIndex ) );
            queues[ portIndex ].drainTo( tupleCount, queueWaitingTimeRecorder );
        }

        return true;
    }

    protected abstract int[] checkQueueSizes ( TupleQueue[] tupleQueues );

}
