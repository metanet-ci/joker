package cs.bilkent.joker.engine.tuplequeue.impl.drainer;

import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import cs.bilkent.joker.engine.tuplequeue.TupleQueue;
import cs.bilkent.joker.engine.util.concurrent.BackoffIdleStrategy;
import cs.bilkent.joker.engine.util.concurrent.IdleStrategy;
import cs.bilkent.joker.operator.impl.TuplesImpl;
import cs.bilkent.joker.partition.impl.PartitionKey;

public class BlockingSinglePortDrainer extends SinglePortDrainer
{

    private final IdleStrategy idleStrategy = BackoffIdleStrategy.newDefaultInstance();
    private final QueueWaitingTimeRecorder queueWaitingTimeRecorder;

    public BlockingSinglePortDrainer ( final String operatorId, final int maxBatchSize )
    {
        super( operatorId, maxBatchSize );
        this.queueWaitingTimeRecorder = new QueueWaitingTimeRecorder( operatorId );
    }

    @Override
    public boolean drain ( final PartitionKey key, final TupleQueue[] queues, final Function<PartitionKey, TuplesImpl> tuplesSupplier )
    {
        checkArgument( queues != null );
        checkArgument( queues.length == 1 );
        checkArgument( tuplesSupplier != null );

        idleStrategy.reset();
        final TupleQueue tupleQueue = queues[ 0 ];
        boolean idle = false;

        while ( tupleQueue.size() < tupleCountToCheck )
        {
            if ( idle )
            {
                return false;
            }

            idle = idleStrategy.idle();
        }

        queueWaitingTimeRecorder.reset();
        queueWaitingTimeRecorder.setParameters( tuplesSupplier.apply( key ).getTuples( 0 ) );
        tupleQueue.drainTo( tupleCountToDrain, queueWaitingTimeRecorder );

        return true;
    }

}
