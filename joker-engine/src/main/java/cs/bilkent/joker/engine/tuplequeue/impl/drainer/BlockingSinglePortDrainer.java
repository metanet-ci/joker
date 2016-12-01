package cs.bilkent.joker.engine.tuplequeue.impl.drainer;

import static com.google.common.base.Preconditions.checkArgument;
import cs.bilkent.joker.engine.partition.PartitionKey;
import cs.bilkent.joker.engine.tuplequeue.TupleQueue;
import cs.bilkent.joker.engine.util.concurrent.BackoffIdleStrategy;
import cs.bilkent.joker.engine.util.concurrent.IdleStrategy;

public class BlockingSinglePortDrainer extends SinglePortDrainer
{

    private final IdleStrategy idleStrategy = BackoffIdleStrategy.newDefaultInstance();

    public BlockingSinglePortDrainer ( final int maxBatchSize )
    {
        super( maxBatchSize );
    }

    @Override
    public void drain ( final boolean maySkipBlocking, final PartitionKey key, final TupleQueue[] tupleQueues )
    {
        checkArgument( tupleQueues != null );
        checkArgument( tupleQueues.length == 1 );

        final TupleQueue tupleQueue = tupleQueues[ 0 ];

        boolean idle = maySkipBlocking;
        while ( tupleQueue.size() < tupleCountToCheck )
        {
            if ( idle )
            {
                return;
            }

            idle = idleStrategy.idle();
        }

        tupleQueue.poll( tupleCountToPoll, tuples );
        this.result = buffer;
        this.key = key;
    }

    @Override
    public void reset ()
    {
        super.reset();
        idleStrategy.reset();
    }

}
