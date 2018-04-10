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

    public BlockingSinglePortDrainer ( final int maxBatchSize )
    {
        super( maxBatchSize );
    }

    @Override
    public boolean drain ( final boolean maySkipBlocking, final PartitionKey key, final TupleQueue[] queues,
                           final Function<PartitionKey, TuplesImpl> tuplesSupplier )
    {
        checkArgument( queues != null );
        checkArgument( queues.length == 1 );
        checkArgument( tuplesSupplier != null );

        idleStrategy.reset();
        final TupleQueue tupleQueue = queues[ 0 ];
        boolean idle = maySkipBlocking;

        while ( tupleQueue.size() < tupleCountToCheck )
        {
            if ( idle )
            {
                return false;
            }

            idle = idleStrategy.idle();
        }

        tupleQueue.poll( tupleCountToPoll, tuplesSupplier.apply( key ).getTuplesModifiable( 0 ) );

        return true;
    }

}
