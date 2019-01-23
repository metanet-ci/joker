package cs.bilkent.joker.engine.tuplequeue.impl.drainer;

import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import cs.bilkent.joker.engine.tuplequeue.TupleQueue;
import cs.bilkent.joker.engine.tuplequeue.TupleQueueDrainer;
import cs.bilkent.joker.engine.util.concurrent.BackoffIdleStrategy;
import cs.bilkent.joker.engine.util.concurrent.IdleStrategy;
import cs.bilkent.joker.operator.impl.TuplesImpl;
import cs.bilkent.joker.partition.impl.PartitionKey;

public class BlockingGreedyDrainer implements TupleQueueDrainer
{

    private final IdleStrategy idleStrategy = BackoffIdleStrategy.newDefaultInstance();

    private final int inputPortCount;

    private final int maxTupleCount;

    public BlockingGreedyDrainer ( final int inputPortCount, final int maxTupleCount )
    {
        this.inputPortCount = inputPortCount;
        this.maxTupleCount = maxTupleCount;
    }

    @Override
    public boolean drain ( final PartitionKey key, final TupleQueue[] queues,
                           final Function<PartitionKey, TuplesImpl> tuplesSupplier )
    {
        checkArgument( queues != null );
        checkArgument( queues.length == inputPortCount );
        checkArgument( tuplesSupplier != null );

        boolean idle = false;
        boolean empty = true;
        while ( empty )
        {
            for ( int i = 0; i < inputPortCount; i++ )
            {
                if ( !queues[ i ].isEmpty() )
                {
                    empty = false;
                    break;
                }
            }

            if ( empty && idle )
            {
                return false;
            }

            idle = idleStrategy.idle();
        }

        final TuplesImpl tuples = tuplesSupplier.apply( key );

        for ( int portIndex = 0; portIndex < inputPortCount; portIndex++ )
        {
            queues[ portIndex ].drainTo( maxTupleCount, tuples.getTuples( portIndex ) );
        }

        return false;
    }

}
