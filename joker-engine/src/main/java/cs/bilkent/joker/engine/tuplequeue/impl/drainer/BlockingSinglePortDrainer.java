package cs.bilkent.joker.engine.tuplequeue.impl.drainer;

import java.util.List;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import cs.bilkent.joker.engine.tuplequeue.TupleQueue;
import cs.bilkent.joker.engine.util.concurrent.BackoffIdleStrategy;
import cs.bilkent.joker.engine.util.concurrent.IdleStrategy;
import cs.bilkent.joker.operator.Tuple;
import cs.bilkent.joker.operator.impl.TuplesImpl;
import cs.bilkent.joker.partition.impl.PartitionKey;

public class BlockingSinglePortDrainer extends SinglePortDrainer
{

    private final IdleStrategy idleStrategy = BackoffIdleStrategy.newDefaultInstance();

    public BlockingSinglePortDrainer ( final int maxBatchSize )
    {
        super( maxBatchSize );
    }

//    private long last = System.nanoTime();

    @Override
    public boolean drain ( final boolean maySkipBlocking,
                           final PartitionKey key,
                           final TupleQueue[] queues,
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

//        boolean log = false;
//        final long now = System.nanoTime();
//        int size = 0;
//        if ( now - last >= TimeUnit.MILLISECONDS.toNanos( 100 ) )
//        {
//            last = now;
//            log = true;
//            size = tupleQueue.size();
//        }

        final List<Tuple> tuples = tuplesSupplier.apply( key ).getTuplesModifiable( 0 );
        tupleQueue.poll( tupleCountToPoll, tuples );

//        if ( log )
//        {
//            System.out.println( "#### ERROR QUEUE SIZE: " + size + " POLLED: " + tuples.size() );
//        }

        return true;
    }

}
