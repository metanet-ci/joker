package cs.bilkent.joker.engine.tuplequeue.impl.drainer;

import cs.bilkent.joker.engine.tuplequeue.TupleQueue;
import cs.bilkent.joker.engine.util.concurrent.BackoffIdleStrategy;
import cs.bilkent.joker.engine.util.concurrent.IdleStrategy;


public class BlockingMultiPortDisjunctiveDrainer extends MultiPortDrainer
{

    private final IdleStrategy idleStrategy = BackoffIdleStrategy.newDefaultInstance();

    public BlockingMultiPortDisjunctiveDrainer ( final int inputPortCount, final int maxBatchSize )
    {
        super( inputPortCount, maxBatchSize );
    }

    @Override
    protected int[] checkQueueSizes ( final boolean maySkipBlocking, final TupleQueue[] tupleQueues )
    {
        while ( true )
        {
            boolean satisfied = false;
            for ( int i = 0; i < limit; i += 2 )
            {
                final int portIndex = tupleCounts[ i ];
                final int tupleCountIndex = i + 1;
                final int tupleCount = tupleCounts[ tupleCountIndex ];
                final TupleQueue tupleQueue = tupleQueues[ portIndex ];
                if ( tupleCount != NO_TUPLES_AVAILABLE )
                {
                    if ( tupleQueue.size() >= tupleCount )
                    {
                        tupleCountsBuffer[ tupleCountIndex ] = tupleCount;
                        satisfied = true;
                    }
                    else
                    {
                        tupleCountsBuffer[ tupleCountIndex ] = NO_TUPLES_AVAILABLE;
                    }
                }
            }

            if ( satisfied )
            {
                return tupleCountsBuffer;
            }
            else
            {
                if ( maySkipBlocking )
                {
                    return null;
                }

                idleStrategy.idle();
            }
        }

    }

    @Override
    public void reset ()
    {
        super.reset();
        idleStrategy.reset();
    }

}
