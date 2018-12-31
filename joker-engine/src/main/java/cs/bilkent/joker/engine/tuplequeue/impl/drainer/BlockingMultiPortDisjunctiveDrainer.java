package cs.bilkent.joker.engine.tuplequeue.impl.drainer;

import cs.bilkent.joker.engine.tuplequeue.TupleQueue;
import cs.bilkent.joker.engine.util.concurrent.BackoffIdleStrategy;
import cs.bilkent.joker.engine.util.concurrent.IdleStrategy;


public class BlockingMultiPortDisjunctiveDrainer extends MultiPortDrainer
{

    private final IdleStrategy idleStrategy = BackoffIdleStrategy.newDefaultInstance();

    public BlockingMultiPortDisjunctiveDrainer ( final String operatorId, final int inputPortCount, final int maxBatchSize )
    {
        super( operatorId, inputPortCount, maxBatchSize );
    }

    @Override
    protected int[] checkQueueSizes ( final TupleQueue[] tupleQueues )
    {
        idleStrategy.reset();

        boolean idle = false;
        while ( true )
        {
            boolean satisfied = false;
            for ( int i = 0; i < limit; i += 2 )
            {
                final int portIndex = tupleCountsToCheck[ i ];
                final int tupleCountIndex = i + 1;
                final int tupleCount = tupleCountsToCheck[ tupleCountIndex ];
                final TupleQueue tupleQueue = tupleQueues[ portIndex ];
                if ( tupleCount == 1 && tupleQueue.isNonEmpty() )
                {
                    tupleCountsBuffer[ tupleCountIndex ] = tupleCountsToDrain[ tupleCountIndex ];
                    satisfied = true;
                }
                else if ( tupleCount != NO_TUPLES_AVAILABLE )
                {
                    if ( tupleQueue.size() >= tupleCount )
                    {
                        tupleCountsBuffer[ tupleCountIndex ] = tupleCountsToDrain[ tupleCountIndex ];
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
                if ( idle )
                {
                    return null;
                }

                idle = idleStrategy.idle();
            }
        }

    }

}
