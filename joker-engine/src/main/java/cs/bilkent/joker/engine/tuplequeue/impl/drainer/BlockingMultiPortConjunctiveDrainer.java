package cs.bilkent.joker.engine.tuplequeue.impl.drainer;

import cs.bilkent.joker.engine.tuplequeue.TupleQueue;
import cs.bilkent.joker.engine.util.concurrent.BackoffIdleStrategy;
import cs.bilkent.joker.engine.util.concurrent.IdleStrategy;

public class BlockingMultiPortConjunctiveDrainer extends MultiPortDrainer
{

    private final IdleStrategy idleStrategy = BackoffIdleStrategy.newDefaultInstance();

    public BlockingMultiPortConjunctiveDrainer ( final String operatorId, final int inputPortCount, final int maxBatchSize )
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
            int satisfied = 0;
            for ( int i = 0; i < limit; i += 2 )
            {
                final int portIndex = tupleCounts[ i ];
                final int tupleCount = tupleCounts[ i + 1 ];
                if ( tupleCount == NO_TUPLES_AVAILABLE || tupleQueues[ portIndex ].size() >= tupleCount )
                {
                    satisfied++;
                }
            }

            if ( satisfied == inputPortCount )
            {
                return tupleCounts;
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
