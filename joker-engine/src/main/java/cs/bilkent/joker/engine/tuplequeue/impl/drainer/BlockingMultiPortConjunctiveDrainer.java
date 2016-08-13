package cs.bilkent.joker.engine.tuplequeue.impl.drainer;

import java.util.concurrent.TimeUnit;

import cs.bilkent.joker.engine.tuplequeue.TupleQueue;

public class BlockingMultiPortConjunctiveDrainer extends MultiPortDrainer
{

    private final long timeoutPerQueue;

    private final TimeUnit unit;

    public BlockingMultiPortConjunctiveDrainer ( final int inputPortCount, final int maxBatchSize, final long timeout, final TimeUnit unit )
    {
        super( inputPortCount, maxBatchSize );
        this.timeoutPerQueue = inputPortCount > 0 ? (long) Math.ceil( ( (double) timeout ) / inputPortCount ) : 0;
        this.unit = unit;
    }

    @Override
    protected int[] checkQueueSizes ( final TupleQueue[] tupleQueues )
    {
        int satisfied = 0;
        for ( int i = 0; i < limit; i += 2 )
        {
            final int portIndex = tupleCounts[ i ];
            final int tupleCount = tupleCounts[ i + 1 ];
            if ( tupleCount == 0 || tupleQueues[ portIndex ].awaitMinimumSize( tupleCount, timeoutPerQueue, unit ) )
            {
                satisfied++;
            }
        }

        return satisfied == inputPortCount ? tupleCounts : null;
    }

}
