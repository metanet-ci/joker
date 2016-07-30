package cs.bilkent.zanza.engine.tuplequeue.impl.drainer;

import cs.bilkent.zanza.engine.tuplequeue.TupleQueue;

public class BlockingMultiPortConjunctiveDrainer extends MultiPortDrainer
{

    private final long timeoutInMillisPerQueue;

    public BlockingMultiPortConjunctiveDrainer ( final int inputPortCount, final int maxBatchSize, final long timeoutInMillis )
    {
        super( inputPortCount, maxBatchSize );
        this.timeoutInMillisPerQueue = inputPortCount > 0 ? (long) Math.ceil( ( (double) timeoutInMillis ) / inputPortCount ) : 0;
    }

    @Override
    protected int[] checkQueueSizes ( final TupleQueue[] tupleQueues )
    {
        int satisfied = 0;
        for ( int i = 0; i < limit; i += 2 )
        {
            final int portIndex = tupleCounts[ i ];
            final int tupleCount = tupleCounts[ i + 1 ];
            if ( tupleCount == 0 || tupleQueues[ portIndex ].awaitMinimumSize( tupleCount, timeoutInMillisPerQueue ) )
            {
                satisfied++;
            }
        }

        return satisfied == inputPortCount ? tupleCounts : null;
    }

}
