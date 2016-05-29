package cs.bilkent.zanza.engine.tuplequeue.impl.drainer;

import cs.bilkent.zanza.engine.tuplequeue.TupleQueue;


public class BlockingMultiPortDisjunctiveDrainer extends MultiPortDrainer
{

    private final long timeoutInMillisPerQueue;

    private final int[] tupleCountsBuffer;

    public BlockingMultiPortDisjunctiveDrainer ( final int inputPortCount, final int maxBatchSize, final long timeoutInMillis )
    {
        super( inputPortCount, maxBatchSize );
        this.timeoutInMillisPerQueue = (long) Math.ceil( timeoutInMillis / inputPortCount );
        this.tupleCountsBuffer = new int[ inputPortCount ];
    }

    @Override
    protected int[] checkQueueSizes ( final TupleQueue[] tupleQueues )
    {
        boolean notFound = true;
        for ( int portIndex = 0; portIndex < inputPortCount; portIndex++ )
        {
            final int tupleCount = this.tupleCounts[ portIndex ];
            final TupleQueue tupleQueue = tupleQueues[ portIndex ];
            if ( notFound )
            {
                if ( tupleQueue.awaitMinimumSize( tupleCount, timeoutInMillisPerQueue ) )
                {
                    tupleCountsBuffer[ portIndex ] = tupleCount;
                    notFound = false;
                }
            }
            else if ( tupleQueue.size() >= tupleCount )
            {
                tupleCountsBuffer[ portIndex ] = tupleCount;
            }
            else
            {
                tupleCountsBuffer[ portIndex ] = NO_TUPLES_AVAILABLE;
            }
        }

        return tupleCountsBuffer;
    }

}
