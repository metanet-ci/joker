package cs.bilkent.zanza.engine.tuplequeue.impl.drainer;

import java.util.Arrays;

import cs.bilkent.zanza.engine.tuplequeue.TupleQueue;


public class BlockingMultiPortDisjunctiveDrainer extends MultiPortDrainer
{

    private final long timeoutInMillisPerQueue;

    private final int[] tupleCountsBuffer;

    public BlockingMultiPortDisjunctiveDrainer ( final int inputPortCount, final int maxBatchSize, final long timeoutInMillis )
    {
        super( inputPortCount, maxBatchSize );
        this.timeoutInMillisPerQueue = inputPortCount > 0 ? (long) Math.ceil( timeoutInMillis / inputPortCount ) : 0;
        this.tupleCountsBuffer = new int[ inputPortCount ];
        Arrays.fill( tupleCountsBuffer, NO_TUPLES_AVAILABLE );
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
                else
                {
                    tupleCountsBuffer[ portIndex ] = NO_TUPLES_AVAILABLE;
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
