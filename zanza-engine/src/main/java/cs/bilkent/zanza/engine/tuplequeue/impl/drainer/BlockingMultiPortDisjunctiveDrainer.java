package cs.bilkent.zanza.engine.tuplequeue.impl.drainer;

import cs.bilkent.zanza.engine.tuplequeue.TupleQueue;


public class BlockingMultiPortDisjunctiveDrainer extends MultiPortDrainer
{

    private final long timeoutInMillisPerQueue;

    public BlockingMultiPortDisjunctiveDrainer ( final int inputPortCount, final long timeoutInMillis )
    {
        super( inputPortCount );
        this.timeoutInMillisPerQueue = (long) Math.ceil( timeoutInMillis / inputPortCount );
    }

    @Override
    protected int[] checkQueueSizes ( final TupleQueue[] tupleQueues )
    {
        int[] tupleCounts = null;
        for ( int portIndex = 0; portIndex < inputPortCount; portIndex++ )
        {
            final int tupleCount = this.tupleCounts[ portIndex ];
            final TupleQueue tupleQueue = tupleQueues[ portIndex ];
            if ( tupleCounts == null )
            {
                if ( tupleQueue.awaitMinimumSize( tupleCount, timeoutInMillisPerQueue ) )
                {
                    tupleCounts = new int[ inputPortCount ];
                    tupleCounts[ portIndex ] = tupleCount;
                }
            }
            else if ( tupleQueue.size() >= tupleCount )
            {
                tupleCounts[ portIndex ] = tupleCount;
            }
            else
            {
                tupleCounts[ portIndex ] = NO_TUPLES_AVAILABLE;
            }
        }

        return tupleCounts;
    }

}
