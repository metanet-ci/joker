package cs.bilkent.zanza.engine.tuplequeue.impl.drainer;

import cs.bilkent.zanza.engine.tuplequeue.TupleQueue;

public class BlockingMultiPortConjunctiveDrainer extends MultiPortDrainer
{

    private final long timeoutInMillisPerQueue;

    public BlockingMultiPortConjunctiveDrainer ( final int inputPortCount, final long timeoutInMillis )
    {
        super( inputPortCount );
        this.timeoutInMillisPerQueue = (long) Math.ceil( timeoutInMillis / inputPortCount );
    }

    @Override
    protected int[] checkQueueSizes ( final TupleQueue[] tupleQueues )
    {
        int satisfied = 0;
        for ( int portIndex = 0; portIndex < inputPortCount; portIndex++ )
        {
            final int tupleCount = tupleCounts[ portIndex ];
            if ( tupleCount == 0 || tupleQueues[ portIndex ].awaitMinimumSize( tupleCount, timeoutInMillisPerQueue ) )
            {
                satisfied++;
            }
        }

        return satisfied == inputPortCount ? tupleCounts : null;
    }

}
