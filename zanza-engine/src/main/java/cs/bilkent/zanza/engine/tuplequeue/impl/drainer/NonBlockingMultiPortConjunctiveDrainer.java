package cs.bilkent.zanza.engine.tuplequeue.impl.drainer;

import cs.bilkent.zanza.engine.tuplequeue.TupleQueue;


public class NonBlockingMultiPortConjunctiveDrainer extends MultiPortDrainer
{

    public NonBlockingMultiPortConjunctiveDrainer ( final int inputPortCount, final int maxBatchSize )
    {
        super( inputPortCount, maxBatchSize );
    }

    protected int[] checkQueueSizes ( final TupleQueue[] tupleQueues )
    {
        int satisfied = 0;
        for ( int i = 0; i < limit; i += 2 )
        {
            final int portIndex = tupleCounts[ i ];
            final int tupleCount = tupleCounts[ i + 1 ];
            if ( tupleQueues[ portIndex ].size() >= tupleCount )
            {
                satisfied++;
            }
        }

        return satisfied == inputPortCount ? tupleCounts : null;
    }

}
