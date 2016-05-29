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
        for ( int portIndex = 0; portIndex < inputPortCount; portIndex++ )
        {
            final int tupleCount = tupleCounts[ portIndex ];
            if ( tupleQueues[ portIndex ].size() >= tupleCount )
            {
                satisfied++;
            }
        }

        return satisfied == inputPortCount ? tupleCounts : null;
    }

}
