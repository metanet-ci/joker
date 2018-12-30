package cs.bilkent.joker.engine.tuplequeue.impl.drainer;

import cs.bilkent.joker.engine.tuplequeue.TupleQueue;


public class NonBlockingMultiPortConjunctiveDrainer extends MultiPortDrainer
{

    public NonBlockingMultiPortConjunctiveDrainer ( final String operatorId, final int inputPortCount, final int maxBatchSize )
    {
        super( operatorId, inputPortCount, maxBatchSize );
    }

    protected int[] checkQueueSizes ( final TupleQueue[] tupleQueues )
    {
        int satisfied = 0;
        for ( int i = 0; i < limit; i += 2 )
        {
            final int portIndex = tupleCountsToCheck[ i ];
            final int tupleCountIndex = i + 1;
            final int tupleCount = tupleCountsToCheck[ tupleCountIndex ];

            if ( tupleCount == NO_TUPLES_AVAILABLE || tupleQueues[ portIndex ].size() >= tupleCount )
            {
                satisfied++;
            }
        }

        return ( satisfied == inputPortCount ) ? tupleCountsToDrain : null;
    }

}
