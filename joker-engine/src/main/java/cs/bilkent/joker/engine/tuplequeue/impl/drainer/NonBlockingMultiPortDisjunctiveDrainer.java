package cs.bilkent.joker.engine.tuplequeue.impl.drainer;

import cs.bilkent.joker.engine.tuplequeue.TupleQueue;


public class NonBlockingMultiPortDisjunctiveDrainer extends MultiPortDrainer
{

    public NonBlockingMultiPortDisjunctiveDrainer ( final String operatorId, final int inputPortCount, final int maxBatchSize )
    {
        super( operatorId, inputPortCount, maxBatchSize );
    }

    @Override
    protected int[] checkQueueSizes ( final TupleQueue[] tupleQueues )
    {
        boolean satisfied = false;
        for ( int i = 0; i < limit; i += 2 )
        {
            final int portIndex = tupleCountsToCheck[ i ];
            final int tupleCountIndex = i + 1;
            final int tupleCount = tupleCountsToCheck[ tupleCountIndex ];

            if ( tupleCount != NO_TUPLES_AVAILABLE )
            {
                if ( tupleQueues[ portIndex ].size() >= tupleCount )
                {
                    tupleCountsBuffer[ tupleCountIndex ] = tupleCountsToDrain[ tupleCountIndex ];
                    satisfied = true;
                }
                else
                {
                    tupleCountsBuffer[ tupleCountIndex ] = NO_TUPLES_AVAILABLE;
                }
            }
        }

        return satisfied ? tupleCountsBuffer : null;
    }

}
