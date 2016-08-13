package cs.bilkent.joker.engine.tuplequeue.impl.drainer;

import cs.bilkent.joker.engine.tuplequeue.TupleQueue;


public class NonBlockingMultiPortDisjunctiveDrainer extends MultiPortDrainer
{

    public NonBlockingMultiPortDisjunctiveDrainer ( final int inputPortCount, final int maxBatchSize )
    {
        super( inputPortCount, maxBatchSize );
    }

    protected int[] checkQueueSizes ( final TupleQueue[] tupleQueues )
    {
        return tupleCounts;
    }

}
