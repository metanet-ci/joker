package cs.bilkent.joker.engine.tuplequeue.impl.drainer;

import static com.google.common.base.Preconditions.checkArgument;
import cs.bilkent.joker.engine.partition.PartitionKey;
import cs.bilkent.joker.engine.tuplequeue.TupleQueue;


public class NonBlockingSinglePortDrainer extends SinglePortDrainer
{

    public NonBlockingSinglePortDrainer ( final int maxBatchSize )
    {
        super( maxBatchSize );
    }

    @Override
    public void drain ( final PartitionKey key, final TupleQueue[] tupleQueues )
    {
        checkArgument( tupleQueues != null );
        checkArgument( tupleQueues.length == 1 );

        final TupleQueue tupleQueue = tupleQueues[ 0 ];

        if ( tupleQueue.size() >= tupleCountToCheck )
        {
            tupleQueue.poll( tupleCountToPoll, tuples );
            this.result = buffer;
            this.key = key;
        }
    }

}
