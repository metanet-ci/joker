package cs.bilkent.joker.engine.tuplequeue.impl.drainer;

import java.util.function.Function;
import javax.annotation.Nullable;

import cs.bilkent.joker.engine.tuplequeue.TupleQueue;
import cs.bilkent.joker.engine.tuplequeue.TupleQueueDrainer;
import cs.bilkent.joker.operator.impl.TuplesImpl;
import cs.bilkent.joker.partition.impl.PartitionKey;

public class EmptyDrainer implements TupleQueueDrainer
{

    @Override
    public boolean drain ( final boolean maySkipBlocking,
                           @Nullable final PartitionKey key,
                           final TupleQueue[] tupleQueues,
                           final Function<PartitionKey, TuplesImpl> tuplesSupplier )
    {
        tuplesSupplier.apply( key );
        return false;
    }

}
