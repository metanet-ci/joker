package cs.bilkent.joker.engine.tuplequeue;

import java.util.function.Function;
import javax.annotation.Nullable;

import cs.bilkent.joker.operator.impl.TuplesImpl;
import cs.bilkent.joker.partition.impl.PartitionKey;

/**
 * Drains tuples from given tuple queues and keeps them in internally to be able to return them afterwards.
 * Implementations can be stateful.
 */
public interface TupleQueueDrainer
{

    default boolean drain ( @Nullable PartitionKey key, TupleQueue[] tupleQueues, Function<PartitionKey, TuplesImpl> tuplesSupplier )
    {
        return drain( false, key, tupleQueues, tuplesSupplier );
    }

    /**
     * Drains tuple queues of which tuples have the given partition key
     *
     * @param maySkipBlocking
     *         a boolean flag to specify if the drainer may not block if it is a blocking drainer
     * @param key
     *         partition key of the tuples which reside in the given tuple queues. Allowed to be null if tuples do not have a partition key
     * @param tupleQueues
     *         tuple queues to be drained
     */
    boolean drain ( boolean maySkipBlocking,
                    @Nullable PartitionKey key,
                    TupleQueue[] tupleQueues,
                    Function<PartitionKey, TuplesImpl> tuplesSupplier );

}
