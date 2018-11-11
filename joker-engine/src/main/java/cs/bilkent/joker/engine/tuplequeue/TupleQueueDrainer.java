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

    /**
     * Drains tuple queues of which tuples have the given partition key
     *
     * @param key
     *         partition key of the tuples which reside in the given tuple queues. Allowed to be null if tuples do not have a partition key
     * @param queues
     *         tuple queues to be drained
     */
    boolean drain ( @Nullable PartitionKey key, TupleQueue[] queues, Function<PartitionKey, TuplesImpl> tuplesSupplier );

}
