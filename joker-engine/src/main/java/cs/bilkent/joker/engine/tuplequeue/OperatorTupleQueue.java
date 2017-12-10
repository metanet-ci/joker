package cs.bilkent.joker.engine.tuplequeue;

import java.util.List;
import java.util.function.Function;

import cs.bilkent.joker.operator.Tuple;
import cs.bilkent.joker.operator.impl.TuplesImpl;
import cs.bilkent.joker.operator.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByPort;
import cs.bilkent.joker.partition.impl.PartitionKey;

/**
 * Manages input {@link Tuple} queues for a single operator instance replica
 */
public interface OperatorTupleQueue
{

    String getOperatorId ();

    int getInputPortCount ();

    /**
     * Attempts to offer given tuples to the tuple queue of given port index without blocking,
     * and returns the number of tuples that are successfully offered.
     *
     * @param portIndex
     *         port index to offer the tuples
     * @param tuples
     *         tuples to be offered
     *
     * @return number of tuples that are successfully offered
     */
    int offer ( int portIndex, List<Tuple> tuples );

    /**
     * Attempts to offer given tuples to the tuple queue of given port index without blocking,
     * starting from the given index (inclusive), and returns the number of tuples that are successfully offered.
     *
     * @param portIndex
     *         port index to offer the tuples
     * @param tuples
     *         tuples to be offered
     * @param fromIndex
     *         starting index of the tuples to be offered (inclusive)
     *
     * @return number of tuples that are successfully offered
     */
    int offer ( int portIndex, List<Tuple> tuples, int fromIndex );


    default void drain ( TupleQueueDrainer drainer, Function<PartitionKey, TuplesImpl> tuplesSupplier )
    {
        drain( false, drainer, tuplesSupplier );
    }

    /**
     * Removes tuples from the underlying input queues using the given {@link TupleQueueDrainer}
     *
     * @param maySkipBlocking
     *         a boolean flag which is passed to drainer to specify if the drainer may not block if it is a blocking drainer
     * @param drainer
     *         to remove tuples from the underlying input queues
     */
    void drain ( boolean maySkipBlocking, TupleQueueDrainer drainer, Function<PartitionKey, TuplesImpl> tuplesSupplier );

    void clear ();

    void setTupleCounts ( int[] tupleCounts, TupleAvailabilityByPort tupleAvailabilityByPort );

    boolean isEmpty ();

    void ensureCapacity ( int capacity );

}
