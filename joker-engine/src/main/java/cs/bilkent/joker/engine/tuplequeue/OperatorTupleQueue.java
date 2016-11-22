package cs.bilkent.joker.engine.tuplequeue;

import java.util.List;
import java.util.concurrent.TimeUnit;

import cs.bilkent.joker.operator.Tuple;
import cs.bilkent.joker.operator.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByPort;

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

    /**
     * Attempts to offer given tuples to the tuple queue of given port index. If there is no empty capacity,
     * it blocks for the given timeout duration. It returns the number of tuples that are successfully offered.
     *
     * @param portIndex
     *         port index to offer the tuples
     * @param tuples
     *         tuples to be offered
     *
     * @return number of tuples that are successfully offered
     */
    int offer ( int portIndex, List<Tuple> tuples, long timeout, TimeUnit unit );

    /**
     * Attempts to offer given tuples to the tuple queue of given port index, starting from the given index (inclusive).
     * If there is no empty capacity, it blocks for the given timeout duration.
     * It returns the number of tuples that are successfully offered.
     *
     * @param portIndex
     *         port index to offer the tuples
     * @param tuples
     *         tuples to be offered
     *
     * @return number of tuples that are successfully offered
     */
    int offer ( int portIndex, List<Tuple> tuples, int fromIndex, long timeout, TimeUnit unit );

    /**
     * Removes tuples from the underlying input queues using the given {@link TupleQueueDrainer}
     *
     * @param drainer
     *         to remove tuples from the underlying input queues
     */
    void drain ( TupleQueueDrainer drainer );

    void clear ();

    void setTupleCounts ( int[] tupleCounts, TupleAvailabilityByPort tupleAvailabilityByPort );

    boolean isOverloaded ();

    boolean isEmpty ();

}
