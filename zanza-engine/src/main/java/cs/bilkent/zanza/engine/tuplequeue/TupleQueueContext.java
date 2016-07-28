package cs.bilkent.zanza.engine.tuplequeue;

import java.util.List;

import cs.bilkent.zanza.operator.Tuple;
import cs.bilkent.zanza.operator.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByPort;

/**
 * Manages input {@link Tuple} queues for a single operator instance replica
 */
public interface TupleQueueContext
{

    String getOperatorId ();

    int getInputPortCount ();

    /**
     * Offers given tuples to the tuple queue of the given port index.
     * Blocks until there is available capacity for all tuples and all of them are successfully offered.
     *
     * @param portIndex
     *         port index to offer the tuples
     * @param tuples
     *         tuples to be offered
     */
    void offer ( int portIndex, List<Tuple> tuples );

    /**
     * Attempts to offer given tuples to the queue as many as possible
     *
     * @param portIndex
     *         port index to offer the tuples
     * @param tuples
     *         tuples to be offered
     * @param timeoutInMillis
     *         duration in milliseconds in which tuple offering is attempted
     *
     * @return number of tuples successfully offered
     */
    int tryOffer ( int portIndex, List<Tuple> tuples, long timeoutInMillis );

    /**
     * Offers given tuples into the queue without checking the available queue capacity.
     *
     * @param portIndex
     *         port index to offer the tuples
     * @param tuples
     *         tuples to be offered
     */
    void forceOffer ( int portIndex, List<Tuple> tuples );

    /**
     * Removes tuples from the underlying input queues using the given {@link TupleQueueDrainer}
     *
     * @param drainer
     *         to remove tuples from the underlying input queues
     */
    void drain ( TupleQueueDrainer drainer );

    void ensureCapacity ( int portIndex, int capacity );

    void clear ();

    void setTupleCounts ( int[] tupleCounts, TupleAvailabilityByPort tupleAvailabilityByPort );

    void prepareGreedyDraining ();

    void enableCapacityCheck ( final int portIndex );

    void disableCapacityCheck ( final int portIndex );

    boolean isCapacityCheckEnabled ( final int portIndex );

}
