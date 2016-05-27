package cs.bilkent.zanza.engine.tuplequeue;

import java.util.List;

import cs.bilkent.zanza.operator.Tuple;

/**
 * Manages input {@link Tuple} queues for a single operator instance replica
 */
public interface TupleQueueContext
{

    String getOperatorId ();

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

    /**
     * Ensures that the tuple queue instance accepts number of tuples given in the parameter without blocking
     *
     * @param capacity
     *         number of tuples guaranteed to be accepted by the tuple queue without blocking
     */
    void ensureCapacity ( int portIndex, int capacity );

    /**
     * Enables capacity check of the tuple queue for the given port index
     *
     * @param portIndex
     *         of the tuple queue to enable capacity check
     */
    void enableCapacityCheck ( int portIndex );

    /**
     * Disables capacity check of the tuple queue for the given port index
     *
     * @param portIndex
     *         of the tuple queue to disable capacity check
     */
    void disableCapacityCheck ( int portIndex );

    /**
     * Checks if the capacity check is enabled for the tuple queue of the given port index
     *
     * @param portIndex
     *         of the tuple queue to check the capacity check
     *
     * @return true if capacity check is enabled for the tuple queue of the given port index
     */
    boolean isCapacityCheckEnabled ( int portIndex );

    /**
     * Checks if the capacity check is disabled for the tuple queue of the given port index
     *
     * @param portIndex
     *         of the tuple queue to check the capacity check
     *
     * @return true if capacity check is disabled for the tuple queue of the given port index
     */
    boolean isCapacityCheckDisabled ( int portIndex );

    void clear ();

}
