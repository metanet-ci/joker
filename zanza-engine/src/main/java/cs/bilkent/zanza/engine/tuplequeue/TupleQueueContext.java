package cs.bilkent.zanza.engine.tuplequeue;

import java.util.List;

import cs.bilkent.zanza.operator.PortsToTuples;
import cs.bilkent.zanza.operator.Tuple;
import cs.bilkent.zanza.scheduling.ScheduleWhenTuplesAvailable.PortToTupleCount;

/**
 * Manages input {@link Tuple} queues for a single operator instance replica
 */
public interface TupleQueueContext
{

    String getOperatorId ();

    /**
     * Adds given input tuples to the queues. It may block if the underlying tuple queues are bounded
     *
     * @param input
     *         tuples to add
     */
    void add ( PortsToTuples input );

    /**
     * Tries to add given input tuples to the queues. It may block for {@code timeoutInMillis} parameter if the underlying
     * queue is bounded and does not have enough empty space.
     *
     * @param input
     *         tuples to add
     * @param timeoutInMillis
     *         duration in milliseconds that is allowed for the call to be blocked if the underlying queues have no enough space
     *
     * @return if tuples are not completely added for some ports in the input, number of tuples successfully added to the queues are
     * returned. Otherwise, returns an empty list.
     */
    List<PortToTupleCount> tryAdd ( PortsToTuples input, long timeoutInMillis );

    /**
     * Removes tuples from the underlying input queues using the given {@link TupleQueueDrainer}
     *
     * @param drainer
     *         to remove tuples from the underlying input queues
     */
    void drain ( TupleQueueDrainer drainer );

    void clear ();

}
