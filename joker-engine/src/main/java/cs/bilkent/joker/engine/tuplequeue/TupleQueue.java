package cs.bilkent.joker.engine.tuplequeue;

import java.util.Collection;
import java.util.List;

import cs.bilkent.joker.operator.Tuple;


public interface TupleQueue
{

    /**
     * Offers the given tuple if there is available capacity in the queue.
     *
     * @param tuple
     *         tuple to be offered to the queue
     *
     * @return true if the offer is successful, false otherwise
     */
    boolean offer ( Tuple tuple );

    /**
     * Offers given tuples to the queue, and returns the number of tuples that are successfully offered.
     * If the queue has not enough capacity, all of the given tuples may not be offered.
     *
     * @param tuples
     *         tuples to be offered to the queue without blocking
     *
     * @return number of tuples that are successfully offered
     */
    int offer ( List<Tuple> tuples );

    /**
     * Offer given tuples to the queue, starting from the given index (inclusive), and returns the number of tuples
     * that are successfully offered. If the queue has not enough capacity, all of the given tuples may not be offered.
     *
     * @param tuples
     *         tuples to be offered to the queue without blocking
     * @param fromIndex
     *         starting index of the tuples to be offered (inclusive)
     *
     * @return number of tuples that are successfully offered
     */
    int offer ( List<Tuple> tuples, int fromIndex );

    /**
     * Polls a single tuple from the queue, if there is any
     *
     * @return polled tuple if there is any, null otherwise
     */
    Tuple poll ();

    /**
     * Polls tuples from the queue with the number at most equal to the given limit value.
     *
     * @param limit
     *         maximum number of tuples to be polled from the queue
     *
     * @return list of tuples polled from the queue.
     */
    List<Tuple> poll ( int limit );

    /**
     * Polls tuples from the queue with the number at most equal to the given limit value.
     *
     * @param limit
     *         maximum number of tuples to be polled from the queue
     * @param tuples
     *         list to add the polled tuples
     */
    int poll ( int limit, Collection<Tuple> tuples );

    int size ();

    default boolean isEmpty ()
    {
        return size() == 0;
    }

    void clear ();

    boolean ensureCapacity ( int capacity );

}
