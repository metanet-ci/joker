package cs.bilkent.joker.engine.tuplequeue;

import java.util.List;
import java.util.concurrent.TimeUnit;

import cs.bilkent.joker.operator.Tuple;


public interface TupleQueue
{

    /**
     * Attempts to offer the given tuple if there is available capacity in the queue.
     * Please note that this method does not block and returns {@code false} immediately if there is no empty capacity.
     *
     * @param tuple
     *         tuple to be offered to the queue
     *
     * @return true if the offer is successful, false otherwise
     */
    boolean offerTuple ( Tuple tuple );

    /**
     * Attempts to offer the given tuple to the queue. If there is no empty capacity, it blocks for the given timeout duration.
     *
     * @param tuple
     *         tuple to be offered to the queue
     *
     * @return true if tuple is offered to the queue before the timeout, false otherwise
     */
    boolean offerTuple ( Tuple tuple, long timeout, TimeUnit unit );

    /**
     * Attempts to offer given tuples to the queue without blocking, and returns the number of tuples that are successfully offered.
     *
     * @param tuples
     *         tuples to be offered to the queue without blocking
     *
     * @return number of tuples that are successfully offered
     */
    int offerTuples ( List<Tuple> tuples );

    /**
     * Attempts to offer given tuples to the queue without blocking, starting from the given index (inclusive),
     * and returns the number of tuples that are successfully offered.
     *
     * @param tuples
     *         tuples to be offered to the queue without blocking
     * @param fromIndex
     *         starting index of the tuples to be offered (inclusive)
     *
     * @return number of tuples that are successfully offered
     */
    int offerTuples ( List<Tuple> tuples, int fromIndex );

    /**
     * Attempts to offer given tuples to the queue. If there is no empty capacity, it blocks for the given timeout duration.
     * It returns the number of tuples that are successfully offered.
     *
     * @param tuples
     *         tuples to be offered to the queue
     *
     * @return number of tuples that are successfully offered
     */
    int offerTuples ( List<Tuple> tuples, long timeout, TimeUnit unit );

    /**
     * Attempts to offer given tuples to the queue, starting from the given index (inclusive). If there is no empty capacity,
     * it blocks for the given timeout duration. It returns the number of tuples that are successfully offered.
     *
     * @param tuples
     *         tuples to be offered to the queue
     * @param fromIndex
     *         starting index of the tuples to be offered (inclusive)
     *
     * @return number of tuples that are successfully offered
     */
    int offerTuples ( List<Tuple> tuples, int fromIndex, long timeout, TimeUnit unit );

    /**
     * Polls tuples from the queue with the number equal to the given count without blocking, if the queue already has enough tuples.
     * It does not block and returns an empty list immediately if the queue has no enough number of tuples.
     *
     * @param count
     *         exact number of tuples to be polled from the queue
     *
     * @return list of tuples polled from the queue. It may be an empty list if the queue has no enough number of tuples to be polled on
     * the invocation time
     */
    List<Tuple> pollTuples ( int count );

    /**
     * Polls tuples from the queue with the number equal to the given count.
     * It blocks for the given timeout duration if the queue has not enough number of tuples.
     *
     * @param count
     *         exact number of tuples to be polled from the queue
     *
     * @return list of tuples polled from the queue. It may be an empty list if the queue has no enough number of tuples to be polled
     * within the given timeout duration
     */
    List<Tuple> pollTuples ( int count, long timeout, TimeUnit unit );

    /**
     * Polls tuples from the queue with the number equal to the given count into the given list, without blocking.
     * It does not modify the given list if the queue has no enough number of tuples.
     *
     * @param count
     *         exact number of tuples to be polled from the queue
     * @param tuples
     *         list to add the polled tuples
     */
    void pollTuples ( int count, List<Tuple> tuples );

    /**
     * Polls tuples from the queue with the number equal to the given count into the provided list.
     * It blocks for the given timeout duration if the queue has no enough number of tuples.
     *
     * @param count
     *         exact number of tuples to be polled from the queue
     * @param tuples
     *         list to add the polled tuples
     */
    void pollTuples ( int count, List<Tuple> tuples, long timeout, TimeUnit unit );

    /**
     * Polls tuples from the queue with the number greater than or equal to the given count, without blocking.
     * It immediately returns an empty list if the queue has no enough number of tuples.
     *
     * @param count
     *         minimum number of tuples to be polled from the queue
     *
     * @return list of the tuples polled from the queue. It is an empty list if the queue has no enough number of tuples.
     */
    List<Tuple> pollTuplesAtLeast ( int count );

    /**
     * Polls tuples from the queue with the number greater than or equal to the given count.
     * It blocks for the given timeout duration if the queue has no enough number of tuples.
     *
     * @param count
     *         exact number of tuples to be polled from the queue
     *
     * @return list of tuples polled from the queue. It is an empty list if the queue has no enough number of tuples.
     */
    List<Tuple> pollTuplesAtLeast ( int count, long timeout, TimeUnit unit );

    /**
     * Polls tuples from the queue with the number greater than or equal to the given count, and less than or equal to the given limit,
     * without blocking. It immediately returns an empty list if the queue has no enough number of tuples.
     *
     * @param count
     *         minimum number of tuples to be polled from the queue
     * @param limit
     *         maximum number of tuples to be polled from the queue
     *
     * @return list of tuples polled from the queue. It is an empty list if the queue has no enough number of tuples.
     */
    List<Tuple> pollTuplesAtLeast ( int count, int limit );

    /**
     * Polls tuples from the queue with the number greater than or equal to the given count, and less than or equal to the given limit.
     * It blocks for the given timeout duration if the queue has no enough number of tuples.
     *
     * @param count
     *         exact number of tuples to be polled from the queue
     * @param limit
     *         maximum number of tuples to be polled from the queue
     *
     * @return list of tuples polled from the queue. It is an empty list if the queue has no enough number of tuples.
     */
    List<Tuple> pollTuplesAtLeast ( int count, int limit, long timeout, TimeUnit unit );

    /**
     * Polls tuples from the queue with the number greater than or equal to the given count into the provided list, without blocking.
     * It does not modify the given list if the queue has no enough number of tuples.
     *
     * @param count
     *         minimum number of tuples to be polled from the queue
     * @param tuples
     *         list to add the polled tuples
     */
    void pollTuplesAtLeast ( int count, List<Tuple> tuples );

    /**
     * Polls tuples from the queue with the number greater than or equal to the given count into the provided list.
     * It blocks for the given timeout duration if the queue has no enough number of tuples.
     *
     * @param count
     *         exact number of tuples to be polled from the queue
     * @param tuples
     *         list to add the polled tuples
     */
    void pollTuplesAtLeast ( int count, List<Tuple> tuples, long timeout, TimeUnit unit );

    /**
     * Polls tuples from the queue with the number greater than or equal to the given count, and less than or equal to the given limit
     * into the provided list, without blocking. It does not modify the given list if the queue has no enough number of tuples.
     *
     * @param count
     *         minimum number of tuples to be polled from the queue
     * @param limit
     *         maximum number of tuples to be polled from the queue
     * @param tuples
     *         list to add the polled tuples
     */
    void pollTuplesAtLeast ( int count, int limit, List<Tuple> tuples );

    /**
     * Polls tuples from the queue with the number greater than or equal to the given count, and less than or equal to the given limit
     * into the provided list. It blocks for the given timeout duration if the queue has no enough number of tuples.
     *
     * @param count
     *         exact number of tuples to be polled from the queue
     * @param limit
     *         maximum number of tuples to be polled from the queue
     * @param tuples
     *         list to add the polled tuples
     */
    void pollTuplesAtLeast ( int count, int limit, List<Tuple> tuples, long timeout, TimeUnit unit );

    /**
     * @param expectedSize
     *         number of tuples to be present in the queue
     *
     * @return true if the queue has number of tuples greater than or equal to the expected size
     */
    boolean awaitMinimumSize ( int expectedSize, long timeout, TimeUnit unit );

    int size ();

    default boolean isEmpty ()
    {
        return size() == 0;
    }

    default boolean isNonEmpty ()
    {
        return size() > 0;
    }

    void clear ();

}
