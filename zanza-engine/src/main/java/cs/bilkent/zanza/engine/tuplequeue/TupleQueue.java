package cs.bilkent.zanza.engine.tuplequeue;

import java.util.List;

import cs.bilkent.zanza.operator.Tuple;


public interface TupleQueue
{

    /**
     * Ensures that the tuple queue instance accepts number of tuples given in the parameter without blocking
     *
     * @param capacity
     *         number of tuples guaranteed to be accepted by the tuple queue without blocking
     */
    void ensureCapacity ( int capacity );

    /**
     * Enables the capacity check such that the available capacity is considered while offering tuples into the queue
     */
    void enableCapacityCheck ();

    /**
     * Disables the capacity check such that offers calls are handled as an unbounded queue
     */
    void disableCapacityCheck ();


    /**
     * Returns true if capacity check is enabled for the queue
     *
     * @return true if capacity check is enabled for the queue
     */
    boolean isCapacityCheckEnabled ();

    /**
     * Offers the given tuple by blocking
     *
     * @param tuple
     *         tuple to be offered to the queue
     */
    void offerTuple ( Tuple tuple );

    /**
     * Attempts to offer the given tuple to the queue by blocking with the given timeout value in milliseconds. It may block if the queue
     * has no capacity
     *
     * @param tuple
     *         tuple to be offered to the queue
     * @param timeoutInMillis
     *         duration in milliseconds that is allowed for the call to be blocked if the queue is full
     *
     * @return true if tuple is offered to the queue before timeout occurs, false otherwise
     */
    boolean tryOfferTuple ( Tuple tuple, long timeoutInMillis );

    /**
     * Offers the given tuple to the queue immediately with no capacity checking
     *
     * @param tuple
     *         tuple to be offered to the queue
     */
    void forceOffer ( Tuple tuple );

    /**
     * Offers given tuples to the queue by blocking. If the queue has no enough capacity, the call blocks until all tuples are added
     *
     * @param tuples
     *         tuples to be offered to the queue by blocking
     */
    void offerTuples ( List<Tuple> tuples );

    /**
     * Attempts to offer given tuples to the queue by blocking with the given timeout value in milliseconds. If the queue has
     * no enough capacity within the timeout duration, the call returns before offering all tuples and reports number of tuples
     * offered to the queue
     *
     * @param tuples
     *         tuples to be offered to the queue
     * @param timeoutInMillis
     *         duration in milliseconds that is allowed for the call to be blocked if the queue has no enough capacity
     *
     * @return number of tuples offered into the queue. Please note that it may be less than number of tuples in the first parameter
     */
    int tryOfferTuples ( List<Tuple> tuples, long timeoutInMillis );

    /**
     * Offers the given tuples to the queue immediately with no capacity checking
     *
     * @param tuples
     *         tuples to be offered to the queue
     */
    void forceOfferTuples ( List<Tuple> tuples );

    /**
     * Polls tuples from the queue with the number equal to the given count. It may block or directly return an empty list if the queue has
     * no enough number of tuples
     *
     * @param count
     *         exact number of tuples to be polled from the queue
     *
     * @return list of tuples polled from the queue. It may be an empty list if the queue has no enough number of tuples to be polled
     */
    List<Tuple> pollTuples ( int count );

    /**
     * Polls tuples from the queue with the number equal to the given count into the provided list. It may block or directly return an
     * empty list if the queue has no enough number of tuples
     *
     * @param count
     *         exact number of tuples to be polled from the queue
     */
    void pollTuples ( int count, List<Tuple> tuples );

    /**
     * Polls tuples from the queue with the number equal to the given count. It may block if the queue has no enough number of tuples
     * within the given timeout duration
     *
     * @param count
     *         exact number of tuples to be polled from the queue
     * @param timeoutInMillis
     *         duration in milliseconds that is allowed for the call to be blocked if the queue has no enough number of tuples
     *
     * @return list of tuples polled from the queue. It may be an empty list if the queue has no enough number of tuples to be polled
     * within the given timeout duration
     */
    List<Tuple> pollTuples ( int count, long timeoutInMillis );

    /**
     * Polls tuples from the queue with the number equal to the given count into the provided list. It may block if the queue has no
     * enough number of tuples within the given timeout duration
     *
     * @param count
     *         exact number of tuples to be polled from the queue
     * @param timeoutInMillis
     *         duration in milliseconds that is allowed for the call to be blocked if the queue has no enough number of tuples
     */
    void pollTuples ( int count, long timeoutInMillis, List<Tuple> tuples );

    /**
     * Polls tuples from the queue with the number greater than or equal to the given count. It may block or directly return an empty list
     * if the queue has no enough number of tuples
     *
     * @param count
     *         minimum number of tuples to be polled from the queue
     *
     * @return list of tuples polled from the queue. It may be an empty list if the queue has no enough number of tuples to be polled
     */
    List<Tuple> pollTuplesAtLeast ( int count );

    List<Tuple> pollTuplesAtLeast ( int count, int limit );

    /**
     * Polls tuples from the queue with the number greater than or equal to the given count into the provided list. It may block or
     * directly return an empty list
     * if the queue has no enough number of tuples
     *
     * @param count
     *         minimum number of tuples to be polled from the queue
     */
    void pollTuplesAtLeast ( int count, List<Tuple> tuples );

    void pollTuplesAtLeast ( int count, int limit, List<Tuple> tuples );

    /**
     * Polls tuples from the queue with the number greater than or equal to the given count. It may block if the queue has no
     * enough number of tuples within the given timeout duration
     *
     * @param count
     *         exact number of tuples to be polled from the queue
     * @param timeoutInMillis
     *         duration in milliseconds that is allowed for the call to be blocked if the queue has no enough number of tuples
     *
     * @return list of tuples polled from the queue. It may be an empty list if the queue has no enough number of tuples to be polled
     */
    List<Tuple> pollTuplesAtLeast ( int count, long timeoutInMillis );

    List<Tuple> pollTuplesAtLeast ( int count, int limit, long timeoutInMillis );

    /**
     * Polls tuples from the queue with the number greater than or equal to the given count into the provided list. It may block if the
     * queue has no enough number of tuples within the given timeout duration
     *
     * @param count
     *         exact number of tuples to be polled from the queue
     * @param timeoutInMillis
     *         duration in milliseconds that is allowed for the call to be blocked if the queue has no enough number of tuples
     */
    void pollTuplesAtLeast ( int count, long timeoutInMillis, List<Tuple> tuples );

    void pollTuplesAtLeast ( int count, int limit, long timeoutInMillis, List<Tuple> tuples );

    /**
     * Returns true if the queue has number of tuples greater than or equal to the expected size. It may block or directly return an
     * empty false if the queue has no enough number of tuples
     *
     * @param expectedSize
     *         number of tuples to be present in the queue
     *
     * @return true if the queue has number of tuples greater than or equal to the expected size
     */
    boolean awaitMinimumSize ( int expectedSize );

    /**
     * @param expectedSize
     *         number of tuples to be present in the queue
     * @param timeoutInMillis
     *         duration in milliseconds that is allowed for the call to be blocked if the queue has no enough number of tuples
     *
     * @return true if the queue has number of tuples greater than or equal to the expected size
     */
    boolean awaitMinimumSize ( int expectedSize, long timeoutInMillis );

    int size ();

    boolean isEmpty ();

    default boolean isNonEmpty ()
    {
        return !isEmpty();
    }

    void clear ();

}
