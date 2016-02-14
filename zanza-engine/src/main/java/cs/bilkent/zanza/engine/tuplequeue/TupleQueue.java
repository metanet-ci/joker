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
     * Offers the given tuple blockingly
     *
     * @param tuple
     *         tuple to be offered to the queue
     */
    void offerTuple ( Tuple tuple );

    /**
     * Attempts to offer the given tuple to the queue blockingly with the given timeout value in milliseconds. It may block if the queue
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
     * Offers given tuples to the queue blockingly. If the queue has no enough capacity, the call blocks until all tuples are added
     *
     * @param tuples
     *         tuples to be offered to the queue blockingly
     */
    void offerTuples ( List<Tuple> tuples );

    /**
     * Attempts to offer given tuples to the queue blockingly with the given timeout value in milliseconds. If the queue has
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
     * Polls tuples from the queue with the number greater than or equal to the given count. It may block or directly return an empty list
     * if the queue has no enough number of tuples
     *
     * @param count
     *         minimum number of tuples to be polled from the queue
     *
     * @return list of tuples polled from the queue. It may be an empty list if the queue has no enough number of tuples to be polled
     */
    List<Tuple> pollTuplesAtLeast ( int count );

    /**
     * Polls tuples from the queue with the number greater than or equal to the given count. It may block if the queue has no
     * enough number of tuples within the given timeout duration
     *
     * @param count
     *         exact number of tuples to be polled from the queue
     *
     * @return list of tuples polled from the queue. It may be an empty list if the queue has no enough number of tuples to be polled
     */
    List<Tuple> pollTuplesAtLeast ( int count, long timeoutInMillis );

    int size ();

    boolean isEmpty ();

    boolean isNonEmpty ();

    void clear ();

}
