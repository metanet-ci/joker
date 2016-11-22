package cs.bilkent.joker.engine.tuplequeue.impl.queue;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkArgument;
import cs.bilkent.joker.engine.tuplequeue.TupleQueue;
import cs.bilkent.joker.operator.Tuple;
import static java.lang.Math.max;

@ThreadSafe
public class MultiThreadedTupleQueue implements TupleQueue
{

    private static final Logger LOGGER = LoggerFactory.getLogger( MultiThreadedTupleQueue.class );


    private final ReentrantLock lock = new ReentrantLock();

    private final Condition emptyCondition = lock.newCondition();

    private final Condition fullCondition = lock.newCondition();

    @GuardedBy( "monitor" )
    private final ArrayDeque<Tuple> queue;

    private final int capacity;

    public MultiThreadedTupleQueue ( final int initialCapacity )
    {
        this( initialCapacity, new ArrayDeque<>( initialCapacity ) );
    }

    public MultiThreadedTupleQueue ( final int initialCapacity, ArrayDeque<Tuple> queue )
    {
        checkArgument( initialCapacity > 0 );
        this.queue = queue;
        this.capacity = initialCapacity;
    }

    @Override
    public boolean offerTuple ( final Tuple tuple )
    {
        return doOfferTuple( tuple, 0 );
    }

    @Override
    public boolean offerTuple ( final Tuple tuple, final long timeout, final TimeUnit unit )
    {
        return doOfferTuple( tuple, unit.toNanos( timeout ) );
    }

    private boolean doOfferTuple ( final Tuple tuple, long timeoutInNanos )
    {
        checkArgument( tuple != null, "tuple can't be null" );

        lock.lock();
        try
        {
            while ( availableCapacity() <= 0 )
            {
                if ( timeoutInNanos <= 0 )
                {
                    return false;
                }

                timeoutInNanos = awaitNanos( fullCondition, timeoutInNanos );
            }

            queue.add( tuple );
            emptyCondition.signal();
        }
        finally
        {
            lock.unlock();
        }

        return true;
    }

    @Override
    public int offerTuples ( final List<Tuple> tuples )
    {
        return doOfferTuples( tuples, 0, 0 );
    }

    @Override
    public int offerTuples ( final List<Tuple> tuples, final int fromIndex )
    {
        return doOfferTuples( tuples, fromIndex, 0 );
    }

    @Override
    public int offerTuples ( final List<Tuple> tuples, final long timeout, final TimeUnit unit )
    {
        return doOfferTuples( tuples, 0, unit.toNanos( timeout ) );
    }

    @Override
    public int offerTuples ( final List<Tuple> tuples, final int fromIndex, final long timeout, final TimeUnit unit )
    {
        return doOfferTuples( tuples, fromIndex, unit.toNanos( timeout ) );
    }

    private int doOfferTuples ( final List<Tuple> tuples, final int fromIndex, long timeoutInNanos )
    {
        checkArgument( tuples != null, "tuples can't be null" );

        lock.lock();
        try
        {
            int i = fromIndex, j = tuples.size();
            while ( true )
            {
                int availableCapacity;
                while ( ( availableCapacity = availableCapacity() ) <= 0 )
                {
                    if ( timeoutInNanos <= 0 )
                    {
                        return i - fromIndex;
                    }

                    timeoutInNanos = awaitNanos( fullCondition, timeoutInNanos );
                }

                int k = i;
                i += availableCapacity;

                if ( i > j )
                {
                    i = j;
                }

                while ( k < i )
                {
                    queue.add( tuples.get( k++ ) );
                }

                emptyCondition.signal();

                if ( i == j )
                {
                    return i - fromIndex;
                }
            }
        }
        finally
        {
            lock.unlock();
        }
    }

    @Override
    public List<Tuple> pollTuples ( final int count )
    {
        return doPollTuples( count, 0, null );
    }

    @Override
    public List<Tuple> pollTuples ( final int count, final long timeout, final TimeUnit unit )
    {
        return doPollTuples( count, unit.toNanos( timeout ), null );
    }

    @Override
    public void pollTuples ( final int count, final List<Tuple> tuples )
    {
        doPollTuples( count, 0, tuples );
    }

    @Override
    public void pollTuples ( final int count, final List<Tuple> tuples, final long timeout, final TimeUnit unit )
    {
        doPollTuples( count, unit.toNanos( timeout ), tuples );
    }

    private List<Tuple> doPollTuples ( final int count, long timeoutInNanos, List<Tuple> tuples )
    {
        checkArgument( count >= 0 );

        lock.lock();
        try
        {
            while ( queue.size() < count )
            {
                if ( timeoutInNanos <= 0 )
                {
                    return tuples != null ? tuples : Collections.emptyList();
                }

                timeoutInNanos = awaitNanos( emptyCondition, timeoutInNanos );
            }

            if ( tuples == null )
            {
                tuples = new ArrayList<>( count );
            }

            for ( int i = 0; i < count; i++ )
            {
                tuples.add( queue.poll() );
            }

            fullCondition.signalAll();

            return tuples;
        }
        finally
        {
            lock.unlock();
        }
    }

    @Override
    public List<Tuple> pollTuplesAtLeast ( final int count )
    {
        return doPollTuplesAtLeast( count, Integer.MAX_VALUE, 0, null );
    }

    @Override
    public List<Tuple> pollTuplesAtLeast ( final int count, final long timeout, final TimeUnit unit )
    {
        return doPollTuplesAtLeast( count, Integer.MAX_VALUE, unit.toNanos( timeout ), null );
    }

    @Override
    public List<Tuple> pollTuplesAtLeast ( final int count, final int limit )
    {
        return doPollTuplesAtLeast( count, limit, 0, null );
    }

    @Override
    public List<Tuple> pollTuplesAtLeast ( final int count, final int limit, final long timeout, final TimeUnit unit )
    {
        checkArgument( limit >= count );
        return doPollTuplesAtLeast( count, limit, unit.toNanos( timeout ), null );
    }

    @Override
    public void pollTuplesAtLeast ( final int count, final List<Tuple> tuples )
    {
        doPollTuplesAtLeast( count, Integer.MAX_VALUE, 0, tuples );
    }

    @Override
    public void pollTuplesAtLeast ( final int count, final List<Tuple> tuples, final long timeout, final TimeUnit unit )
    {
        doPollTuplesAtLeast( count, Integer.MAX_VALUE, unit.toNanos( timeout ), tuples );
    }

    @Override
    public void pollTuplesAtLeast ( final int count, final int limit, final List<Tuple> tuples )
    {
        checkArgument( limit >= count );
        doPollTuplesAtLeast( count, limit, 0, tuples );
    }

    @Override
    public void pollTuplesAtLeast ( final int count, final int limit, final List<Tuple> tuples, final long timeout, final TimeUnit unit )
    {
        doPollTuplesAtLeast( count, limit, unit.toNanos( timeout ), tuples );
    }

    private List<Tuple> doPollTuplesAtLeast ( final int count, int limit, long timeoutInNanos, List<Tuple> tuples )
    {
        checkArgument( count >= 0 );

        lock.lock();
        try
        {
            // checkArgument( effectiveCapacity >= count );

            while ( queue.size() < count )
            {
                if ( timeoutInNanos <= 0 )
                {
                    return tuples != null ? tuples : Collections.emptyList();
                }

                timeoutInNanos = awaitNanos( emptyCondition, timeoutInNanos );
            }

            if ( tuples == null )
            {
                tuples = new ArrayList<>( count );
            }

            final Iterator<Tuple> it = queue.iterator();
            while ( it.hasNext() && limit-- > 0 )
            {
                tuples.add( it.next() );
                it.remove();
            }

            fullCondition.signalAll();

            return tuples;
        }
        finally
        {
            lock.unlock();
        }
    }

    @Override
    public boolean awaitMinimumSize ( final int expectedSize, final long timeout, final TimeUnit unit )
    {
        return doAwaitMinimumSize( expectedSize, unit.toNanos( timeout ) );
    }

    private boolean doAwaitMinimumSize ( final int expectedSize, long timeoutInNanos )
    {
        lock.lock();
        try
        {
            while ( queue.size() < expectedSize )
            {
                if ( timeoutInNanos <= 0 )
                {
                    return false;
                }

                timeoutInNanos = awaitNanos( emptyCondition, timeoutInNanos );
            }
        }
        finally
        {
            lock.unlock();
        }

        return true;
    }

    @Override
    public int size ()
    {
        lock.lock();
        try
        {
            return queue.size();
        }
        finally
        {
            lock.unlock();
        }
    }

    @Override
    public void clear ()
    {
        lock.lock();
        try
        {
            queue.clear();

            fullCondition.signalAll();
        }
        finally
        {
            lock.unlock();
        }
    }

    public SingleThreadedTupleQueue toSingleThreadedTupleQueue ()
    {
        return new SingleThreadedTupleQueue( this.queue );
    }

    private long awaitNanos ( final Condition condition, final long durationInNanos )
    {
        try
        {
            return condition.awaitNanos( durationInNanos );
        }
        catch ( InterruptedException e )
        {
            LOGGER.warn( "Thread {} interrupted while waiting in queue", Thread.currentThread().getName() );
            Thread.currentThread().interrupt();
            return 0;
        }
    }

    private int availableCapacity ()
    {
        return max( ( capacity - queue.size() ), 0 );
    }

}
