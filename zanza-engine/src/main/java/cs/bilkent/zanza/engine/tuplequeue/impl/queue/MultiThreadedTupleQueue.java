package cs.bilkent.zanza.engine.tuplequeue.impl.queue;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkArgument;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueue;
import cs.bilkent.zanza.operator.Tuple;
import static java.lang.Math.max;
import static java.lang.System.nanoTime;

@ThreadSafe
public class MultiThreadedTupleQueue implements TupleQueue
{

    private static Logger LOGGER = LoggerFactory.getLogger( MultiThreadedTupleQueue.class );


    private final ReentrantLock lock = new ReentrantLock();

    private final Condition emptyCondition = lock.newCondition();

    private final Condition fullCondition = lock.newCondition();

    @GuardedBy( "monitor" )
    private final Queue<Tuple> queue;

    // TODO volatile keyword is removed. check this
    private int capacity;

    private boolean capacityCheckEnabled;

    public MultiThreadedTupleQueue ( final int initialCapacity )
    {
        this( initialCapacity, true );
    }

    public MultiThreadedTupleQueue ( final int initialCapacity, final boolean capacityCheckEnabled )
    {
        checkArgument( initialCapacity > 0 );
        this.queue = new ArrayDeque<>( initialCapacity );
        this.capacity = initialCapacity;
        this.capacityCheckEnabled = capacityCheckEnabled;
    }

    @Override
    public void ensureCapacity ( final int newCapacity )
    {
        if ( newCapacity > capacity )
        {
            lock.lock();
            try
            {
                capacity = newCapacity;
                fullCondition.signalAll();
            }
            finally
            {
                lock.unlock();
            }
        }
    }

    @Override
    public void enableCapacityCheck ()
    {
        lock.lock();
        try
        {
            capacityCheckEnabled = true;
        }
        finally
        {
            lock.unlock();
        }
    }

    @Override
    public void disableCapacityCheck ()
    {
        lock.lock();
        try
        {
            capacityCheckEnabled = false;
            fullCondition.signalAll();
        }
        finally
        {
            lock.unlock();
        }
    }

    @Override
    public boolean isCapacityCheckEnabled ()
    {
        lock.lock();
        try
        {
            return capacityCheckEnabled;
        }
        finally
        {
            lock.unlock();
        }
    }

    @Override
    public void offerTuple ( final Tuple tuple )
    {
        doOfferTuple( tuple, Long.MAX_VALUE );
    }

    @Override
    public boolean tryOfferTuple ( final Tuple tuple, final long timeoutInMillis )
    {
        return doOfferTuple( tuple, timeoutInMillis * 1_000_000 );
    }

    @Override
    public void forceOffer ( final Tuple tuple )
    {
        lock.lock();
        try
        {
            queue.add( tuple );
            emptyCondition.signal();
        }
        finally
        {
            lock.unlock();
        }
    }

    private boolean doOfferTuple ( final Tuple tuple, final long timeoutInNanos )
    {
        checkArgument( tuple != null, "tuple can't be null" );

        final long startNanos = nanoTime();

        lock.lock();
        try
        {
            while ( availableCapacity() <= 0 )
            {
                final long remainingNanos = timeoutInNanos - ( nanoTime() - startNanos );
                if ( remainingNanos <= 0 )
                {
                    return false;
                }

                awaitInNanos( fullCondition, remainingNanos );
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
    public void offerTuples ( final List<Tuple> tuples )
    {
        doOfferTuples( tuples, Long.MAX_VALUE );
    }

    @Override
    public int tryOfferTuples ( final List<Tuple> tuples, final long timeoutInMillis )
    {
        return doOfferTuples( tuples, timeoutInMillis * 1_000_000 );
    }

    @Override
    public void forceOfferTuples ( final List<Tuple> tuples )
    {
        lock.lock();
        try
        {
            queue.addAll( tuples );
            emptyCondition.signal();
        }
        finally
        {
            lock.unlock();
        }
    }

    private int doOfferTuples ( final List<Tuple> tuples, final long timeoutInNanos )
    {
        checkArgument( tuples != null, "tuples can't be null" );

        final long startNanos = nanoTime();

        lock.lock();
        try
        {
            int i = 0, j = tuples.size();
            while ( true )
            {
                int availableCapacity;
                while ( ( availableCapacity = availableCapacity() ) <= 0 )
                {
                    final long remainingNanos = timeoutInNanos - ( nanoTime() - startNanos );
                    if ( remainingNanos <= 0 )
                    {
                        return i;
                    }

                    awaitInNanos( fullCondition, remainingNanos );
                }

                int k = i;
                if ( availableCapacity == Integer.MAX_VALUE )
                {
                    i = j;
                }
                else
                {
                    i += availableCapacity;

                    if ( i > j )
                    {
                        i = j;
                    }
                }

                while ( k < i )
                {
                    queue.add( tuples.get( k++ ) );
                }

                emptyCondition.signal();

                if ( i == j )
                {
                    return i;
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
        return doPollTuples( count, Long.MAX_VALUE, null );
    }

    @Override
    public void pollTuples ( final int count, final List<Tuple> tuples )
    {
        doPollTuples( count, Long.MAX_VALUE, tuples );
    }

    @Override
    public List<Tuple> pollTuples ( final int count, final long timeoutInMillis )
    {
        return doPollTuples( count, timeoutInMillis * 1_000_000, null );
    }

    @Override
    public void pollTuples ( final int count, final long timeoutInMillis, final List<Tuple> tuples )
    {
        doPollTuples( count, timeoutInMillis * 1_000_000, tuples );
    }

    private List<Tuple> doPollTuples ( final int count, final long timeoutInNanos, List<Tuple> tuples )
    {
        checkArgument( count >= 0 );
        final long startNanos = nanoTime();

        lock.lock();
        try
        {
            if ( capacityCheckEnabled )
            {
                checkArgument( capacity >= count );
            }

            while ( queue.size() < count )
            {
                final long remainingNanos = timeoutInNanos - ( nanoTime() - startNanos );
                if ( remainingNanos <= 0 )
                {
                    return tuples != null ? tuples : Collections.emptyList();
                }

                awaitInNanos( emptyCondition, remainingNanos );
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
        return doPollTuplesAtLeast( count, Integer.MAX_VALUE, Long.MAX_VALUE, null );
    }

    @Override
    public List<Tuple> pollTuplesAtLeast ( final int count, final int limit )
    {
        return doPollTuplesAtLeast( count, limit, Long.MAX_VALUE, null );
    }

    @Override
    public void pollTuplesAtLeast ( final int count, final List<Tuple> tuples )
    {
        doPollTuplesAtLeast( count, Integer.MAX_VALUE, Long.MAX_VALUE, tuples );
    }

    @Override
    public void pollTuplesAtLeast ( final int count, final int limit, final List<Tuple> tuples )
    {
        checkArgument( limit >= count );
        doPollTuplesAtLeast( count, limit, Long.MAX_VALUE, tuples );
    }

    @Override
    public List<Tuple> pollTuplesAtLeast ( final int count, final long timeoutInMillis )
    {
        return doPollTuplesAtLeast( count, Integer.MAX_VALUE, timeoutInMillis * 1_000_000, null );
    }

    @Override
    public List<Tuple> pollTuplesAtLeast ( final int count, final int limit, final long timeoutInMillis )
    {
        checkArgument( limit >= count );
        return doPollTuplesAtLeast( count, limit, timeoutInMillis * 1_000_000, null );
    }

    @Override
    public void pollTuplesAtLeast ( final int count, final long timeoutInMillis, final List<Tuple> tuples )
    {
        doPollTuplesAtLeast( count, Integer.MAX_VALUE, timeoutInMillis * 1_000_000, tuples );
    }

    @Override
    public void pollTuplesAtLeast ( final int count, final int limit, final long timeoutInMillis, final List<Tuple> tuples )
    {
        doPollTuplesAtLeast( count, limit, timeoutInMillis * 1_000_000, tuples );
    }

    private List<Tuple> doPollTuplesAtLeast ( final int count, int limit, final long timeoutInNanos, List<Tuple> tuples )
    {
        checkArgument( count >= 0 );

        final long startNanos = nanoTime();
        lock.lock();
        try
        {
            if ( capacityCheckEnabled )
            {
                checkArgument( capacity >= count );
            }

            while ( queue.size() < count )
            {
                final long remainingNanos = timeoutInNanos - ( nanoTime() - startNanos );
                if ( remainingNanos <= 0 )
                {
                    return tuples != null ? tuples : Collections.emptyList();
                }

                awaitInNanos( emptyCondition, remainingNanos );
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
    public boolean awaitMinimumSize ( final int expectedSize )
    {
        return doAwaitMinimumSize( expectedSize, Long.MAX_VALUE );
    }

    @Override
    public boolean awaitMinimumSize ( final int expectedSize, final long timeoutInMillis )
    {
        return doAwaitMinimumSize( expectedSize, timeoutInMillis * 1_000_000 );
    }

    private boolean doAwaitMinimumSize ( final int expectedSize, final long timeoutInNanos )
    {
        final long startNanos = nanoTime();

        lock.lock();
        try
        {
            while ( queue.size() < expectedSize )
            {
                final long remainingNanos = timeoutInNanos - ( nanoTime() - startNanos );
                if ( remainingNanos <= 0 )
                {
                    return false;
                }

                awaitInNanos( emptyCondition, remainingNanos );
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

    private void awaitInNanos ( final Condition condition, final long durationInNanos )
    {
        try
        {
            condition.awaitNanos( durationInNanos );
        }
        catch ( InterruptedException e )
        {
            LOGGER.warn( "Thread {} interrupted while waiting in queue", Thread.currentThread().getName() );
            Thread.currentThread().interrupt();
        }
    }

    private int availableCapacity ()
    {
        if ( capacityCheckEnabled )
        {
            final int availableCapacity = capacity - queue.size();
            return max( availableCapacity, 0 );
        }

        return Integer.MAX_VALUE;
    }

}
