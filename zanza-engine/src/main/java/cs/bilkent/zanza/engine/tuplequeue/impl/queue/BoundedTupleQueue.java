package cs.bilkent.zanza.engine.tuplequeue.impl.queue;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkArgument;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueue;
import cs.bilkent.zanza.operator.Tuple;

@ThreadSafe
public class BoundedTupleQueue implements TupleQueue
{

    private static Logger LOGGER = LoggerFactory.getLogger( BoundedTupleQueue.class );

    public static final int DEFAULT_WAIT_TIME_IN_MILLIS = 50;


    private final Object monitor = new Object();

    @GuardedBy( "monitor" )
    private final Queue<Tuple> queue;

    private volatile int capacity;

    public BoundedTupleQueue ( final int initialCapacity )
    {
        this.queue = new ArrayDeque<>( initialCapacity );
        this.capacity = initialCapacity;
    }

    @Override
    public void ensureCapacity ( final int newCapacity )
    {
        if ( newCapacity > capacity )
        {
            capacity = newCapacity;
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

    private boolean doOfferTuple ( final Tuple tuple, final long timeoutInNanos )
    {
        checkArgument( tuple != null, "tuple can't be null" );

        final long startNanos = System.nanoTime();
        int capacity = this.capacity;
        synchronized ( monitor )
        {
            while ( queue.size() >= capacity )
            {
                if ( ( System.nanoTime() - startNanos ) >= timeoutInNanos )
                {
                    return false;
                }

                monitorWait();
                capacity = this.capacity;
            }

            queue.add( tuple );
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

    private int doOfferTuples ( final List<Tuple> tuples, final long timeoutInNanos )
    {
        checkArgument( tuples != null, "tuples can't be null" );

        final long startNanos = System.nanoTime();
        synchronized ( monitor )
        {
            int i = 0, j = tuples.size();
            while ( true )
            {
                int capacity = this.capacity;

                int availableCapacity;
                while ( ( availableCapacity = ( capacity - queue.size() ) ) <= 0 )
                {
                    if ( ( System.nanoTime() - startNanos ) >= timeoutInNanos )
                    {
                        return i;
                    }

                    monitorWait();
                    capacity = this.capacity;

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

                if ( i == j )
                {
                    return i;
                }
            }
        }
    }

    @Override
    public List<Tuple> pollTuples ( final int count )
    {
        return doPollTuples( count, Long.MAX_VALUE );
    }

    @Override
    public List<Tuple> pollTuples ( final int count, final long timeoutInMillis )
    {
        return doPollTuples( count, timeoutInMillis * 1_000_000 );
    }

    private List<Tuple> doPollTuples ( final int count, final long timeoutInNanos )
    {
        checkArgument( count >= 0 );
        checkArgument( capacity >= count );

        final long startNanos = System.nanoTime();
        synchronized ( monitor )
        {
            while ( queue.size() < count )
            {
                final long elapsed = System.nanoTime() - startNanos;
                if ( elapsed >= timeoutInNanos )
                {
                    return Collections.emptyList();
                }
                monitorWait( ( timeoutInNanos - elapsed ) / 1_000_000 );
            }

            final List<Tuple> tuples = new ArrayList<>( count );
            for ( int i = 0; i < count; i++ )
            {
                tuples.add( queue.poll() );
            }

            monitorNotifyAll();

            return tuples;
        }
    }

    @Override
    public List<Tuple> pollTuplesAtLeast ( final int count )
    {
        return doPollTuplesAtLeast( count, Long.MAX_VALUE );
    }

    @Override
    public List<Tuple> pollTuplesAtLeast ( final int count, final long timeoutInMillis )
    {
        return doPollTuplesAtLeast( count, timeoutInMillis * 1_000_000 );
    }

    private List<Tuple> doPollTuplesAtLeast ( final int count, final long timeoutInNanos )
    {
        checkArgument( count >= 0 );
        checkArgument( capacity >= count );

        final long startNanos = System.nanoTime();
        synchronized ( monitor )
        {
            while ( queue.size() < count )
            {
                final long elapsed = System.nanoTime() - startNanos;
                if ( elapsed >= timeoutInNanos )
                {
                    return Collections.emptyList();
                }
                monitorWait( ( timeoutInNanos - elapsed ) / 1_000_000 );
            }

            final List<Tuple> tuples = new ArrayList<>( count );
            final Iterator<Tuple> it = queue.iterator();
            while ( it.hasNext() )
            {
                tuples.add( it.next() );
                it.remove();
            }

            monitorNotifyAll();

            return tuples;
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
        final long startNanos = System.nanoTime();
        synchronized ( monitor )
        {
            while ( queue.size() < expectedSize )
            {
                final long elapsed = System.nanoTime() - startNanos;
                if ( elapsed >= timeoutInNanos )
                {
                    return false;
                }
                monitorWait( ( timeoutInNanos - elapsed ) / 1_000_000 );
            }
        }

        return true;
    }

    @Override
    public int size ()
    {
        synchronized ( monitor )
        {
            return queue.size();
        }
    }

    @Override
    public boolean isEmpty ()
    {
        synchronized ( monitor )
        {
            return queue.isEmpty();
        }
    }

    @Override
    public boolean isNonEmpty ()
    {
        synchronized ( monitor )
        {
            return !queue.isEmpty();
        }
    }

    @Override
    public void clear ()
    {
        synchronized ( monitor )
        {
            queue.clear();

            monitorNotifyAll();
        }
    }

    private void monitorWait ()
    {
        monitorWait( DEFAULT_WAIT_TIME_IN_MILLIS );
    }

    private void monitorWait ( final long durationInMillis )
    {
        try
        {
            monitor.wait( durationInMillis > 0 ? durationInMillis : 1 );
        }
        catch ( InterruptedException e )
        {
            LOGGER.warn( "Thread {} interrupted while waiting in queue", Thread.currentThread().getName() );
            Thread.currentThread().interrupt();
        }
    }

    private void monitorNotifyAll ()
    {
        monitor.notifyAll();
    }

}
