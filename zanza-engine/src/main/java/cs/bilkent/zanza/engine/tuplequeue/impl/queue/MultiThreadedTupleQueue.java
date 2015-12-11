package cs.bilkent.zanza.engine.tuplequeue.impl.queue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import cs.bilkent.zanza.engine.tuplequeue.TupleQueue;
import cs.bilkent.zanza.operator.Tuple;
import static java.lang.Math.max;
import uk.co.real_logic.agrona.concurrent.ManyToManyConcurrentArrayQueue;

public class MultiThreadedTupleQueue implements TupleQueue
{

    private static final int RESIZING_COUNTER_VALUE = -1;


    // Used for mutual-exclusion during resizing
    private final AtomicInteger count = new AtomicInteger( 0 );

    private ManyToManyConcurrentArrayQueue<Tuple> queue;

    public MultiThreadedTupleQueue ( final int capacity )
    {
        // ManyToManyConcurrentArrayQueue doesn't support capacity = 1
        this.queue = new ManyToManyConcurrentArrayQueue<>( max( 2, capacity ) );
    }

    @Override
    public void offerTuple ( final Tuple tuple )
    {
        boolean offering = true;
        while ( true )
        {
            final int val = count.get();

            if ( offering )
            {
                if ( val != RESIZING_COUNTER_VALUE && count.compareAndSet( val, val + 1 ) )
                {
                    if ( queue.offer( tuple ) )
                    {
                        count.decrementAndGet();
                        break;
                    }
                    else if ( count.compareAndSet( 1, RESIZING_COUNTER_VALUE ) )
                    {
                        resize();
                        queue.offer( tuple );
                        count.set( 0 );
                        break;
                    }
                    else
                    {
                        offering = false;
                        count.decrementAndGet();
                    }
                }
            }
            else if ( count.compareAndSet( 0, RESIZING_COUNTER_VALUE ) )
            {
                resize();
                queue.offer( tuple );
                count.set( 0 );
                break;
            }
            else
            {
                offering = true;
            }
        }
    }

    @Override
    public List<Tuple> pollTuples ( final int count )
    {
        int val;
        do
        {
            val = this.count.get();
        } while ( val == RESIZING_COUNTER_VALUE || !this.count.compareAndSet( val, val + 1 ) );

        List<Tuple> tuples = Collections.emptyList();
        if ( size() >= count )
        {
            tuples = new ArrayList<>( count );
            queue.drainTo( tuples, count );
        }

        this.count.decrementAndGet();

        return tuples;
    }

    @Override
    public List<Tuple> pollTuplesAtLeast ( final int count )
    {
        int val;
        do
        {
            val = this.count.get();
        } while ( val == RESIZING_COUNTER_VALUE || !this.count.compareAndSet( val, val + 1 ) );

        List<Tuple> tuples = Collections.emptyList();
        final int size = size();
        if ( size >= count )
        {
            tuples = new ArrayList<>( size );
            queue.drainTo( tuples, size );
        }

        this.count.decrementAndGet();

        return tuples;
    }

    @Override
    public int size ()
    {
        return queue.size();
    }

    @Override
    public void clear ()
    {
        int val;
        do
        {
            val = count.get();
        } while ( val == RESIZING_COUNTER_VALUE || !count.compareAndSet( val, val + 1 ) );

        queue.clear();

        count.decrementAndGet();
    }

    private void resize ()
    {
        final int newCapacity = (int) ( Math.ceil( 1.5 * this.queue.capacity() ) );
        final ManyToManyConcurrentArrayQueue<Tuple> queue = new ManyToManyConcurrentArrayQueue<>( newCapacity );
        this.queue.drain( queue::offer );
        this.queue = queue;
    }

}
