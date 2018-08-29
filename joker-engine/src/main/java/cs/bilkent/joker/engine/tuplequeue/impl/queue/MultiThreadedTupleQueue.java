package cs.bilkent.joker.engine.tuplequeue.impl.queue;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import javax.annotation.concurrent.ThreadSafe;

import org.jctools.queues.MpscArrayQueue;

import static com.google.common.base.Preconditions.checkArgument;
import cs.bilkent.joker.engine.tuplequeue.TupleQueue;
import cs.bilkent.joker.operator.Tuple;
import static java.lang.Math.min;

@ThreadSafe
public class MultiThreadedTupleQueue implements TupleQueue
{

    private MpscArrayQueue<Tuple> queue;

    public MultiThreadedTupleQueue ( final int initialCapacity )
    {
        checkArgument( initialCapacity > 0 );
        this.queue = new MpscArrayQueue<>( initialCapacity );
    }

    @Override
    public boolean offer ( final Tuple tuple )
    {
        return queue.offer( tuple );
    }

    @Override
    public int offer ( final List<Tuple> tuples )
    {
        return doOfferTuples( tuples, 0 );
    }

    @Override
    public int offer ( final List<Tuple> tuples, final int fromIndex )
    {
        return doOfferTuples( tuples, fromIndex );
    }

    private int doOfferTuples ( final List<Tuple> tuples, final int fromIndex )
    {
        checkArgument( tuples != null, "tuples can't be null" );

        int offered = 0;
        for ( int i = fromIndex, j = tuples.size(); i < j; i++ )
        {
            if ( queue.offer( tuples.get( i ) ) )
            {
                offered++;
            }
            else
            {
                break;
            }
        }

        return offered;
    }

    @Override
    public Tuple poll ()
    {
        return queue.poll();
    }

    @Override
    public List<Tuple> poll ( final int count )
    {
        final List<Tuple> tuples = new ArrayList<>( min( count, size() ) );
        queue.drain( tuples::add, count );
        return tuples;
    }

    @Override
    public int drainTo ( final int count, final List<Tuple> tuples )
    {
        return queue.drain( tuples::add, count );
    }

    @Override
    public int drainTo ( final int limit, final Consumer<Tuple> consumer )
    {
        int polled = 0;
        for ( int i = 0; i < limit; i++ )
        {
            final Tuple item = queue.poll();
            if ( item != null )
            {
                consumer.accept( item );
                polled++;
            }
            else
            {
                break;
            }
        }

        return polled;
    }

    @Override
    public int size ()
    {
        return queue.size();
    }

    @Override
    public void clear ()
    {
        queue.clear();
    }

    // THIS METHOD IS NOT THREAD-SAFE !!!
    @Override
    public boolean ensureCapacity ( final int capacity )
    {
        if ( capacity > queue.capacity() )
        {
            final MpscArrayQueue<Tuple> newQueue = new MpscArrayQueue<>( capacity );
            queue.drain( newQueue::offer );
            this.queue = newQueue;

            return true;
        }

        return false;
    }

}
