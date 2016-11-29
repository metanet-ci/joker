package cs.bilkent.joker.engine.tuplequeue.impl.queue;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import javax.annotation.concurrent.ThreadSafe;

import org.agrona.concurrent.ManyToOneConcurrentArrayQueue;

import static com.google.common.base.Preconditions.checkArgument;
import cs.bilkent.joker.engine.tuplequeue.TupleQueue;
import cs.bilkent.joker.operator.Tuple;
import static java.lang.Math.max;

@ThreadSafe
public class MultiThreadedTupleQueue implements TupleQueue
{

    private final ManyToOneConcurrentArrayQueue<Tuple> queue;

    private final int capacity;

    public MultiThreadedTupleQueue ( final int initialCapacity )
    {
        checkArgument( initialCapacity > 0 );
        this.capacity = initialCapacity;
        this.queue = new ManyToOneConcurrentArrayQueue<>( initialCapacity );
    }

    public MultiThreadedTupleQueue ( final int initialCapacity, ArrayDeque<Tuple> queue )
    {
        checkArgument( initialCapacity > 0 );
        this.capacity = initialCapacity;
        this.queue = new ManyToOneConcurrentArrayQueue<>( max( initialCapacity, queue.size() ) );
        for ( Tuple tuple : queue )
        {
            this.queue.offer( tuple );
        }
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
        final List<Tuple> tuples = new ArrayList<>();
        queue.drainTo( tuples, count );
        return tuples;
    }

    @Override
    public int poll ( final int count, final Collection<Tuple> tuples )
    {
        return queue.drainTo( tuples, count );
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

    public SingleThreadedTupleQueue toSingleThreadedTupleQueue ()
    {
        final SingleThreadedTupleQueue queue = new SingleThreadedTupleQueue( capacity );

        Tuple tuple;
        while ( ( tuple = poll() ) != null )
        {
            queue.offer( tuple );
        }

        return queue;
    }

}
