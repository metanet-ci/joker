package cs.bilkent.joker.engine.tuplequeue.impl.queue;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import javax.annotation.concurrent.NotThreadSafe;

import static com.google.common.base.Preconditions.checkArgument;
import cs.bilkent.joker.engine.tuplequeue.TupleQueue;
import cs.bilkent.joker.operator.Tuple;
import static java.lang.Math.min;

@NotThreadSafe
public class SingleThreadedTupleQueue implements TupleQueue
{

    private final ArrayDeque<Tuple> queue;

    public SingleThreadedTupleQueue ( final int initialCapacity )
    {
        checkArgument( initialCapacity > 0 );
        this.queue = new ArrayDeque<>( initialCapacity );
    }

    @Override
    public boolean offer ( final Tuple tuple )
    {
        queue.offer( tuple );
        return true;
    }

    @Override
    public int offer ( final List<Tuple> tuples )
    {
        queue.addAll( tuples );
        return tuples.size();
    }

    @Override
    public int offer ( final List<Tuple> tuples, final int fromIndex )
    {
        final int j = tuples.size();
        for ( int i = fromIndex; i < j; i++ )
        {
            queue.add( tuples.get( i ) );
        }

        return j - fromIndex;
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
        doPollTuples( count, tuples );
        return tuples;
    }

    @Override
    public int drainTo ( final int count, final List<Tuple> tuples )
    {
        return doPollTuples( count, tuples );
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

    private int doPollTuples ( final int count, List<Tuple> tuples )
    {
        int polled = 0;
        for ( int i = 0; i < count; i++ )
        {
            final Tuple item = queue.poll();
            if ( item != null )
            {
                tuples.add( item );
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

    @Override
    public boolean ensureCapacity ( final int capacity )
    {
        return false;
    }

}
