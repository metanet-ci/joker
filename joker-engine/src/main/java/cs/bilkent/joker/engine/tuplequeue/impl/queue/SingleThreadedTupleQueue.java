package cs.bilkent.joker.engine.tuplequeue.impl.queue;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import javax.annotation.concurrent.NotThreadSafe;

import static com.google.common.base.Preconditions.checkArgument;
import cs.bilkent.joker.engine.tuplequeue.TupleQueue;
import cs.bilkent.joker.operator.Tuple;

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
        final List<Tuple> tuples = new ArrayList<>();
        doPollTuples( count, tuples );
        return tuples;
    }

    @Override
    public int poll ( final int count, final Collection<Tuple> tuples )
    {
        return doPollTuples( count, tuples );
    }

    private int doPollTuples ( final int count, Collection<Tuple> tuples )
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
