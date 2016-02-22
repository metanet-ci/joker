package cs.bilkent.zanza.engine.tuplequeue.impl.queue;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import javax.annotation.concurrent.NotThreadSafe;

import cs.bilkent.zanza.engine.tuplequeue.TupleQueue;
import cs.bilkent.zanza.operator.Tuple;

@NotThreadSafe
public class UnboundedTupleQueue implements TupleQueue
{

    private final ArrayDeque<Tuple> queue;

    public UnboundedTupleQueue ( final int initialCapacity )
    {
        this.queue = new ArrayDeque<>( initialCapacity );
    }

    @Override
    public void ensureCapacity ( final int capacity )
    {

    }

    @Override
    public void offerTuple ( final Tuple tuple )
    {
        queue.offer( tuple );
    }

    @Override
    public boolean tryOfferTuple ( final Tuple tuple, final long timeoutInMillis )
    {
        offerTuple( tuple );
        return true;
    }

    @Override
    public void offerTuples ( final List<Tuple> tuples )
    {
        for ( Tuple tuple : tuples )
        {
            queue.offer( tuple );
        }
    }

    @Override
    public int tryOfferTuples ( final List<Tuple> tuples, final long timeoutInMillis )
    {
        offerTuples( tuples );
        return tuples.size();
    }

    @Override
    public List<Tuple> pollTuples ( final int count )
    {
        if ( size() >= count )
        {
            final List<Tuple> tuples = new ArrayList<>( count );
            for ( int i = 0; i < count; i++ )
            {
                tuples.add( queue.poll() );
            }

            return tuples;
        }

        return Collections.emptyList();
    }

    @Override
    public List<Tuple> pollTuples ( final int count, final long timeoutInMillis )
    {
        return pollTuples( count );
    }

    @Override
    public List<Tuple> pollTuplesAtLeast ( final int count )
    {
        if ( size() >= count )
        {
            final List<Tuple> tuples = new ArrayList<>( count );
            final Iterator<Tuple> it = queue.iterator();
            while ( it.hasNext() )
            {
                tuples.add( it.next() );
                it.remove();
            }
            queue.clear();

            return tuples;
        }

        return Collections.emptyList();
    }

    @Override
    public List<Tuple> pollTuplesAtLeast ( final int count, final long timeoutInMillis )
    {
        return pollTuplesAtLeast( count );
    }

    @Override
    public boolean awaitMinimumSize ( final int expectedSize )
    {
        return queue.size() >= expectedSize;
    }

    @Override
    public boolean awaitMinimumSize ( final int expectedSize, final long timeoutInMillis )
    {
        return queue.size() >= expectedSize;
    }

    @Override
    public int size ()
    {
        return queue.size();
    }

    @Override
    public boolean isEmpty ()
    {
        return queue.isEmpty();
    }

    @Override
    public boolean isNonEmpty ()
    {
        return !queue.isEmpty();
    }

    @Override
    public void clear ()
    {
        queue.clear();
    }

}
