package cs.bilkent.zanza.engine.tuplequeue.impl.queue;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;

import cs.bilkent.zanza.engine.tuplequeue.TupleQueue;
import cs.bilkent.zanza.operator.Tuple;


public class SingleThreadedTupleQueue implements TupleQueue
{

    private final Queue<Tuple> queue;

    public SingleThreadedTupleQueue ( final int initialCapacity )
    {
        this.queue = new ArrayDeque<>( initialCapacity );
    }

    @Override
    public void offerTuple ( final Tuple tuple )
    {
        queue.offer( tuple );
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
    public int size ()
    {
        return queue.size();
    }

    @Override
    public void clear ()
    {
        queue.clear();
    }

}
