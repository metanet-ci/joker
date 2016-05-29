package cs.bilkent.zanza.engine.tuplequeue.impl.drainer;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueue;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueueDrainer;
import cs.bilkent.zanza.operator.Tuple;
import cs.bilkent.zanza.operator.impl.TuplesImpl;

public class GreedyDrainer implements TupleQueueDrainer
{

    private final int inputPortCount;


    private final TuplesImpl buffer;

    private TuplesImpl result;

    private Object key;

    public GreedyDrainer ( final int inputPortCount )
    {
        this.inputPortCount = inputPortCount;
        this.buffer = new TuplesImpl( inputPortCount );
    }

    @Override
    public void drain ( final Object key, final TupleQueue[] tupleQueues )
    {
        checkArgument( tupleQueues != null );
        checkArgument( tupleQueues.length == inputPortCount );

        for ( int portIndex = 0; portIndex < inputPortCount; portIndex++ )
        {
            final List<Tuple> tuples = buffer.getTuplesModifiable( portIndex );
            tupleQueues[ portIndex ].pollTuplesAtLeast( 0, tuples );
        }

        this.result = buffer;
        this.key = key;
    }

    @Override
    public TuplesImpl getResult ()
    {
        return result;
    }

    @Override
    public Object getKey ()
    {
        return key;
    }

    @Override
    public void reset ()
    {
        buffer.clear();
        result = null;
        key = null;
    }

}
