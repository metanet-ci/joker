package cs.bilkent.zanza.engine.tuplequeue.impl.drainer;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueue;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueueDrainer;
import cs.bilkent.zanza.operator.Tuple;
import cs.bilkent.zanza.operator.impl.TuplesImpl;

// TODO We can make this smarter. It can directly poll non-empty queues if there is any
public class GreedyDrainer implements TupleQueueDrainer
{

    private final int inputPortCount;

    private final long timeoutInMillis;

    private final TuplesImpl buffer;

    private TuplesImpl result;

    private Object key;

    public GreedyDrainer ( final int inputPortCount )
    {
        this( inputPortCount, Long.MAX_VALUE );
    }

    public GreedyDrainer ( final int inputPortCount, final long timeoutInMillis )
    {
        this.inputPortCount = inputPortCount;
        this.timeoutInMillis = timeoutInMillis;
        this.buffer = new TuplesImpl( inputPortCount );
    }

    @Override
    public void drain ( final Object key, final TupleQueue[] tupleQueues )
    {
        checkArgument( tupleQueues != null );
        checkArgument( tupleQueues.length == inputPortCount );

        boolean satisfied = false;

        for ( int portIndex = 0; portIndex < inputPortCount; portIndex++ )
        {
            final List<Tuple> tuples = buffer.getTuplesModifiable( portIndex );
            tupleQueues[ portIndex ].pollTuplesAtLeast( 1, timeoutInMillis, tuples );
            satisfied |= !tuples.isEmpty();
        }

        if ( satisfied )
        {
            this.result = buffer;
            this.key = key;
        }
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
