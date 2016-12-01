package cs.bilkent.joker.engine.tuplequeue.impl.drainer;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import cs.bilkent.joker.engine.partition.PartitionKey;
import cs.bilkent.joker.engine.tuplequeue.TupleQueue;
import cs.bilkent.joker.engine.tuplequeue.TupleQueueDrainer;
import cs.bilkent.joker.operator.Tuple;
import cs.bilkent.joker.operator.impl.TuplesImpl;

public class GreedyDrainer implements TupleQueueDrainer
{

    private final int inputPortCount;


    private final TuplesImpl buffer;

    private TuplesImpl result;

    private PartitionKey key;

    public GreedyDrainer ( final int inputPortCount )
    {
        this.inputPortCount = inputPortCount;
        this.buffer = new TuplesImpl( inputPortCount );
    }

    @Override
    public void drain ( final boolean maySkipBlocking, final PartitionKey key, final TupleQueue[] tupleQueues )
    {
        checkArgument( tupleQueues != null );
        checkArgument( tupleQueues.length == inputPortCount );

        for ( int portIndex = 0; portIndex < inputPortCount; portIndex++ )
        {
            final List<Tuple> tuples = buffer.getTuplesModifiable( portIndex );
            tupleQueues[ portIndex ].poll( Integer.MAX_VALUE, tuples );
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
    public PartitionKey getKey ()
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
