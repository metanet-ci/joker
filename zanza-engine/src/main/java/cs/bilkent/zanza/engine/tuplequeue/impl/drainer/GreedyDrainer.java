package cs.bilkent.zanza.engine.tuplequeue.impl.drainer;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueue;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueueDrainer;
import cs.bilkent.zanza.operator.PortsToTuples;
import cs.bilkent.zanza.operator.PortsToTuplesAccessor;
import cs.bilkent.zanza.operator.Tuple;


public class GreedyDrainer implements TupleQueueDrainer
{

    private PortsToTuples portsToTuples = new PortsToTuples();

    private Object key;

    @Override
    public void drain ( final Object key, final TupleQueue[] tupleQueues )
    {
        checkArgument( tupleQueues != null );
        checkArgument( tupleQueues.length > 0 );

        for ( int portIndex = 0; portIndex < tupleQueues.length; portIndex++ )
        {
            final List<Tuple> tuples = tupleQueues[ portIndex ].pollTuplesAtLeast( 0 );
            if ( !tuples.isEmpty() )
            {
                this.key = key;
                PortsToTuplesAccessor.addAll( portsToTuples, portIndex, tuples );
            }
        }
    }

    @Override
    public PortsToTuples getResult ()
    {
        return portsToTuples;
    }

    @Override
    public Object getKey ()
    {
        return key;
    }

}
