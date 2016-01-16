package cs.bilkent.zanza.engine.tuplequeue.impl.consumer;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueue;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueuesConsumer;
import cs.bilkent.zanza.operator.PortsToTuples;
import cs.bilkent.zanza.operator.PortsToTuplesAccessor;
import cs.bilkent.zanza.operator.Tuple;

public class DrainAllAvailableTuples implements TupleQueuesConsumer
{

    private PortsToTuples portsToTuples;

    @Override
    public void accept ( final TupleQueue[] tupleQueues )
    {
        checkArgument( tupleQueues != null );
        checkArgument( tupleQueues.length > 0 );

        for ( int portIndex = 0; portIndex < tupleQueues.length; portIndex++ )
        {
            final List<Tuple> tuples = tupleQueues[ portIndex ].pollTuplesAtLeast( 1 );
            if ( !tuples.isEmpty() )
            {
                if ( portsToTuples == null )
                {
                    portsToTuples = new PortsToTuples();
                }

                PortsToTuplesAccessor.addAll( portsToTuples, portIndex, tuples );
            }
        }
    }

    @Override
    public PortsToTuples getPortsToTuples ()
    {
        return portsToTuples;
    }

}
