package cs.bilkent.zanza.engine.tuplequeue.impl.consumer;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueue;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueuesConsumer;
import cs.bilkent.zanza.operator.PortsToTuples;
import static cs.bilkent.zanza.operator.PortsToTuplesAccessor.PORTS_TO_TUPLES_CONSTRUCTOR;
import cs.bilkent.zanza.operator.Tuple;
import uk.co.real_logic.agrona.collections.Int2ObjectHashMap;

public class DrainAllAvailableTuples implements TupleQueuesConsumer
{

    private PortsToTuples portsToTuples;

    @Override
    public void accept ( final TupleQueue[] tupleQueues )
    {
        checkArgument( tupleQueues != null );
        checkArgument( tupleQueues.length > 0 );

        Int2ObjectHashMap<List<Tuple>> tuplesByPorts = null;

        for ( int portIndex = 0; portIndex < tupleQueues.length; portIndex++ )
        {
            final List<Tuple> tuples = tupleQueues[ portIndex ].pollTuplesAtLeast( 1 );
            if ( !tuples.isEmpty() )
            {
                if ( tuplesByPorts == null )
                {
                    tuplesByPorts = new Int2ObjectHashMap<>( tupleQueues.length, .9 );
                }

                tuplesByPorts.put( portIndex, tuples );
            }
        }

        portsToTuples = tuplesByPorts != null ? PORTS_TO_TUPLES_CONSTRUCTOR.apply( tuplesByPorts ) : null;
    }

    @Override
    public PortsToTuples getPortsToTuples ()
    {
        return portsToTuples;
    }

}
