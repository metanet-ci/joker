package cs.bilkent.zanza.engine.tuplequeue.impl.consumer;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueue;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueuesConsumer;
import static cs.bilkent.zanza.flow.Port.DEFAULT_PORT_INDEX;
import cs.bilkent.zanza.operator.PortsToTuples;
import static cs.bilkent.zanza.operator.PortsToTuplesAccessor.PORTS_TO_TUPLES_CONSTRUCTOR;
import cs.bilkent.zanza.operator.Tuple;
import cs.bilkent.zanza.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByCount;
import static cs.bilkent.zanza.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByCount.EXACT;
import uk.co.real_logic.agrona.collections.Int2ObjectHashMap;

public class DrainSinglePortTuples implements TupleQueuesConsumer
{

    private final int tupleCount;

    private final TupleAvailabilityByCount tupleAvailabilityByCount;

    private PortsToTuples portsToTuples;

    public DrainSinglePortTuples ( final int tupleCount, final TupleAvailabilityByCount tupleAvailabilityByCount )
    {
        checkArgument( tupleCount > 0 );
        checkArgument( tupleAvailabilityByCount != null );
        this.tupleCount = tupleCount;
        this.tupleAvailabilityByCount = tupleAvailabilityByCount;
    }

    @Override
    public void accept ( final TupleQueue[] tupleQueues )
    {
        checkArgument( tupleQueues != null );
        checkArgument( tupleQueues.length == 1 );

        Int2ObjectHashMap<List<Tuple>> tuplesByPorts = null;

        final TupleQueue tupleQueue = tupleQueues[ 0 ];
        final List<Tuple> tuples = ( tupleAvailabilityByCount == EXACT )
                                   ? tupleQueue.pollTuples( tupleCount )
                                   : tupleQueue.pollTuplesAtLeast( tupleCount );
        if ( !tuples.isEmpty() )
        {
            tuplesByPorts = new Int2ObjectHashMap<>( tupleQueues.length, .9 );
            tuplesByPorts.put( DEFAULT_PORT_INDEX, tuples );
        }

        portsToTuples = tuplesByPorts != null ? PORTS_TO_TUPLES_CONSTRUCTOR.apply( tuplesByPorts ) : null;
    }

    @Override
    public PortsToTuples getPortsToTuples ()
    {
        return portsToTuples;
    }

}
