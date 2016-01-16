package cs.bilkent.zanza.engine.tuplequeue.impl.consumer;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueue;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueuesConsumer;
import cs.bilkent.zanza.operator.PortsToTuples;
import static cs.bilkent.zanza.operator.PortsToTuplesAccessor.PORTS_TO_TUPLES_CONSTRUCTOR;
import cs.bilkent.zanza.operator.Tuple;
import cs.bilkent.zanza.scheduling.ScheduleWhenTuplesAvailable;
import cs.bilkent.zanza.scheduling.ScheduleWhenTuplesAvailable.PortToTupleCount;
import cs.bilkent.zanza.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByCount;
import static cs.bilkent.zanza.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByCount.AT_LEAST_BUT_SAME_ON_ALL_PORTS;
import static cs.bilkent.zanza.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByCount.EXACT;
import cs.bilkent.zanza.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByPort;
import static cs.bilkent.zanza.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByPort.AVAILABLE_ON_ALL_PORTS;
import static cs.bilkent.zanza.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByPort.AVAILABLE_ON_ANY_PORT;
import uk.co.real_logic.agrona.collections.Int2ObjectHashMap;

public class DrainMultiPortTuples implements TupleQueuesConsumer
{


    private final TupleAvailabilityByCount tupleAvailabilityByCount;

    private final TupleAvailabilityByPort tupleAvailabilityByPort;

    private final List<PortToTupleCount> tupleCountByPortIndex;

    private PortsToTuples portsToTuples;

    public DrainMultiPortTuples ( ScheduleWhenTuplesAvailable scheduleWhenTuplesAvailable )
    {
        checkArgument( !( scheduleWhenTuplesAvailable.getTupleAvailabilityByPort() == AVAILABLE_ON_ANY_PORT
                          && scheduleWhenTuplesAvailable.getTupleAvailabilityByCount() == AT_LEAST_BUT_SAME_ON_ALL_PORTS ) );

        this.tupleAvailabilityByCount = scheduleWhenTuplesAvailable.getTupleAvailabilityByCount();
        this.tupleAvailabilityByPort = scheduleWhenTuplesAvailable.getTupleAvailabilityByPort();
        this.tupleCountByPortIndex = scheduleWhenTuplesAvailable.getTupleCountByPortIndex();
    }

    @Override
    public void accept ( final TupleQueue[] tupleQueues )
    {
        checkArgument( tupleQueues != null );
        checkArgument( tupleQueues.length > 1 );

        Int2ObjectHashMap<List<Tuple>> tuplesByPorts = null;

        if ( checkQueueSizes( tupleQueues ) )
        {
            for ( PortToTupleCount p : tupleCountByPortIndex )
            {
                final int portIndex = p.portIndex;
                final int tupleCount = p.tupleCount;

                final TupleQueue tupleQueue = tupleQueues[ portIndex ];
                final boolean pollWithExactCount =
                        tupleAvailabilityByCount == EXACT || tupleAvailabilityByCount == AT_LEAST_BUT_SAME_ON_ALL_PORTS;

                final List<Tuple> tuples = pollWithExactCount
                                           ? tupleQueue.pollTuples( tupleCount )
                                           : tupleQueue.pollTuplesAtLeast( tupleCount );

                if ( !tuples.isEmpty() )
                {
                    if ( tuplesByPorts == null )
                    {
                        tuplesByPorts = new Int2ObjectHashMap<>( tupleQueues.length, .9 );
                    }
                    tuplesByPorts.put( portIndex, tuples );
                }
            }
        }

        portsToTuples = tuplesByPorts != null ? PORTS_TO_TUPLES_CONSTRUCTOR.apply( tuplesByPorts ) : null;
    }

    @Override
    public PortsToTuples getPortsToTuples ()
    {
        return portsToTuples;
    }

    private boolean checkQueueSizes ( final TupleQueue[] tupleQueues )
    {
        if ( tupleAvailabilityByPort == AVAILABLE_ON_ALL_PORTS )
        {
            for ( PortToTupleCount p : tupleCountByPortIndex )
            {
                final int size = tupleQueues[ p.portIndex ].size();
                if ( size < p.tupleCount )
                {
                    return false;
                }
            }
        }

        return true;
    }

}
