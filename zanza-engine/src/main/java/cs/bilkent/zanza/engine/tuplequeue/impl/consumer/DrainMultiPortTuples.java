package cs.bilkent.zanza.engine.tuplequeue.impl.consumer;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueue;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueuesConsumer;
import cs.bilkent.zanza.operator.PortsToTuples;
import cs.bilkent.zanza.operator.PortsToTuplesAccessor;
import cs.bilkent.zanza.operator.Tuple;
import cs.bilkent.zanza.scheduling.ScheduleWhenTuplesAvailable;
import cs.bilkent.zanza.scheduling.ScheduleWhenTuplesAvailable.PortToTupleCount;
import cs.bilkent.zanza.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByCount;
import static cs.bilkent.zanza.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByCount.AT_LEAST_BUT_SAME_ON_ALL_PORTS;
import static cs.bilkent.zanza.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByCount.EXACT;
import cs.bilkent.zanza.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByPort;
import static cs.bilkent.zanza.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByPort.AVAILABLE_ON_ANY_PORT;

abstract class DrainMultiPortTuples implements TupleQueuesConsumer
{

    protected final TupleAvailabilityByCount tupleAvailabilityByCount;

    protected final TupleAvailabilityByPort tupleAvailabilityByPort;

    protected final List<PortToTupleCount> tupleCountByPortIndex;

    protected PortsToTuples portsToTuples;

    public DrainMultiPortTuples ( ScheduleWhenTuplesAvailable strategy )
    {
        checkArgument( !( strategy.getTupleAvailabilityByPort() == AVAILABLE_ON_ANY_PORT
                          && strategy.getTupleAvailabilityByCount() == AT_LEAST_BUT_SAME_ON_ALL_PORTS ) );

        this.tupleAvailabilityByCount = strategy.getTupleAvailabilityByCount();
        this.tupleAvailabilityByPort = strategy.getTupleAvailabilityByPort();
        this.tupleCountByPortIndex = strategy.getTupleCountByPortIndex();
    }

    @Override
    public void accept ( final TupleQueue[] tupleQueues )
    {
        checkArgument( tupleQueues != null );
        checkArgument( tupleQueues.length > 1 );

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
                    if ( portsToTuples == null )
                    {
                        portsToTuples = new PortsToTuples();
                    }

                    PortsToTuplesAccessor.addAll( portsToTuples, portIndex, tuples );
                }
            }
        }
    }

    @Override
    public PortsToTuples getPortsToTuples ()
    {
        return portsToTuples;
    }

    protected abstract boolean checkQueueSizes ( final TupleQueue[] tupleQueues );


}
