package cs.bilkent.zanza.engine.tuplequeue.impl.drainer;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueue;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueueDrainer;
import cs.bilkent.zanza.operator.PortsToTuples;
import cs.bilkent.zanza.operator.PortsToTuplesAccessor;
import cs.bilkent.zanza.operator.Tuple;
import cs.bilkent.zanza.scheduling.ScheduleWhenTuplesAvailable.PortToTupleCount;
import cs.bilkent.zanza.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByCount;
import static cs.bilkent.zanza.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByCount.AT_LEAST_BUT_SAME_ON_ALL_PORTS;
import static cs.bilkent.zanza.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByCount.EXACT;

abstract class MultiPortDrainer implements TupleQueueDrainer
{

    protected final TupleAvailabilityByCount tupleAvailabilityByCount;

    protected final List<PortToTupleCount> tupleCountByPortIndex;

    private PortsToTuples portsToTuples;

    private Object key;

    public MultiPortDrainer ( final TupleAvailabilityByCount tupleAvailabilityByCount, final List<PortToTupleCount> tupleCountByPortIndex )
    {
        this.tupleAvailabilityByCount = tupleAvailabilityByCount;
        this.tupleCountByPortIndex = tupleCountByPortIndex;
    }

    @Override
    public void drain ( final Object key, final TupleQueue[] tupleQueues )
    {
        checkArgument( tupleQueues != null );
        checkArgument( tupleQueues.length > 1 );

        for ( PortToTupleCount p : checkQueueSizes( tupleQueues ) )
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
                    this.key = key;
                }

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

    protected abstract List<PortToTupleCount> checkQueueSizes ( final TupleQueue[] tupleQueues );

}
