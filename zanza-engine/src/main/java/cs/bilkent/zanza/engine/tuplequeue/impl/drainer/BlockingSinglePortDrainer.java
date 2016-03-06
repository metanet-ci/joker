package cs.bilkent.zanza.engine.tuplequeue.impl.drainer;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueue;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueueDrainer;
import static cs.bilkent.zanza.flow.Port.DEFAULT_PORT_INDEX;
import cs.bilkent.zanza.operator.PortsToTuples;
import cs.bilkent.zanza.operator.PortsToTuplesAccessor;
import cs.bilkent.zanza.operator.Tuple;
import cs.bilkent.zanza.operator.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByCount;
import static cs.bilkent.zanza.operator.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByCount.EXACT;

public class BlockingSinglePortDrainer implements TupleQueueDrainer
{

    private final int tupleCount;

    private final TupleAvailabilityByCount tupleAvailabilityByCount;

    private final int timeoutInMillis;

    private PortsToTuples portsToTuples;

    private Object key;

    public BlockingSinglePortDrainer ( final int tupleCount,
                                       final TupleAvailabilityByCount tupleAvailabilityByCount,
                                       final int timeoutInMillis )
    {
        checkArgument( tupleCount > 0 );
        checkArgument( tupleAvailabilityByCount != null );
        this.tupleCount = tupleCount;
        this.tupleAvailabilityByCount = tupleAvailabilityByCount;
        this.timeoutInMillis = timeoutInMillis;
    }

    @Override
    public void drain ( final Object key, final TupleQueue[] tupleQueues )
    {
        checkArgument( tupleQueues != null );
        checkArgument( tupleQueues.length == 1 );

        final TupleQueue tupleQueue = tupleQueues[ 0 ];
        final List<Tuple> tuples = ( tupleAvailabilityByCount == EXACT )
                                   ? tupleQueue.pollTuples( tupleCount, timeoutInMillis )
                                   : tupleQueue.pollTuplesAtLeast( tupleCount, timeoutInMillis );
        if ( !tuples.isEmpty() )
        {
            portsToTuples = new PortsToTuples();
            PortsToTuplesAccessor.addAll( portsToTuples, DEFAULT_PORT_INDEX, tuples );
            this.key = key;
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
