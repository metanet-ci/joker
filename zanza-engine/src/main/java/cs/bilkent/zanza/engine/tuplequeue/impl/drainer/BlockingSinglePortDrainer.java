package cs.bilkent.zanza.engine.tuplequeue.impl.drainer;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueue;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueueDrainer;
import static cs.bilkent.zanza.flow.Port.DEFAULT_PORT_INDEX;
import cs.bilkent.zanza.operator.Tuple;
import cs.bilkent.zanza.operator.impl.TuplesImpl;
import cs.bilkent.zanza.operator.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByCount;
import static cs.bilkent.zanza.operator.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByCount.EXACT;

public class BlockingSinglePortDrainer implements TupleQueueDrainer
{

    private final long timeoutInMillis;

    private int tupleCount;

    private TupleAvailabilityByCount tupleAvailabilityByCount;

    private final TuplesImpl buffer = new TuplesImpl( 1 );

    private final List<Tuple> tuples;

    private TuplesImpl result;

    private Object key;

    public BlockingSinglePortDrainer ( final long timeoutInMillis )
    {
        this.timeoutInMillis = timeoutInMillis;
        this.tuples = buffer.getTuplesModifiable( DEFAULT_PORT_INDEX );
    }

    public void setParameters ( final TupleAvailabilityByCount tupleAvailabilityByCount, final int tupleCount )
    {
        checkArgument( tupleCount > 0 );
        checkArgument( tupleAvailabilityByCount != null );
        this.tupleCount = tupleCount;
        this.tupleAvailabilityByCount = tupleAvailabilityByCount;
    }

    @Override
    public void drain ( final Object key, final TupleQueue[] tupleQueues )
    {
        checkArgument( tupleQueues != null );
        checkArgument( tupleQueues.length == 1 );

        final TupleQueue tupleQueue = tupleQueues[ 0 ];
        if ( tupleAvailabilityByCount == EXACT )
        {
            tupleQueue.pollTuples( tupleCount, timeoutInMillis, tuples );
        }
        else
        {
            tupleQueue.pollTuplesAtLeast( tupleCount, timeoutInMillis, tuples );
        }

        if ( !tuples.isEmpty() )
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
        tuples.clear();
        result = null;
        key = null;
    }

}
