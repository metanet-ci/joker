package cs.bilkent.joker.engine.tuplequeue.impl.drainer;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import cs.bilkent.joker.engine.tuplequeue.TupleQueue;
import cs.bilkent.joker.engine.tuplequeue.TupleQueueDrainer;
import static cs.bilkent.joker.flow.Port.DEFAULT_PORT_INDEX;
import cs.bilkent.joker.operator.Tuple;
import cs.bilkent.joker.operator.impl.TuplesImpl;
import cs.bilkent.joker.operator.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByCount;
import static cs.bilkent.joker.operator.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByCount.EXACT;
import static java.lang.Math.max;


public class NonBlockingSinglePortDrainer implements TupleQueueDrainer
{

    private final int maxBatchSize;

    private int tupleCount;

    private TupleAvailabilityByCount tupleAvailabilityByCount;

    private final TuplesImpl buffer = new TuplesImpl( 1 );

    private final List<Tuple> tuples = buffer.getTuplesModifiable( DEFAULT_PORT_INDEX );

    private TuplesImpl result;

    private Object key;

    public NonBlockingSinglePortDrainer ( final int maxBatchSize )
    {
        this.maxBatchSize = maxBatchSize;
    }

    public void setParameters ( final TupleAvailabilityByCount tupleAvailabilityByCount, final int tupleCount )
    {
        checkArgument( tupleAvailabilityByCount != null );
        checkArgument( tupleCount > 0, "invalid tuple count %s", tupleCount );
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
            tupleQueue.pollTuples( tupleCount, buffer.getTuplesModifiable( DEFAULT_PORT_INDEX ) );
        }
        else
        {
            tupleQueue.pollTuplesAtLeast( tupleCount, max( tupleCount, maxBatchSize ), tuples );
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
