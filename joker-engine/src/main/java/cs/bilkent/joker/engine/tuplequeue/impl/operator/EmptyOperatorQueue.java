package cs.bilkent.joker.engine.tuplequeue.impl.operator;

import java.util.List;
import java.util.function.Function;

import cs.bilkent.joker.engine.tuplequeue.OperatorQueue;
import cs.bilkent.joker.engine.tuplequeue.TupleQueue;
import cs.bilkent.joker.engine.tuplequeue.TupleQueueDrainer;
import cs.bilkent.joker.engine.tuplequeue.impl.queue.SingleThreadedTupleQueue;
import cs.bilkent.joker.operator.Tuple;
import cs.bilkent.joker.operator.impl.TuplesImpl;
import cs.bilkent.joker.operator.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByPort;
import cs.bilkent.joker.partition.impl.PartitionKey;

public class EmptyOperatorQueue implements OperatorQueue
{

    private final String operatorId;

    private final TupleQueue[] tupleQueues;

    public EmptyOperatorQueue ( final String operatorId, final int inputPortCount )
    {
        this.operatorId = operatorId;
        this.tupleQueues = new TupleQueue[ inputPortCount ];
        for ( int portIndex = 0; portIndex < inputPortCount; portIndex++ )
        {
            this.tupleQueues[ portIndex ] = new SingleThreadedTupleQueue( 1 );
        }
    }

    @Override
    public String getOperatorId ()
    {
        return operatorId;
    }

    @Override
    public int getInputPortCount ()
    {
        return tupleQueues.length;
    }

    @Override
    public int offer ( final int portIndex, final List<Tuple> tuples )
    {
        if ( tuples == null || tuples.isEmpty() )
        {
            return 0;
        }

        throw new UnsupportedOperationException( operatorId );
    }

    @Override
    public int offer ( final int portIndex, final List<Tuple> tuples, final int fromIndex )
    {
        if ( tuples == null || tuples.isEmpty() || fromIndex == ( tuples.size() - 1 ) )
        {
            return 0;
        }

        throw new UnsupportedOperationException( operatorId );
    }

    @Override
    public void drain ( final TupleQueueDrainer drainer,
                        final Function<PartitionKey, TuplesImpl> tuplesSupplier )
    {
        drainer.drain( null, tupleQueues, tuplesSupplier );
    }

    @Override
    public void clear ()
    {

    }

    @Override
    public void setTupleCounts ( final int[] tupleCounts, final TupleAvailabilityByPort tupleAvailabilityByPort )
    {

    }

    @Override
    public boolean isEmpty ()
    {
        return true;
    }

    @Override
    public void ensureCapacity ( final int capacity )
    {

    }

}
