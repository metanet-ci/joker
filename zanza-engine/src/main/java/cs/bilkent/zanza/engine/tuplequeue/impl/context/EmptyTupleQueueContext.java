package cs.bilkent.zanza.engine.tuplequeue.impl.context;

import java.util.List;

import cs.bilkent.zanza.engine.tuplequeue.TupleQueue;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueueContext;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueueDrainer;
import cs.bilkent.zanza.engine.tuplequeue.impl.queue.SingleThreadedTupleQueue;
import cs.bilkent.zanza.operator.Tuple;
import cs.bilkent.zanza.operator.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByPort;

public class EmptyTupleQueueContext implements TupleQueueContext
{

    private final String operatorId;

    private final TupleQueue[] tupleQueues;

    public EmptyTupleQueueContext ( final String operatorId, final int inputPortCount )
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
    public void offer ( final int portIndex, final List<Tuple> tuples )
    {
        throw new UnsupportedOperationException( operatorId );
    }

    @Override
    public int tryOffer ( final int portIndex, final List<Tuple> tuples, final long timeoutInMillis )
    {
        throw new UnsupportedOperationException( operatorId );
    }

    @Override
    public void forceOffer ( final int portIndex, final List<Tuple> tuples )
    {
        throw new UnsupportedOperationException( operatorId );
    }

    @Override
    public void drain ( final TupleQueueDrainer drainer )
    {
        drainer.drain( null, tupleQueues );
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
    public void prepareGreedyDraining ()
    {

    }

    @Override
    public void enableCapacityCheck ( final int portIndex )
    {

    }

    @Override
    public void disableCapacityCheck ( final int portIndex )
    {

    }

    @Override
    public boolean isCapacityCheckEnabled ( final int portIndex )
    {
        return false;
    }

}
