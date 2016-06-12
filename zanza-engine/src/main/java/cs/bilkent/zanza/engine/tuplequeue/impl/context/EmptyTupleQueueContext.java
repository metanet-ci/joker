package cs.bilkent.zanza.engine.tuplequeue.impl.context;

import java.util.List;

import cs.bilkent.zanza.engine.tuplequeue.TupleQueue;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueueContext;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueueDrainer;
import cs.bilkent.zanza.engine.tuplequeue.impl.queue.SingleThreadedTupleQueue;
import cs.bilkent.zanza.operator.Tuple;

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
    public void ensureCapacity ( final int portIndex, final int capacity )
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

    @Override
    public boolean isCapacityCheckDisabled ( final int portIndex )
    {
        return true;
    }

    @Override
    public void clear ()
    {

    }

}
