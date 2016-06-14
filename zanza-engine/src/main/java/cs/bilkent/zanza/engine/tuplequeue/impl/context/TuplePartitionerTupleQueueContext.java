package cs.bilkent.zanza.engine.tuplequeue.impl.context;

import java.util.List;

import cs.bilkent.zanza.engine.tuplequeue.TupleQueue;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueueContext;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueueDrainer;
import cs.bilkent.zanza.operator.Tuple;

public class TuplePartitionerTupleQueueContext implements TupleQueueContext
{

    private final PartitionedTupleQueueContext internal;


    public TuplePartitionerTupleQueueContext ( final PartitionedTupleQueueContext internal )
    {
        this.internal = internal;
    }

    @Override
    public String getOperatorId ()
    {
        return internal.getOperatorId();
    }

    @Override
    public void offer ( final int portIndex, final List<Tuple> tuples )
    {
        for ( Tuple tuple : tuples )
        {
            final TupleQueue[] tupleQueues = internal.getTupleQueues( tuple );
            tupleQueues[ portIndex ].offerTuple( tuple );
        }
    }

    @Override
    public int tryOffer ( final int portIndex, final List<Tuple> tuples, final long timeoutInMillis )
    {
        throw new UnsupportedOperationException( getOperatorId() + " partitioner" );
    }

    @Override
    public void forceOffer ( final int portIndex, final List<Tuple> tuples )
    {
        throw new UnsupportedOperationException( getOperatorId() + " partitioner" );
    }

    @Override
    public void drain ( final TupleQueueDrainer drainer )
    {
        internal.drain( drainer );
    }

    @Override
    public void ensureCapacity ( final int portIndex, final int capacity )
    {
        throw new UnsupportedOperationException( getOperatorId() + " partitioner" );
    }

    @Override
    public void clear ()
    {
        internal.clear();
    }

}
