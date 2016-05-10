package cs.bilkent.zanza.engine.tuplequeue.impl.context;

import java.util.List;

import cs.bilkent.zanza.engine.tuplequeue.TupleQueue;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueueContext;
import cs.bilkent.zanza.operator.Tuple;

public abstract class AbstractTupleQueueContext implements TupleQueueContext
{

    final String operatorId;

    private final int inputPortCount;

    AbstractTupleQueueContext ( final String operatorId, final int inputPortCount )
    {
        this.operatorId = operatorId;
        this.inputPortCount = inputPortCount;
    }

    protected abstract TupleQueue[] getTupleQueues ( final List<Tuple> tuples );

    @Override
    public void offer ( final int portIndex, final List<Tuple> tuples )
    {
        final TupleQueue[] tupleQueues = getTupleQueues( tuples );

        if ( tupleQueues != null )
        {
            tupleQueues[ portIndex ].offerTuples( tuples );
        }
    }

    @Override
    public int tryOffer ( final int portIndex, final List<Tuple> tuples, final long timeoutInMillis )
    {
        if ( tuples == null )
        {
            return -1;
        }

        final TupleQueue[] tupleQueues = getTupleQueues( tuples );

        if ( tupleQueues != null )
        {
            return tupleQueues[ portIndex ].tryOfferTuples( tuples, timeoutInMillis );
        }

        return -1;
    }

    @Override
    public void forceOffer ( final int portIndex, final List<Tuple> tuples )
    {
        final TupleQueue[] tupleQueues = getTupleQueues( tuples );

        if ( tupleQueues == null )
        {
            return;
        }

        tupleQueues[ portIndex ].forceOfferTuples( tuples );
    }

}
