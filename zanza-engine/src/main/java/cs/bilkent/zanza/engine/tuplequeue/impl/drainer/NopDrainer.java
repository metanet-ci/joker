package cs.bilkent.zanza.engine.tuplequeue.impl.drainer;

import javax.annotation.Nullable;

import cs.bilkent.zanza.engine.tuplequeue.TupleQueue;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueueDrainer;
import cs.bilkent.zanza.operator.impl.TuplesImpl;

public class NopDrainer implements TupleQueueDrainer
{
    @Override
    public void drain ( @Nullable final Object key, final TupleQueue[] tupleQueues )
    {

    }

    @Override
    public TuplesImpl getResult ()
    {
        return null;
    }

    @Override
    public Object getKey ()
    {
        return null;
    }

    @Override
    public void reset ()
    {

    }

}
