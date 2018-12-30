package cs.bilkent.joker.engine.tuplequeue.impl.drainer;

import java.util.List;
import java.util.function.Consumer;

import cs.bilkent.joker.engine.pipeline.impl.downstreamcollector.LazyNanoTimeSupplier;
import cs.bilkent.joker.operator.Tuple;

class QueueWaitingTimeRecorder implements Consumer<Tuple>
{

    private final String operatorId;
    private final LazyNanoTimeSupplier nanoTimeSupplier = new LazyNanoTimeSupplier();
    private List<Tuple> tuples;

    QueueWaitingTimeRecorder ( final String operatorId )
    {
        this.operatorId = operatorId;
    }

    void reset ()
    {
        nanoTimeSupplier.reset();
    }

    void setParameters ( final List<Tuple> tuples )
    {
        this.tuples = tuples;
    }

    @Override
    public void accept ( final Tuple tuple )
    {
        tuple.recordQueueWaitingTime( operatorId, nanoTimeSupplier );
        tuples.add( tuple );
    }
}
