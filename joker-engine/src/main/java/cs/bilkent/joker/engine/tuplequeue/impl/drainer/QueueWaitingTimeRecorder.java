package cs.bilkent.joker.engine.tuplequeue.impl.drainer;

import java.util.List;
import java.util.function.Consumer;

import cs.bilkent.joker.operator.Tuple;

class QueueWaitingTimeRecorder implements Consumer<Tuple>
{

    private final String operatorId;
    private long now;
    private List<Tuple> tuples;

    QueueWaitingTimeRecorder ( final String operatorId )
    {
        this.operatorId = operatorId;
    }

    void setParameters ( final long now, final List<Tuple> tuples )
    {
        this.now = now;
        this.tuples = tuples;
    }

    @Override
    public void accept ( final Tuple tuple )
    {
        tuple.recordQueueWaitingTime( operatorId, now );
        tuples.add( tuple );
    }
}
