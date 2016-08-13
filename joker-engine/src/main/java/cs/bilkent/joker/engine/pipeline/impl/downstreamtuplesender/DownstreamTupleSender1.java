package cs.bilkent.joker.engine.pipeline.impl.downstreamtuplesender;

import java.util.concurrent.Future;
import java.util.function.Supplier;

import cs.bilkent.joker.engine.pipeline.DownstreamTupleSender;
import cs.bilkent.joker.engine.tuplequeue.TupleQueueContext;
import cs.bilkent.joker.operator.impl.TuplesImpl;

public class DownstreamTupleSender1 implements DownstreamTupleSender, Supplier<TupleQueueContext>
{

    private final int sourcePortIndex;

    private final int destinationPortIndex;

    private final TupleQueueContext tupleQueueContext;

    public DownstreamTupleSender1 ( final int sourcePortIndex, final int destinationPortIndex, final TupleQueueContext tupleQueueContext )
    {
        this.sourcePortIndex = sourcePortIndex;
        this.destinationPortIndex = destinationPortIndex;
        this.tupleQueueContext = tupleQueueContext;
    }

    @Override
    public Future<Void> send ( final TuplesImpl tuples )
    {
        tupleQueueContext.offer( destinationPortIndex, tuples.getTuples( sourcePortIndex ) );
        return null;
    }

    @Override
    public TupleQueueContext get ()
    {
        return tupleQueueContext;
    }

}
