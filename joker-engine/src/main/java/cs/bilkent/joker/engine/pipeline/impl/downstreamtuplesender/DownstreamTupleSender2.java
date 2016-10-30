package cs.bilkent.joker.engine.pipeline.impl.downstreamtuplesender;

import java.util.concurrent.Future;
import java.util.function.Supplier;

import cs.bilkent.joker.engine.pipeline.DownstreamTupleSender;
import cs.bilkent.joker.engine.tuplequeue.OperatorTupleQueue;
import cs.bilkent.joker.operator.impl.TuplesImpl;

public class DownstreamTupleSender2 implements DownstreamTupleSender, Supplier<OperatorTupleQueue>
{

    private final int sourcePortIndex1;

    private final int destinationPortIndex1;

    private final int sourcePortIndex2;

    private final int destinationPortIndex2;

    private final OperatorTupleQueue operatorTupleQueue;

    public DownstreamTupleSender2 ( final int sourcePortIndex1,
                                    final int destinationPortIndex1,
                                    final int sourcePortIndex2,
                                    final int destinationPortIndex2, final OperatorTupleQueue operatorTupleQueue )
    {
        this.sourcePortIndex1 = sourcePortIndex1;
        this.destinationPortIndex1 = destinationPortIndex1;
        this.sourcePortIndex2 = sourcePortIndex2;
        this.destinationPortIndex2 = destinationPortIndex2;
        this.operatorTupleQueue = operatorTupleQueue;
    }

    @Override
    public Future<Void> send ( final TuplesImpl tuples )
    {
        operatorTupleQueue.offer( destinationPortIndex1, tuples.getTuples( sourcePortIndex1 ) );
        operatorTupleQueue.offer( destinationPortIndex2, tuples.getTuples( sourcePortIndex2 ) );
        return null;
    }

    @Override
    public OperatorTupleQueue get ()
    {
        return operatorTupleQueue;
    }

}
