package cs.bilkent.joker.engine.pipeline.impl.downstreamtuplesender;

import java.util.concurrent.Future;
import java.util.function.Supplier;

import cs.bilkent.joker.engine.pipeline.DownstreamTupleSender;
import cs.bilkent.joker.engine.pipeline.DownstreamTupleSenderFailureFlag;
import cs.bilkent.joker.engine.tuplequeue.OperatorTupleQueue;
import cs.bilkent.joker.operator.impl.TuplesImpl;

public class DownstreamTupleSender1 extends AbstractDownstreamTupleSender implements DownstreamTupleSender, Supplier<OperatorTupleQueue>
{

    private final int sourcePortIndex;

    private final int destinationPortIndex;

    private final OperatorTupleQueue operatorTupleQueue;

    public DownstreamTupleSender1 ( final DownstreamTupleSenderFailureFlag failureFlag,
                                    final int sourcePortIndex,
                                    final int destinationPortIndex,
                                    final OperatorTupleQueue operatorTupleQueue )
    {
        super( failureFlag );
        this.sourcePortIndex = sourcePortIndex;
        this.destinationPortIndex = destinationPortIndex;
        this.operatorTupleQueue = operatorTupleQueue;
    }

    @Override
    public Future<Void> send ( final TuplesImpl tuples )
    {
        send( operatorTupleQueue, destinationPortIndex, tuples.getTuplesModifiable( sourcePortIndex ) );
        return null;
    }

    @Override
    public OperatorTupleQueue get ()
    {
        return operatorTupleQueue;
    }

}
