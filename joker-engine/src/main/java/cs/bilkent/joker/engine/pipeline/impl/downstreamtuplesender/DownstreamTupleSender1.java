package cs.bilkent.joker.engine.pipeline.impl.downstreamtuplesender;

import java.util.List;
import java.util.concurrent.Future;
import java.util.function.Supplier;

import cs.bilkent.joker.engine.exception.JokerException;
import cs.bilkent.joker.engine.pipeline.DownstreamTupleSender;
import cs.bilkent.joker.engine.pipeline.DownstreamTupleSenderFailureFlag;
import cs.bilkent.joker.engine.tuplequeue.OperatorTupleQueue;
import cs.bilkent.joker.engine.util.concurrent.BackoffIdleStrategy;
import cs.bilkent.joker.engine.util.concurrent.IdleStrategy;
import cs.bilkent.joker.operator.Tuple;
import cs.bilkent.joker.operator.impl.TuplesImpl;

public class DownstreamTupleSender1 implements DownstreamTupleSender, Supplier<OperatorTupleQueue>
{

    private final IdleStrategy idleStrategy = BackoffIdleStrategy.newDefaultInstance();

    private final DownstreamTupleSenderFailureFlag failureFlag;

    private final int sourcePortIndex;

    private final int destinationPortIndex;

    private final OperatorTupleQueue operatorTupleQueue;

    public DownstreamTupleSender1 ( final DownstreamTupleSenderFailureFlag failureFlag,
                                    final int sourcePortIndex,
                                    final int destinationPortIndex,
                                    final OperatorTupleQueue operatorTupleQueue )
    {
        this.failureFlag = failureFlag;
        this.sourcePortIndex = sourcePortIndex;
        this.destinationPortIndex = destinationPortIndex;
        this.operatorTupleQueue = operatorTupleQueue;
    }

    @Override
    public Future<Void> send ( final TuplesImpl input )
    {
        idleStrategy.reset();

        final List<Tuple> tuples = input.getTuplesModifiable( sourcePortIndex );
        final int size = tuples.size();
        int fromIndex = 0;
        while ( true )
        {
            final int offered = operatorTupleQueue.offer( destinationPortIndex, tuples, fromIndex );
            fromIndex += offered;
            if ( fromIndex == size )
            {
                break;
            }
            else if ( offered == 0 )
            {
                if ( idleStrategy.idle() )
                {
                    if ( failureFlag.isFailed() )
                    {
                        throw new JokerException( "Not sending tuples to downstream since failure flag is set" );
                    }
                }
            }
        }

        return null;
    }

    @Override
    public OperatorTupleQueue get ()
    {
        return operatorTupleQueue;
    }

}
