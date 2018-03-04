package cs.bilkent.joker.engine.pipeline.impl.downstreamtuplesender;

import java.util.List;
import java.util.function.Supplier;

import cs.bilkent.joker.engine.exception.JokerException;
import cs.bilkent.joker.engine.pipeline.DownstreamTupleSender;
import cs.bilkent.joker.engine.pipeline.DownstreamTupleSenderFailureFlag;
import cs.bilkent.joker.engine.tuplequeue.OperatorQueue;
import cs.bilkent.joker.engine.util.concurrent.BackoffIdleStrategy;
import cs.bilkent.joker.engine.util.concurrent.IdleStrategy;
import cs.bilkent.joker.operator.Tuple;
import cs.bilkent.joker.operator.impl.TuplesImpl;

public class DownstreamTupleSender1 implements DownstreamTupleSender, Supplier<OperatorQueue>
{

    private final IdleStrategy idleStrategy = BackoffIdleStrategy.newDefaultInstance();

    private final DownstreamTupleSenderFailureFlag failureFlag;

    private final int sourcePortIndex;

    private final int destinationPortIndex;

    private final OperatorQueue operatorQueue;

    public DownstreamTupleSender1 ( final DownstreamTupleSenderFailureFlag failureFlag,
                                    final int sourcePortIndex,
                                    final int destinationPortIndex, final OperatorQueue operatorQueue )
    {
        this.failureFlag = failureFlag;
        this.sourcePortIndex = sourcePortIndex;
        this.destinationPortIndex = destinationPortIndex;
        this.operatorQueue = operatorQueue;
    }

    @Override
    public void send ( final TuplesImpl input )
    {
        idleStrategy.reset();

        final List<Tuple> tuples = input.getTuplesModifiable( sourcePortIndex );
        final int size = tuples.size();
        int fromIndex = 0;
        while ( true )
        {
            final int offered = operatorQueue.offer( destinationPortIndex, tuples, fromIndex );
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
    }

    @Override
    public OperatorQueue get ()
    {
        return operatorQueue;
    }

}
