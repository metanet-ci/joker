package cs.bilkent.joker.engine.pipeline.impl.downstreamtuplesender;

import java.util.List;

import cs.bilkent.joker.engine.exception.JokerException;
import cs.bilkent.joker.engine.pipeline.DownstreamTupleSender;
import cs.bilkent.joker.engine.pipeline.DownstreamTupleSenderFailureFlag;
import cs.bilkent.joker.engine.tuplequeue.OperatorTupleQueue;
import cs.bilkent.joker.operator.Tuple;

public abstract class AbstractDownstreamTupleSender implements DownstreamTupleSender
{

    private final DownstreamTupleSenderFailureFlag failureFlag;

    public AbstractDownstreamTupleSender ( final DownstreamTupleSenderFailureFlag failureFlag )
    {
        this.failureFlag = failureFlag;
    }

    protected void send ( final OperatorTupleQueue operatorTupleQueue, final int destinationPortIndex, final List<Tuple> tuples )
    {
        final int size = tuples.size();
        int fromIndex = 0;
        while ( true )
        {
            fromIndex += operatorTupleQueue.offer( destinationPortIndex, tuples, fromIndex, OFFER_TIMEOUT, OFFER_TIME_UNIT );
            if ( fromIndex == size )
            {
                break;
            }
            else if ( failureFlag.isFailed() )
            {
                throw new JokerException( "Not sending tuples to downstream since failure flag is set" );
            }
        }

    }

}
