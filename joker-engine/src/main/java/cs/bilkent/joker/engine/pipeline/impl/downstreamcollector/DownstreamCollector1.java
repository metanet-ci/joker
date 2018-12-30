package cs.bilkent.joker.engine.pipeline.impl.downstreamcollector;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import javax.inject.Named;

import static cs.bilkent.joker.JokerModule.DOWNSTREAM_FAILURE_FLAG_NAME;
import cs.bilkent.joker.engine.exception.JokerException;
import cs.bilkent.joker.engine.metric.PipelineReplicaMeter.Ticker;
import cs.bilkent.joker.engine.pipeline.DownstreamCollector;
import cs.bilkent.joker.engine.tuplequeue.OperatorQueue;
import cs.bilkent.joker.engine.util.concurrent.BackoffIdleStrategy;
import cs.bilkent.joker.engine.util.concurrent.IdleStrategy;
import cs.bilkent.joker.operator.Tuple;
import cs.bilkent.joker.operator.impl.TuplesImpl;

public class DownstreamCollector1 implements DownstreamCollector, Supplier<OperatorQueue>
{

    private final IdleStrategy idleStrategy = BackoffIdleStrategy.newDefaultInstance();

    private final AtomicBoolean failureFlag;

    private final int sourcePortIndex;

    private final int destinationPortIndex;

    private final OperatorQueue operatorQueue;

    private final Ticker ticker;

    public DownstreamCollector1 ( @Named( DOWNSTREAM_FAILURE_FLAG_NAME ) final AtomicBoolean failureFlag,
                                  final int sourcePortIndex,
                                  final int destinationPortIndex, final OperatorQueue operatorQueue, final Ticker ticker )
    {
        this.failureFlag = failureFlag;
        this.sourcePortIndex = sourcePortIndex;
        this.destinationPortIndex = destinationPortIndex;
        this.operatorQueue = operatorQueue;
        this.ticker = ticker;
    }

    @Override
    public void accept ( final TuplesImpl input )
    {
        final List<Tuple> tuples = input.getTuplesModifiable( sourcePortIndex );
        final int size = tuples.size();
        if ( size == 0 )
        {
            return;
        }

        idleStrategy.reset();
        int fromIndex = 0;
        boolean recordQueueOfferTime = ticker.tryTick();

        while ( true )
        {
            if ( recordQueueOfferTime )
            {
                tuples.get( 0 ).setQueueOfferTime( System.nanoTime() );
                recordQueueOfferTime = false;
            }

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
                    if ( failureFlag.get() )
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
