package cs.bilkent.joker.engine.pipeline.impl.downstreamcollector;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import javax.inject.Named;

import static cs.bilkent.joker.JokerModule.DOWNSTREAM_FAILURE_FLAG_NAME;
import cs.bilkent.joker.engine.exception.JokerException;
import cs.bilkent.joker.engine.pipeline.DownstreamCollector;
import static cs.bilkent.joker.engine.pipeline.impl.downstreamcollector.TupleLatencyUtils.setQueueOfferTime;
import cs.bilkent.joker.engine.tuplequeue.OperatorQueue;
import cs.bilkent.joker.engine.util.concurrent.BackoffIdleStrategy;
import cs.bilkent.joker.engine.util.concurrent.IdleStrategy;
import cs.bilkent.joker.operator.Tuple;
import cs.bilkent.joker.operator.impl.TuplesImpl;

public class DownstreamCollector1 implements DownstreamCollector, Supplier<OperatorQueue>
{

    private final IdleStrategy idleStrategy = BackoffIdleStrategy.newDefaultInstance();

    private final LazyNanoTimeSupplier nanoTimeSupplier = new LazyNanoTimeSupplier();

    private final AtomicBoolean failureFlag;

    private final int sourcePortIndex;

    private final int destinationPortIndex;

    private final OperatorQueue operatorQueue;

    public DownstreamCollector1 ( @Named( DOWNSTREAM_FAILURE_FLAG_NAME ) final AtomicBoolean failureFlag,
                                  final int sourcePortIndex,
                                  final int destinationPortIndex,
                                  final OperatorQueue operatorQueue )
    {
        this.failureFlag = failureFlag;
        this.sourcePortIndex = sourcePortIndex;
        this.destinationPortIndex = destinationPortIndex;
        this.operatorQueue = operatorQueue;
    }

    @Override
    public void accept ( final TuplesImpl input )
    {
        idleStrategy.reset();
        nanoTimeSupplier.reset();

        final List<Tuple> tuples = input.getTuplesModifiable( sourcePortIndex );
        final int size = tuples.size();
        int fromIndex = 0;
        while ( true )
        {
            setQueueOfferTime( tuples, fromIndex, nanoTimeSupplier );
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
