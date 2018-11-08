package cs.bilkent.joker.engine.pipeline.impl.downstreamcollector;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import javax.inject.Named;

import static com.google.common.base.Preconditions.checkArgument;
import static cs.bilkent.joker.JokerModule.DOWNSTREAM_FAILURE_FLAG_NAME;
import cs.bilkent.joker.engine.exception.JokerException;
import cs.bilkent.joker.engine.pipeline.DownstreamCollector;
import static cs.bilkent.joker.engine.pipeline.impl.downstreamcollector.TupleLatencyUtils.setQueueOfferTime;
import cs.bilkent.joker.engine.tuplequeue.OperatorQueue;
import cs.bilkent.joker.engine.util.concurrent.BackoffIdleStrategy;
import cs.bilkent.joker.engine.util.concurrent.IdleStrategy;
import cs.bilkent.joker.operator.Tuple;
import cs.bilkent.joker.operator.impl.TuplesImpl;
import static java.util.Arrays.fill;

public class DownstreamCollectorN implements DownstreamCollector, Supplier<OperatorQueue>
{

    private final IdleStrategy idleStrategy = BackoffIdleStrategy.newDefaultInstance();

    private final LazyNanoTimeSupplier nanoTimeSupplier = new LazyNanoTimeSupplier();

    private final AtomicBoolean failureFlag;

    private final int portCount;

    private final int[] ports;

    private final int[] fromIndices;

    private final int limit;

    private final OperatorQueue operatorQueue;

    public DownstreamCollectorN ( @Named( DOWNSTREAM_FAILURE_FLAG_NAME ) final AtomicBoolean failureFlag,
                                  final int[] sourcePorts,
                                  final int[] destinationPorts,
                                  final OperatorQueue operatorQueue )
    {
        this.failureFlag = failureFlag;
        checkArgument( sourcePorts.length == destinationPorts.length,
                       "source ports size = %s and destination ports = %s ! operatorId=%s",
                       sourcePorts.length,
                       destinationPorts.length,
                       operatorQueue.getOperatorId() );
        this.portCount = sourcePorts.length;
        this.ports = new int[ portCount * 2 ];
        this.fromIndices = new int[ portCount * 2 ];
        this.limit = this.ports.length - 1;
        for ( int i = 0; i < portCount; i++ )
        {
            ports[ i * 2 ] = sourcePorts[ i ];
            ports[ i * 2 + 1 ] = destinationPorts[ i ];
        }
        this.operatorQueue = operatorQueue;
    }

    @Override
    public void accept ( final TuplesImpl input )
    {
        fill( fromIndices, 0 );
        idleStrategy.reset();
        nanoTimeSupplier.reset();
        int done = 0;

        while ( true )
        {
            boolean idle = true;
            for ( int i = 0; i < limit; i += 2 )
            {
                final int sourcePortIndex = ports[ i ];
                final int destinationPortIndex = ports[ i + 1 ];
                int fromIndex = fromIndices[ sourcePortIndex ];
                final List<Tuple> tuples = input.getTuplesModifiable( sourcePortIndex );

                if ( fromIndex < tuples.size() )
                {
                    setQueueOfferTime( tuples, fromIndex, nanoTimeSupplier );
                    final int offered = operatorQueue.offer( destinationPortIndex, tuples, fromIndex );
                    fromIndex += offered;
                    fromIndices[ sourcePortIndex ] = fromIndex;
                    if ( fromIndex == tuples.size() )
                    {
                        done++;
                    }
                    if ( offered > 0 )
                    {
                        idle = false;
                    }
                }
            }

            if ( done == portCount )
            {
                break;
            }
            else if ( idle )
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
