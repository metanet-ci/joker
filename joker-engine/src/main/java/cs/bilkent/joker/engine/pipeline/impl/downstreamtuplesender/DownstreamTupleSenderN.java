package cs.bilkent.joker.engine.pipeline.impl.downstreamtuplesender;

import java.util.List;
import java.util.concurrent.Future;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import cs.bilkent.joker.engine.exception.JokerException;
import cs.bilkent.joker.engine.pipeline.DownstreamTupleSender;
import cs.bilkent.joker.engine.pipeline.DownstreamTupleSenderFailureFlag;
import cs.bilkent.joker.engine.tuplequeue.OperatorTupleQueue;
import cs.bilkent.joker.engine.util.concurrent.BackoffIdleStrategy;
import cs.bilkent.joker.engine.util.concurrent.IdleStrategy;
import cs.bilkent.joker.operator.Tuple;
import cs.bilkent.joker.operator.impl.TuplesImpl;
import static java.util.Arrays.fill;

public class DownstreamTupleSenderN implements DownstreamTupleSender, Supplier<OperatorTupleQueue>
{

    private final IdleStrategy idleStrategy = BackoffIdleStrategy.newDefaultInstance();

    private final DownstreamTupleSenderFailureFlag failureFlag;

    private final int portCount;

    private final int[] ports;

    private final int[] fromIndices;

    private final int limit;

    private final OperatorTupleQueue operatorTupleQueue;

    public DownstreamTupleSenderN ( final DownstreamTupleSenderFailureFlag failureFlag,
                                    final int[] sourcePorts,
                                    final int[] destinationPorts,
                                    final OperatorTupleQueue operatorTupleQueue )
    {
        this.failureFlag = failureFlag;
        checkArgument( sourcePorts.length == destinationPorts.length,
                       "source ports size = %s and destination ports = %s ! operatorId=%s",
                       sourcePorts.length,
                       destinationPorts.length,
                       operatorTupleQueue.getOperatorId() );
        this.portCount = sourcePorts.length;
        this.ports = new int[ portCount * 2 ];
        this.fromIndices = new int[ portCount * 2 ];
        this.limit = this.ports.length - 1;
        for ( int i = 0; i < portCount; i++ )
        {
            ports[ i * 2 ] = sourcePorts[ i ];
            ports[ i * 2 + 1 ] = destinationPorts[ i ];
        }
        this.operatorTupleQueue = operatorTupleQueue;
    }

    @Override
    public Future<Void> send ( final TuplesImpl input )
    {
        reset();
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
                    final int offered = operatorTupleQueue.offer( destinationPortIndex, tuples, fromIndex );
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
                    if ( failureFlag.isFailed() )
                    {
                        throw new JokerException( "Not sending tuples to downstream since failure flag is set" );
                    }
                }
            }
        }

        return null;
    }

    private void reset ()
    {
        fill( fromIndices, 0 );
        idleStrategy.reset();
    }

    @Override
    public OperatorTupleQueue get ()
    {
        return operatorTupleQueue;
    }

}
