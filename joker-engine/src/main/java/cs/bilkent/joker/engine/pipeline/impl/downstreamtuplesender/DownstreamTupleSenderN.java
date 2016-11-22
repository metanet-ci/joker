package cs.bilkent.joker.engine.pipeline.impl.downstreamtuplesender;

import java.util.concurrent.Future;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import cs.bilkent.joker.engine.pipeline.DownstreamTupleSender;
import cs.bilkent.joker.engine.pipeline.DownstreamTupleSenderFailureFlag;
import cs.bilkent.joker.engine.tuplequeue.OperatorTupleQueue;
import cs.bilkent.joker.operator.impl.TuplesImpl;

public class DownstreamTupleSenderN extends AbstractDownstreamTupleSender implements DownstreamTupleSender, Supplier<OperatorTupleQueue>
{

    private final int[] ports;

    private final int limit;

    private final OperatorTupleQueue operatorTupleQueue;

    public DownstreamTupleSenderN ( final DownstreamTupleSenderFailureFlag failureFlag,
                                    final int[] sourcePorts,
                                    final int[] destinationPorts,
                                    final OperatorTupleQueue operatorTupleQueue )
    {
        super( failureFlag );
        checkArgument( sourcePorts.length == destinationPorts.length,
                       "source ports size = %s and destination ports = %s ! operatorId=%s",
                       sourcePorts.length,
                       destinationPorts.length,
                       operatorTupleQueue.getOperatorId() );
        final int portCount = sourcePorts.length;
        this.ports = new int[ portCount * 2 ];
        this.limit = this.ports.length - 1;
        for ( int i = 0; i < portCount; i++ )
        {
            ports[ i * 2 ] = sourcePorts[ i ];
            ports[ i * 2 + 1 ] = destinationPorts[ i ];
        }
        this.operatorTupleQueue = operatorTupleQueue;
    }

    @Override
    public Future<Void> send ( final TuplesImpl tuples )
    {
        for ( int i = 0; i < limit; i += 2 )
        {
            send( operatorTupleQueue, ports[ i + 1 ], tuples.getTuplesModifiable( ports[ i ] ) );
        }

        return null;
    }

    @Override
    public OperatorTupleQueue get ()
    {
        return operatorTupleQueue;
    }

}
