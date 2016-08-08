package cs.bilkent.zanza.engine.pipeline.impl.downstreamtuplesender;

import java.util.concurrent.Future;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import cs.bilkent.zanza.engine.pipeline.DownstreamTupleSender;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueueContext;
import cs.bilkent.zanza.operator.impl.TuplesImpl;

public class DownstreamTupleSenderN implements DownstreamTupleSender, Supplier<TupleQueueContext>
{

    private final int[] ports;

    private final int limit;

    private final TupleQueueContext tupleQueueContext;

    public DownstreamTupleSenderN ( final int[] sourcePorts, final int[] destinationPorts, final TupleQueueContext tupleQueueContext )
    {
        checkArgument( sourcePorts.length == destinationPorts.length,
                       "source ports size = %s and destination ports = %s ! operatorId=%s",
                       sourcePorts.length,
                       destinationPorts.length,
                       tupleQueueContext.getOperatorId() );
        final int portCount = sourcePorts.length;
        this.ports = new int[ portCount * 2 ];
        this.limit = this.ports.length - 1;
        for ( int i = 0; i < portCount; i++ )
        {
            ports[ i * 2 ] = sourcePorts[ i ];
            ports[ i * 2 + 1 ] = destinationPorts[ i ];
        }
        this.tupleQueueContext = tupleQueueContext;
    }

    @Override
    public Future<Void> send ( final TuplesImpl tuples )
    {
        for ( int i = 0; i < limit; i += 2 )
        {
            tupleQueueContext.offer( ports[ i + 1 ], tuples.getTuples( ports[ i ] ) );
        }

        return null;
    }

    @Override
    public TupleQueueContext get ()
    {
        return tupleQueueContext;
    }

}
