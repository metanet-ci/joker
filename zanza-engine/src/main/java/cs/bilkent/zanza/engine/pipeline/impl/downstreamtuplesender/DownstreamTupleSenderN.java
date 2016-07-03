package cs.bilkent.zanza.engine.pipeline.impl.downstreamtuplesender;

import java.util.concurrent.Future;

import static com.google.common.base.Preconditions.checkArgument;
import cs.bilkent.zanza.engine.pipeline.DownstreamTupleSender;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueueContext;
import cs.bilkent.zanza.operator.impl.TuplesImpl;

public class DownstreamTupleSenderN implements DownstreamTupleSender
{

    private final int[][] ports;

    private final int portCount;

    private final TupleQueueContext tupleQueueContext;

    public DownstreamTupleSenderN ( final int[] sourcePorts, final int[] destinationPorts, final TupleQueueContext tupleQueueContext )
    {
        checkArgument( sourcePorts.length == destinationPorts.length );
        this.portCount = sourcePorts.length;
        this.ports = new int[ portCount ][ 2 ];
        for ( int i = 0; i < portCount; i++ )
        {
            ports[ i ][ 0 ] = sourcePorts[ i ];
            ports[ i ][ 1 ] = destinationPorts[ i ];
        }
        this.tupleQueueContext = tupleQueueContext;
    }

    @Override
    public Future<Void> send ( final TuplesImpl tuples )
    {
        for ( int i = 0; i < portCount; i++ )
        {
            tupleQueueContext.offer( ports[ i ][ 1 ], tuples.getTuples( ports[ i ][ 0 ] ) );
        }

        return null;
    }
}
