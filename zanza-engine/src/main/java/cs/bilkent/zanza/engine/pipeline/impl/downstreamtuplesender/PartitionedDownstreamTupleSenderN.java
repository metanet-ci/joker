package cs.bilkent.zanza.engine.pipeline.impl.downstreamtuplesender;

import java.util.concurrent.Future;

import static com.google.common.base.Preconditions.checkArgument;
import cs.bilkent.zanza.engine.partition.PartitionKeyFunction;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueueContext;
import cs.bilkent.zanza.operator.impl.TuplesImpl;

public class PartitionedDownstreamTupleSenderN extends AbstractPartitionedDownstreamTupleSender
{

    private final int[][] ports;

    private final int portCount;

    public PartitionedDownstreamTupleSenderN ( final int[] sourcePorts,
                                               final int[] destinationPorts,
                                               final int partitionCount,
                                               final int[] partitionDistribution,
                                               final TupleQueueContext[] tupleQueueContexts,
                                               final PartitionKeyFunction partitionKeyExtractor )
    {
        super( partitionCount, partitionDistribution, tupleQueueContexts, partitionKeyExtractor );
        checkArgument( sourcePorts.length == destinationPorts.length );
        this.portCount = sourcePorts.length;
        this.ports = new int[ portCount ][ 2 ];
        for ( int i = 0; i < portCount; i++ )
        {
            ports[ i ][ 0 ] = sourcePorts[ i ];
            ports[ i ][ 1 ] = destinationPorts[ i ];
        }
    }

    @Override
    public Future<Void> send ( final TuplesImpl tuples )
    {
        for ( int i = 0; i < portCount; i++ )
        {
            send( tuples, ports[ i ][ 0 ], ports[ i ][ 1 ] );
        }

        return null;
    }
}
