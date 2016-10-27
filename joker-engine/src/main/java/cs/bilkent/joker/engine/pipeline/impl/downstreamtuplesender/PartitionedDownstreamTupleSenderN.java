package cs.bilkent.joker.engine.pipeline.impl.downstreamtuplesender;

import java.util.concurrent.Future;

import static com.google.common.base.Preconditions.checkArgument;
import cs.bilkent.joker.engine.partition.PartitionKeyExtractor;
import cs.bilkent.joker.engine.tuplequeue.TupleQueueContext;
import cs.bilkent.joker.operator.impl.TuplesImpl;

public class PartitionedDownstreamTupleSenderN extends AbstractPartitionedDownstreamTupleSender
{

    private final int[] ports;

    private final int limit;

    public PartitionedDownstreamTupleSenderN ( final int[] sourcePorts,
                                               final int[] destinationPorts,
                                               final int partitionCount,
                                               final int[] partitionDistribution,
                                               final TupleQueueContext[] tupleQueueContexts,
                                               final PartitionKeyExtractor partitionKeyExtractor )
    {
        super( partitionCount, partitionDistribution, tupleQueueContexts, partitionKeyExtractor );
        checkArgument( sourcePorts.length == destinationPorts.length,
                       "source ports size = %s and destination ports = %s ! destination operatorId=%s",
                       sourcePorts.length,
                       destinationPorts.length,
                       tupleQueueContexts[ 0 ].getOperatorId() );
        final int portCount = sourcePorts.length;
        this.ports = new int[ portCount * 2 ];
        this.limit = this.ports.length - 1;
        for ( int i = 0; i < portCount; i++ )
        {
            ports[ i * 2 ] = sourcePorts[ i ];
            ports[ i * 2 + 1 ] = destinationPorts[ i ];
        }
    }

    @Override
    public Future<Void> send ( final TuplesImpl tuples )
    {
        for ( int i = 0; i < limit; i += 2 )
        {
            send( tuples, ports[ i ], ports[ i + 1 ] );
        }

        return null;
    }
}
