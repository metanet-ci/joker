package cs.bilkent.zanza.engine.pipeline.impl.downstreamtuplesender;

import java.util.concurrent.Future;

import cs.bilkent.zanza.engine.partition.PartitionKeyFunction;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueueContext;
import cs.bilkent.zanza.operator.impl.TuplesImpl;

public class PartitionedDownstreamTupleSender1 extends AbstractPartitionedDownstreamTupleSender
{

    private final int sourcePortIndex;

    private final int destinationPortIndex;

    public PartitionedDownstreamTupleSender1 ( final int sourcePortIndex,
                                               final int destinationPortIndex,
                                               final int partitionCount,
                                               final int[] partitionDistribution,
                                               final TupleQueueContext[] tupleQueueContexts,
                                               final PartitionKeyFunction partitionKeyExtractor )
    {
        super( partitionCount, partitionDistribution, tupleQueueContexts, partitionKeyExtractor );
        this.sourcePortIndex = sourcePortIndex;
        this.destinationPortIndex = destinationPortIndex;

    }

    @Override
    public Future<Void> send ( final TuplesImpl input )
    {
        send( input, sourcePortIndex, destinationPortIndex );
        return null;
    }

}
