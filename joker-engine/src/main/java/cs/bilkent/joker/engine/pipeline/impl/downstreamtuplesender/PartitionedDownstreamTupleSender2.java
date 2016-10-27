package cs.bilkent.joker.engine.pipeline.impl.downstreamtuplesender;

import java.util.concurrent.Future;

import cs.bilkent.joker.engine.partition.PartitionKeyExtractor;
import cs.bilkent.joker.engine.tuplequeue.TupleQueueContext;
import cs.bilkent.joker.operator.impl.TuplesImpl;

public class PartitionedDownstreamTupleSender2 extends AbstractPartitionedDownstreamTupleSender
{

    private final int sourcePortIndex1;

    private final int destinationPortIndex1;

    private final int sourcePortIndex2;

    private final int destinationPortIndex2;

    public PartitionedDownstreamTupleSender2 ( final int sourcePortIndex1,
                                               final int destinationPortIndex1,
                                               final int sourcePortIndex2,
                                               final int destinationPortIndex2,
                                               final int partitionCount,
                                               final int[] partitionDistribution,
                                               final TupleQueueContext[] tupleQueueContexts,
                                               final PartitionKeyExtractor partitionKeyExtractor )
    {
        super( partitionCount, partitionDistribution, tupleQueueContexts, partitionKeyExtractor );
        this.sourcePortIndex1 = sourcePortIndex1;
        this.destinationPortIndex1 = destinationPortIndex1;
        this.sourcePortIndex2 = sourcePortIndex2;
        this.destinationPortIndex2 = destinationPortIndex2;
    }

    @Override
    public Future<Void> send ( final TuplesImpl input )
    {
        send( input, sourcePortIndex1, destinationPortIndex1 );
        send( input, sourcePortIndex2, destinationPortIndex2 );
        return null;
    }

}
