package cs.bilkent.joker.engine.pipeline.impl.downstreamtuplesender;

import java.util.concurrent.Future;

import cs.bilkent.joker.engine.partition.PartitionKeyExtractor;
import cs.bilkent.joker.engine.pipeline.DownstreamTupleSenderFailureFlag;
import cs.bilkent.joker.engine.tuplequeue.OperatorTupleQueue;
import cs.bilkent.joker.operator.impl.TuplesImpl;

public class PartitionedDownstreamTupleSender1 extends AbstractPartitionedDownstreamTupleSender
{

    private final int sourcePortIndex;

    private final int destinationPortIndex;

    public PartitionedDownstreamTupleSender1 ( final DownstreamTupleSenderFailureFlag failureFlag, final int sourcePortIndex,
                                               final int destinationPortIndex,
                                               final int partitionCount,
                                               final int[] partitionDistribution, final OperatorTupleQueue[] operatorTupleQueues,
                                               final PartitionKeyExtractor partitionKeyExtractor )
    {
        super( failureFlag, partitionCount, partitionDistribution, operatorTupleQueues, partitionKeyExtractor );
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
