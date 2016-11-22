package cs.bilkent.joker.engine.pipeline.impl.downstreamtuplesender;

import java.util.concurrent.Future;

import cs.bilkent.joker.engine.partition.PartitionKeyExtractor;
import cs.bilkent.joker.engine.pipeline.DownstreamTupleSenderFailureFlag;
import cs.bilkent.joker.engine.tuplequeue.OperatorTupleQueue;
import cs.bilkent.joker.operator.impl.TuplesImpl;

public class PartitionedDownstreamTupleSender4 extends AbstractPartitionedDownstreamTupleSender
{

    private final int sourcePortIndex1;

    private final int destinationPortIndex1;

    private final int sourcePortIndex2;

    private final int destinationPortIndex2;

    private final int sourcePortIndex3;

    private final int destinationPortIndex3;

    private final int sourcePortIndex4;

    private final int destinationPortIndex4;

    public PartitionedDownstreamTupleSender4 ( final DownstreamTupleSenderFailureFlag failureFlag, final int sourcePortIndex1,
                                               final int destinationPortIndex1,
                                               final int sourcePortIndex2,
                                               final int destinationPortIndex2,
                                               final int sourcePortIndex3,
                                               final int destinationPortIndex3,
                                               final int sourcePortIndex4,
                                               final int destinationPortIndex4,
                                               final int partitionCount,
                                               final int[] partitionDistribution, final OperatorTupleQueue[] operatorTupleQueues,
                                               final PartitionKeyExtractor partitionKeyExtractor )
    {
        super( failureFlag, partitionCount, partitionDistribution, operatorTupleQueues, partitionKeyExtractor );
        this.sourcePortIndex1 = sourcePortIndex1;
        this.destinationPortIndex1 = destinationPortIndex1;
        this.sourcePortIndex2 = sourcePortIndex2;
        this.destinationPortIndex2 = destinationPortIndex2;
        this.sourcePortIndex3 = sourcePortIndex3;
        this.destinationPortIndex3 = destinationPortIndex3;
        this.sourcePortIndex4 = sourcePortIndex4;
        this.destinationPortIndex4 = destinationPortIndex4;
    }

    @Override
    public Future<Void> send ( final TuplesImpl input )
    {
        send( input, sourcePortIndex1, destinationPortIndex1 );
        send( input, sourcePortIndex2, destinationPortIndex2 );
        send( input, sourcePortIndex3, destinationPortIndex3 );
        send( input, sourcePortIndex4, destinationPortIndex4 );
        return null;
    }

}
