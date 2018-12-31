package cs.bilkent.joker.engine.pipeline.impl.downstreamcollector;

import java.util.concurrent.atomic.AtomicBoolean;
import javax.inject.Named;

import static cs.bilkent.joker.JokerModule.DOWNSTREAM_FAILURE_FLAG_NAME;
import cs.bilkent.joker.engine.metric.PipelineReplicaMeter.Ticker;
import cs.bilkent.joker.engine.partition.PartitionKeyExtractor;
import cs.bilkent.joker.engine.tuplequeue.OperatorQueue;
import cs.bilkent.joker.operator.impl.TuplesImpl;

public class PartitionedDownstreamCollector1 extends AbstractPartitionedDownstreamCollector
{

    private final int sourcePortIndex;

    private final int destinationPortIndex;

    public PartitionedDownstreamCollector1 ( @Named( DOWNSTREAM_FAILURE_FLAG_NAME ) final AtomicBoolean failureFlag,
                                             final int sourcePortIndex,
                                             final int destinationPortIndex,
                                             final int partitionCount,
                                             final int[] partitionDistribution,
                                             final OperatorQueue[] operatorQueues,
                                             final PartitionKeyExtractor partitionKeyExtractor,
                                             final Ticker ticker )
    {
        super( failureFlag, partitionCount, partitionDistribution, operatorQueues, partitionKeyExtractor, ticker );
        this.sourcePortIndex = sourcePortIndex;
        this.destinationPortIndex = destinationPortIndex;
    }

    @Override
    public void accept ( final TuplesImpl input )
    {
        send( input, sourcePortIndex, destinationPortIndex );
    }

}
