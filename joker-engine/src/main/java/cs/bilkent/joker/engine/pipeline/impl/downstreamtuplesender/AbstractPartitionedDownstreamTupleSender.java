package cs.bilkent.joker.engine.pipeline.impl.downstreamtuplesender;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Future;
import java.util.function.Supplier;

import cs.bilkent.joker.engine.partition.PartitionKeyFunction;
import static cs.bilkent.joker.engine.partition.PartitionUtil.getPartitionId;
import cs.bilkent.joker.engine.pipeline.DownstreamTupleSender;
import cs.bilkent.joker.engine.tuplequeue.TupleQueueContext;
import cs.bilkent.joker.operator.Tuple;
import cs.bilkent.joker.operator.impl.TuplesImpl;

public abstract class AbstractPartitionedDownstreamTupleSender implements DownstreamTupleSender, Supplier<TupleQueueContext[]>
{

    private final int partitionCount;

    private final int[] partitionDistribution;

    private final int replicaCount;

    private final TupleQueueContext[] tupleQueueContexts;

    private final PartitionKeyFunction partitionKeyExtractor;

    private List<Tuple>[] tupleLists;

    public AbstractPartitionedDownstreamTupleSender ( final int partitionCount,
                                                      final int[] partitionDistribution,
                                                      final TupleQueueContext[] tupleQueueContexts,
                                                      final PartitionKeyFunction partitionKeyExtractor )
    {
        this.partitionCount = partitionCount;
        this.partitionDistribution = Arrays.copyOf( partitionDistribution, partitionDistribution.length );
        this.replicaCount = tupleQueueContexts.length;
        this.tupleQueueContexts = Arrays.copyOf( tupleQueueContexts, tupleQueueContexts.length );
        this.partitionKeyExtractor = partitionKeyExtractor;
        this.tupleLists = new List[ tupleQueueContexts.length ];
        for ( int i = 0; i < tupleQueueContexts.length; i++ )
        {
            tupleLists[ i ] = new ArrayList<>();
        }
    }

    public final TupleQueueContext[] get ()
    {
        return Arrays.copyOf( tupleQueueContexts, tupleQueueContexts.length );
    }

    protected final Future<Void> send ( final TuplesImpl input, final int sourcePortIndex, final int destinationPortIndex )
    {
        for ( Tuple tuple : input.getTuples( sourcePortIndex ) )
        {
            final int partitionId = getPartitionId( partitionKeyExtractor.getPartitionHash( tuple ), partitionCount );
            final int replicaIndex = partitionDistribution[ partitionId ];
            tupleLists[ replicaIndex ].add( tuple );
        }

        for ( int i = 0; i < replicaCount; i++ )
        {
            final List<Tuple> tuples = tupleLists[ i ];
            if ( tuples.size() > 0 )
            {
                tupleQueueContexts[ i ].offer( destinationPortIndex, tuples );
                tuples.clear();
            }
        }

        return null;
    }

}
