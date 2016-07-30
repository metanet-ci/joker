package cs.bilkent.zanza.engine.pipeline.impl.downstreamtuplesender;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Future;

import cs.bilkent.zanza.engine.partition.PartitionKeyFunction;
import static cs.bilkent.zanza.engine.partition.PartitionUtil.getPartitionId;
import cs.bilkent.zanza.engine.pipeline.DownstreamTupleSender;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueueContext;
import cs.bilkent.zanza.operator.Tuple;
import cs.bilkent.zanza.operator.impl.TuplesImpl;

public abstract class AbstractPartitionedDownstreamTupleSender implements DownstreamTupleSender
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
