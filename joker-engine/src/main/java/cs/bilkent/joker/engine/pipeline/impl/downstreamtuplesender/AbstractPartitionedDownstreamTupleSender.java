package cs.bilkent.joker.engine.pipeline.impl.downstreamtuplesender;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Future;
import java.util.function.Supplier;

import cs.bilkent.joker.engine.partition.PartitionKeyExtractor;
import static cs.bilkent.joker.engine.partition.PartitionUtil.getPartitionId;
import cs.bilkent.joker.engine.pipeline.DownstreamTupleSender;
import cs.bilkent.joker.engine.tuplequeue.OperatorTupleQueue;
import cs.bilkent.joker.operator.Tuple;
import cs.bilkent.joker.operator.impl.TuplesImpl;

public abstract class AbstractPartitionedDownstreamTupleSender implements DownstreamTupleSender, Supplier<OperatorTupleQueue[]>
{

    private final int partitionCount;

    private final int[] partitionDistribution;

    private final int replicaCount;

    private final OperatorTupleQueue[] operatorTupleQueues;

    private final PartitionKeyExtractor partitionKeyExtractor;

    private List<Tuple>[] tupleLists;

    public AbstractPartitionedDownstreamTupleSender ( final int partitionCount,
                                                      final int[] partitionDistribution, final OperatorTupleQueue[] operatorTupleQueues,
                                                      final PartitionKeyExtractor partitionKeyExtractor )
    {
        this.partitionCount = partitionCount;
        this.partitionDistribution = Arrays.copyOf( partitionDistribution, partitionDistribution.length );
        this.replicaCount = operatorTupleQueues.length;
        this.operatorTupleQueues = Arrays.copyOf( operatorTupleQueues, operatorTupleQueues.length );
        this.partitionKeyExtractor = partitionKeyExtractor;
        this.tupleLists = new List[ operatorTupleQueues.length ];
        for ( int i = 0; i < operatorTupleQueues.length; i++ )
        {
            tupleLists[ i ] = new ArrayList<>();
        }
    }

    public final OperatorTupleQueue[] get ()
    {
        return Arrays.copyOf( operatorTupleQueues, operatorTupleQueues.length );
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
                operatorTupleQueues[ i ].offer( destinationPortIndex, tuples );
                tuples.clear();
            }
        }

        return null;
    }

}
