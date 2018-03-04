package cs.bilkent.joker.engine.pipeline.impl.downstreamtuplesender;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;

import cs.bilkent.joker.engine.exception.JokerException;
import cs.bilkent.joker.engine.partition.PartitionKeyExtractor;
import static cs.bilkent.joker.engine.partition.PartitionUtil.getPartitionId;
import cs.bilkent.joker.engine.pipeline.DownstreamTupleSender;
import cs.bilkent.joker.engine.pipeline.DownstreamTupleSenderFailureFlag;
import cs.bilkent.joker.engine.tuplequeue.OperatorQueue;
import cs.bilkent.joker.operator.Tuple;
import cs.bilkent.joker.operator.impl.TuplesImpl;

public abstract class AbstractPartitionedDownstreamTupleSender extends AbstractDownstreamTupleSender implements DownstreamTupleSender, Supplier<OperatorQueue[]>
{

    private final int partitionCount;

    private final int[] partitionDistribution;

    private final int replicaCount;

    private final OperatorQueue[] operatorQueues;

    private final PartitionKeyExtractor partitionKeyExtractor;

    private List<Tuple>[] tupleLists;

    private int[] indices;

    AbstractPartitionedDownstreamTupleSender ( final DownstreamTupleSenderFailureFlag failureFlag,
                                               final int partitionCount,
                                               final int[] partitionDistribution, final OperatorQueue[] operatorQueues,
                                               final PartitionKeyExtractor partitionKeyExtractor )
    {
        super( failureFlag );
        this.partitionCount = partitionCount;
        this.partitionDistribution = Arrays.copyOf( partitionDistribution, partitionDistribution.length );
        this.replicaCount = operatorQueues.length;
        this.operatorQueues = Arrays.copyOf( operatorQueues, operatorQueues.length );
        this.partitionKeyExtractor = partitionKeyExtractor;
        this.tupleLists = new List[ operatorQueues.length ];
        this.indices = new int[ operatorQueues.length ];
        for ( int i = 0; i < operatorQueues.length; i++ )
        {
            tupleLists[ i ] = new ArrayList<>();
        }
    }

    public final OperatorQueue[] get ()
    {
        return Arrays.copyOf( operatorQueues, operatorQueues.length );
    }

    protected final void send ( final TuplesImpl input, final int sourcePortIndex, final int destinationPortIndex )
    {
        for ( Tuple tuple : input.getTuplesModifiable( sourcePortIndex ) )
        {
            final int partitionId = getPartitionId( partitionKeyExtractor.getPartitionHash( tuple ), partitionCount );
            final int replicaIndex = partitionDistribution[ partitionId ];
            tupleLists[ replicaIndex ].add( tuple );
        }

        int completed;
        while ( true )
        {
            completed = 0;
            for ( int i = 0; i < replicaCount; i++ )
            {
                final List<Tuple> tuples = tupleLists[ i ];
                int fromIndex = indices[ i ];
                if ( fromIndex < tuples.size() )
                {
                    final int offered = operatorQueues[ i ].offer( destinationPortIndex, tuples, fromIndex );
                    if ( offered == 0 )
                    {
                        if ( idleStrategy.idle() )
                        {
                            if ( failureFlag.isFailed() )
                            {
                                throw new JokerException( "Not sending tuples to downstream since failure flag is set" );
                            }
                        }
                    }
                    else
                    {
                        idleStrategy.reset();
                        fromIndex += offered;
                        indices[ i ] = fromIndex;
                    }
                }

                if ( fromIndex == tuples.size() )
                {
                    completed++;
                }
            }

            if ( completed == replicaCount )
            {
                break;
            }
        }

        idleStrategy.reset();
        for ( int i = 0; i < replicaCount; i++ )
        {
            tupleLists[ i ].clear();
            indices[ i ] = 0;
        }
    }

}
