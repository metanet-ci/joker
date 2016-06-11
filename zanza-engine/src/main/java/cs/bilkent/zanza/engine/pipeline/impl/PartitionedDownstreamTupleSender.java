package cs.bilkent.zanza.engine.pipeline.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.function.Function;

import cs.bilkent.zanza.engine.pipeline.DownstreamTupleSender;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueueContext;
import cs.bilkent.zanza.operator.Tuple;
import cs.bilkent.zanza.operator.impl.TuplesImpl;

public class PartitionedDownstreamTupleSender implements DownstreamTupleSender
{

    private final int sourcePortIndex;

    private final int destinationPortIndex;

    private final TupleQueueContext tupleQueueContext;

    private final Function<Tuple, Object> partitionKeyExtractor;

    private Map<Object, List<Tuple>> tuplesByKey = new HashMap<>();

    private List<List<Tuple>> tupleLists = new ArrayList<>();

    public PartitionedDownstreamTupleSender ( final int sourcePortIndex,
                                              final int destinationPortIndex,
                                              final TupleQueueContext tupleQueueContext,
                                              final Function<Tuple, Object> partitionKeyExtractor )
    {
        this.sourcePortIndex = sourcePortIndex;
        this.destinationPortIndex = destinationPortIndex;
        this.tupleQueueContext = tupleQueueContext;
        this.partitionKeyExtractor = partitionKeyExtractor;
    }

    @Override
    public Future<Void> send ( final TuplesImpl input )
    {
        for ( Tuple tuple : input.getTuples( sourcePortIndex ) )
        {
            final Object key = partitionKeyExtractor.apply( tuple );
            List<Tuple> tuples = tuplesByKey.get( key );
            if ( tuples == null )
            {
                if ( tupleLists.isEmpty() )
                {
                    tuples = new ArrayList<>();
                }
                else
                {
                    tuples = tupleLists.remove( tupleLists.size() - 1 );
                }

                tuplesByKey.put( key, tuples );
            }

            tuples.add( tuple );
        }

        for ( List<Tuple> tuples : tuplesByKey.values() )
        {
            tupleQueueContext.offer( destinationPortIndex, tuples );
            tuples.clear();
            tupleLists.add( tuples );
        }
        tuplesByKey.clear();

        return null;
    }

}
