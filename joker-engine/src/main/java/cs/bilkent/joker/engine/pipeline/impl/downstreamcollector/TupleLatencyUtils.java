package cs.bilkent.joker.engine.pipeline.impl.downstreamcollector;

import java.util.List;

import cs.bilkent.joker.operator.Tuple;

final class TupleLatencyUtils
{

    private TupleLatencyUtils ()
    {
    }

    static void setQueueOfferTime ( final List<Tuple> tuples, final int fromIndex, final long now )
    {
        for ( int i = fromIndex; i < tuples.size(); i++ )
        {
            tuples.get( i ).setQueueOfferTime( now );
        }
    }

}
