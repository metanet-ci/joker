package cs.bilkent.joker.engine.pipeline.impl.downstreamcollector;

import java.util.List;
import java.util.function.LongSupplier;

import cs.bilkent.joker.operator.Tuple;

final class TupleLatencyUtils
{

    private TupleLatencyUtils ()
    {
    }

    static void setQueueOfferTime ( final List<Tuple> tuples, final int fromIndex, final LongSupplier timeSupplier )
    {
        for ( int i = fromIndex; i < tuples.size(); i++ )
        {
            tuples.get( i ).setQueueOfferTime( timeSupplier );
        }
    }

}
