package cs.bilkent.joker.operator;

import java.util.List;

import cs.bilkent.joker.engine.metric.LatencyMeter;
import cs.bilkent.joker.operator.impl.TuplesImpl;
import cs.bilkent.joker.operator.utils.Triple;

public final class TupleAccessor
{

    private TupleAccessor ()
    {
    }

    public static void setIngestionTime ( final Tuple tuple, final long ingestionTime )
    {
        tuple.setIngestionTime( ingestionTime );
    }

    public static void recordLatencies ( final TuplesImpl tuples, final LatencyMeter meter, final long now )
    {
        for ( int i = 0; i < tuples.getPortCount(); i++ )
        {
            final List<Tuple> l = tuples.getTuplesModifiable( i );
            for ( int j = 0; j < l.size(); j++ )
            {
                final Tuple tuple = l.get( j );

                if ( tuple.isIngestionTimeNA() )
                {
                    return;
                }

                final long ingestionTime = tuple.getIngestionTime();

                final long tupleLatency = ( now - ingestionTime );
                if ( tupleLatency > 0 )
                {
                    meter.recordTuple( tupleLatency );
                }

                final List<Triple<String, Boolean, Long>> recs = tuple.getLatencyRecs();

                for ( int k = 0; k < recs.size(); k++ )
                {
                    final Triple<String, Boolean, Long> record = recs.get( k );
                    final long lt = record._3;
                    if ( lt <= 0 )
                    {
                        continue;
                    }

                    final boolean isInvocation = record._2;
                    final String operatorId = record._1;

                    if ( isInvocation )
                    {
                        meter.recordInvocation( operatorId, lt );
                    }
                    else
                    {
                        meter.recordQueue( operatorId, lt );
                    }
                }
            }
        }
    }

    public static void recordInvocationLatency ( final Tuple tuple, final String operatorId, final long latency )
    {
        tuple.recordInvocationLatency( operatorId, latency );
    }

    public static long getIngestionTime ( final Tuple tuple )
    {
        return tuple.getIngestionTime();
    }

    public static void overwriteIngestionTime ( final Tuple target, final Tuple source )
    {
        target.overwriteIngestionTime( source );
    }

    public static void setQueueOfferTime ( final List<Tuple> tuples, final int fromIndex, final long now )
    {
        for ( int i = fromIndex; i < tuples.size(); i++ )
        {
            tuples.get( i ).setQueueOfferTime( now );
        }
    }

    public static void recordQueueLatency ( final List<Tuple> tuples, final String operatorId, final long now )
    {
        for ( int i = 0; i < tuples.size(); i++ )
        {
            tuples.get( i ).recordQueueLatency( operatorId, now );
        }
    }

}
