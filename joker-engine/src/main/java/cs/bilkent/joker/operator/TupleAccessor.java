package cs.bilkent.joker.operator;

import java.util.List;

import cs.bilkent.joker.engine.metric.LatencyMeter;
import cs.bilkent.joker.operator.utils.Triple;

public final class TupleAccessor
{

    public static final long INGESTION_TIME_NA = Tuple.INGESTION_TIME_NA;

    private TupleAccessor ()
    {
    }

    public static void setIngestionTime ( final Tuple tuple, final long ingestionTime )
    {
        if ( ingestionTime == INGESTION_TIME_NA )
        {
            return;
        }

        tuple.setIngestionTime( ingestionTime );
    }

    public static void recordLatency ( final Tuple tuple, final LatencyMeter meter, final long end )
    {
        final long ingestionTime = tuple.getIngestionTime();
        if ( ingestionTime == INGESTION_TIME_NA )
        {
            return;
        }

        final long tupleLatency = ( end - ingestionTime );
        if ( tupleLatency > 0 )
        {
            meter.recordTuple( tupleLatency );
        }

        final List<Triple<String, Boolean, Long>> recs = tuple.getLatencyRecs();

        for ( int i = 0; i < recs.size(); i++ )
        {
            final Triple<String, Boolean, Long> record = recs.get( i );
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

}
