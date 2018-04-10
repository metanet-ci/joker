package cs.bilkent.joker.operator;

import cs.bilkent.joker.engine.metric.LatencyMeter;
import static cs.bilkent.joker.operator.Tuple.INGESTION_TIME_NA;

public final class TupleAccessor
{

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

    public static void record ( final Tuple tuple, final LatencyMeter meter, final long now )
    {
        final long ingestionTime = tuple.getIngestionTime();
        if ( ingestionTime == INGESTION_TIME_NA )
        {
            return;
        }

        final long latency = ( now - ingestionTime );
        if ( latency > 0 )
        {
            meter.record( latency );
        }
    }

    public static long getIngestionTime ( final Tuple tuple )
    {
        return tuple.getIngestionTime();
    }

}
