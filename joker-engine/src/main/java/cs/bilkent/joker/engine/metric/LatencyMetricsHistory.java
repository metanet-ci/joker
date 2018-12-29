package cs.bilkent.joker.engine.metric;

import java.util.Arrays;
import java.util.List;

import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.Math.sqrt;
import static java.lang.System.arraycopy;
import static java.util.Arrays.asList;

public class LatencyMetricsHistory
{

    private final LatencyMetrics[] history;

    private final int count;

    public LatencyMetricsHistory ( final LatencyMetrics initial, final int historySize )
    {
        this.history = new LatencyMetrics[ historySize ];
        this.history[ 0 ] = initial;
        this.count = 1;
    }

    private LatencyMetricsHistory ( final LatencyMetrics[] history, final int count )
    {
        this.history = history;
        this.count = count;
    }

    public LatencyMetricsHistory add ( final LatencyMetrics latencyMetrics )
    {
        final LatencyMetrics[] h = new LatencyMetrics[ this.history.length ];
        h[ 0 ] = latencyMetrics;
        final int length = max( 0, min( this.count, this.history.length - 1 ) );
        arraycopy( this.history, 0, h, 1, length );

        return new LatencyMetricsHistory( h, length + 1 );
    }

    public LatencyMetrics getLatest ()
    {
        return history[ 0 ];
    }

    public long getTupleLatencyMean ()
    {
        return (long) getAll().stream().mapToLong( LatencyMetrics::getTupleLatencyMean ).average().orElse( 0 );
    }

    public long getTupleLatencyStdDev ()
    {
        final long variance = (long) getAll().stream().mapToLong( LatencyMetrics::getTupleLatencyVariance ).average().orElse( 0 );
        return (long) sqrt( variance );
    }

    public long getServiceTimeMean ( final String operatorId )
    {
        return (long) getAll().stream().mapToLong( m -> m.getServiceTimeMean( operatorId ) ).average().orElse( 0 );
    }

    public long getServiceTimeStdDev ( final String operatorId )
    {
        final long variance = (long) getAll().stream().mapToLong( m -> m.getServiceTimeVar( operatorId ) ).average().orElse( 0 );
        return (long) sqrt( variance );
    }

    public long getQueueWaitingTimeMean ( final String operatorId )
    {
        return (long) getAll().stream().mapToLong( m -> m.getQueueWaitingTimeMean( operatorId ) ).average().orElse( 0 );
    }

    public long getQueueWaitingTimeStdDev ( final String operatorId )
    {
        final long variance = (long) getAll().stream().mapToLong( m -> m.getQueueWaitingTimeVar( operatorId ) ).average().orElse( 0 );
        return (long) sqrt( variance );
    }

    public long getInterArrivalTimeMean ( final String operatorId )
    {
        return (long) getAll().stream().mapToLong( m -> m.getInterArrivalTimeMean( operatorId ) ).average().orElse( 0 );
    }

    public long getInterArrivalTimeStdDev ( final String operatorId )
    {
        final long variance = (long) getAll().stream().mapToLong( m -> m.getInterArrivalTimeVar( operatorId ) ).average().orElse( 0 );
        return (long) sqrt( variance );
    }

    public int getCount ()
    {
        return count;
    }

    public List<LatencyMetrics> getAll ()
    {
        return asList( history ).subList( 0, count );
    }

    public String getSinkOperatorId ()
    {
        return getLatest().getSinkOperatorId();
    }

    public int getReplicaIndex ()
    {
        return getLatest().getReplicaIndex();
    }

    public int getFlowVersion ()
    {
        return getLatest().getFlowVersion();
    }

    @Override
    public String toString ()
    {
        return "LatencyMetricsHistory{" + "history=" + Arrays.toString( history ) + ", count=" + count + '}';
    }
}
