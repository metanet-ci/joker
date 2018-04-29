package cs.bilkent.joker.engine.metric;

import java.util.List;

import cs.bilkent.joker.engine.flow.PipelineId;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.System.arraycopy;
import static java.util.Arrays.asList;

public class PipelineMetricsHistory
{

    private final PipelineMetrics[] history;

    private final int count;

    public PipelineMetricsHistory ( final PipelineMetrics initial, final int historySize )
    {
        this.history = new PipelineMetrics[ historySize ];
        this.history[ 0 ] = initial;
        this.count = 1;
    }

    private PipelineMetricsHistory ( final PipelineMetrics[] history, final int count )
    {
        this.history = history;
        this.count = count;
    }

    public PipelineMetricsHistory add ( final PipelineMetrics pipelineMetrics )
    {
        final PipelineMetrics[] h = new PipelineMetrics[ this.history.length ];
        h[ 0 ] = pipelineMetrics;
        final int length = max( 0, min( this.count, this.history.length - 1 ) );
        arraycopy( this.history, 0, h, 1, length );

        return new PipelineMetricsHistory( h, length + 1 );
    }

    public PipelineMetrics getLatest ()
    {
        return history[ 0 ];
    }

    public int getCount ()
    {
        return count;
    }

    public List<PipelineMetrics> getAll ()
    {
        return asList( history ).subList( 0, count );
    }

    public int getRegionId ()
    {
        return getPipelineId().getRegionId();
    }

    public PipelineId getPipelineId ()
    {
        return getLatest().getPipelineId();
    }

    public int getFlowVersion ()
    {
        return getLatest().getFlowVersion();
    }

    public int getReplicaCount ()
    {
        return getLatest().getReplicaCount();
    }

    public int getOperatorCount ()
    {
        return getLatest().getOperatorCount();
    }

    public int getInputPortCount ()
    {
        return getLatest().getInputPortCount();
    }

}
