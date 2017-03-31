package cs.bilkent.joker.engine.metric;

import java.util.List;

import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.System.arraycopy;
import static java.util.Arrays.asList;

public class PipelineMetricsHistory
{

    private final PipelineMetricsSnapshot[] snapshots;

    private final int count;

    public PipelineMetricsHistory ( final PipelineMetricsSnapshot initial, final int historySize )
    {
        this.snapshots = new PipelineMetricsSnapshot[ historySize ];
        this.snapshots[ 0 ] = initial;
        this.count = 1;
    }

    private PipelineMetricsHistory ( final PipelineMetricsSnapshot[] snapshots, final int count )
    {
        this.snapshots = snapshots;
        this.count = count;
    }

    public PipelineMetricsHistory add ( final PipelineMetricsSnapshot snapshot )
    {
        final PipelineMetricsSnapshot[] snapshots = new PipelineMetricsSnapshot[ this.snapshots.length ];
        snapshots[ 0 ] = snapshot;
        final int length = max( 0, min( this.count, this.snapshots.length - 1 ) );
        arraycopy( this.snapshots, 0, snapshots, 1, length );

        return new PipelineMetricsHistory( snapshots, length + 1 );
    }

    public PipelineMetricsSnapshot getLatestSnapshot ()
    {
        return snapshots[ 0 ];
    }

    public int getCount ()
    {
        return count;
    }

    public int getHistorySize ()
    {
        return snapshots.length;
    }

    public List<PipelineMetricsSnapshot> getSnapshots ()
    {
        return asList( snapshots ).subList( 0, count );
    }

}
