package cs.bilkent.joker.engine.metric;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

import cs.bilkent.joker.engine.flow.PipelineId;
import static java.util.Comparator.comparing;

public class FlowMetrics
{

    private final int period;

    private final Map<PipelineId, PipelineMetricsHistory> histories;

    public FlowMetrics ( final int period, final Map<PipelineId, PipelineMetricsHistory> histories )
    {
        this.period = period;
        this.histories = new TreeMap<>( histories );
    }

    public int getPeriod ()
    {
        return period;
    }

    public PipelineMetricsHistory getPipelineMetricsHistory ( final PipelineId pipelineId )
    {
        return histories.get( pipelineId );
    }

    public PipelineMetrics getLatestPipelineMetrics ( final PipelineId pipelineId )
    {
        final PipelineMetricsHistory pipelineMetricsHistory = histories.get( pipelineId );
        return pipelineMetricsHistory != null ? pipelineMetricsHistory.getLatest() : null;
    }

    public List<PipelineMetricsHistory> getRegionMetrics ( final int regionId )
    {
        final List<PipelineMetricsHistory> metrics = histories.values()
                                                              .stream()
                                                              .filter( history -> history.getRegionId() == regionId )
                                                              .collect( Collectors.toList() );

        metrics.sort( comparing( PipelineMetricsHistory::getPipelineId ) );

        return metrics;
    }

    @Override
    public String toString ()
    {
        return "FlowMetrics{" + "period=" + period + ", histories=" + histories + '}';
    }

}
