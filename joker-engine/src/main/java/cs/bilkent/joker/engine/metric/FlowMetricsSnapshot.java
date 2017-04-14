package cs.bilkent.joker.engine.metric;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

import cs.bilkent.joker.engine.flow.PipelineId;
import static java.util.Comparator.comparing;

public class FlowMetricsSnapshot
{

    private final int period;

    private final Map<PipelineId, PipelineMetricsHistory> pipelineMetricHistories;

    public FlowMetricsSnapshot ( final int period, final Map<PipelineId, PipelineMetricsHistory> pipelineMetricHistories )
    {
        this.period = period;
        this.pipelineMetricHistories = new TreeMap<>( pipelineMetricHistories );
    }

    public int getPeriod ()
    {
        return period;
    }

    public PipelineMetricsHistory getPipelineMetricsHistory ( final PipelineId pipelineId )
    {
        return pipelineMetricHistories.get( pipelineId );
    }

    public List<PipelineMetricsHistory> getRegionMetrics ( final int regionId )
    {
        final List<PipelineMetricsHistory> metrics = pipelineMetricHistories.values()
                                                                            .stream()
                                                                            .filter( history -> history.getRegionId() == regionId )
                                                                            .collect( Collectors.toList() );

        metrics.sort( comparing( PipelineMetricsHistory::getPipelineId ) );

        return metrics;
    }

}
