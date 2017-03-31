package cs.bilkent.joker.engine.metric;

import java.util.Map;
import java.util.TreeMap;

import cs.bilkent.joker.engine.flow.PipelineId;

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

}
