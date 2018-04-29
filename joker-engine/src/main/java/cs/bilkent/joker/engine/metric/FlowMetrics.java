package cs.bilkent.joker.engine.metric;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import cs.bilkent.joker.engine.flow.PipelineId;
import cs.bilkent.joker.operator.utils.Pair;
import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.toList;

public class FlowMetrics
{

    private final int period;

    private final Map<PipelineId, PipelineMetricsHistory> pipelineMetricsHistories;

    private final Map<Pair<String, Integer>, LatencyMetricsHistory> latencyMetricsHistories;

    public FlowMetrics ( final int period,
                         final Map<PipelineId, PipelineMetricsHistory> pipelineMetricsHistories,
                         final Map<Pair<String, Integer>, LatencyMetricsHistory> latencyMetricsHistories )
    {
        this.period = period;
        this.pipelineMetricsHistories = new TreeMap<>( pipelineMetricsHistories );
        this.latencyMetricsHistories = new HashMap<>( latencyMetricsHistories );
    }

    public int getPeriod ()
    {
        return period;
    }

    public PipelineMetricsHistory getPipelineMetricsHistory ( final PipelineId pipelineId )
    {
        return pipelineMetricsHistories.get( pipelineId );
    }

    public PipelineMetrics getLatestPipelineMetrics ( final PipelineId pipelineId )
    {
        final PipelineMetricsHistory pipelineMetricsHistory = pipelineMetricsHistories.get( pipelineId );
        return pipelineMetricsHistory != null ? pipelineMetricsHistory.getLatest() : null;
    }

    public List<PipelineMetricsHistory> getRegionMetrics ( final int regionId )
    {
        return pipelineMetricsHistories.values()
                                       .stream()
                                       .filter( history -> history.getRegionId() == regionId )
                                       .sorted( comparing( PipelineMetricsHistory::getPipelineId ) )
                                       .collect( toList() );
    }

    public List<PipelineMetrics> getRegionMetrics ( final int regionId,
                                                    final PipelineMetricsHistorySummarizer pipelineMetricsHistorySummarizer )
    {
        return getRegionMetrics( regionId ).stream().map( pipelineMetricsHistorySummarizer::summarize ).collect( toList() );
    }

    public LatencyMetricsHistory getLatencyMetricsHistory ( final Pair<String, Integer> operatorReplica )
    {
        return latencyMetricsHistories.get( operatorReplica );
    }

    public Collection<LatencyMetricsHistory> getLatencyMetricsHistories ()
    {
        return Collections.unmodifiableCollection( latencyMetricsHistories.values() );
    }

    @Override
    public String toString ()
    {
        return "FlowMetrics{" + "period=" + period + ", pipelineMetricsHistories=" + pipelineMetricsHistories + ", latencyMetricsHistories="
               + latencyMetricsHistories + '}';
    }
}
