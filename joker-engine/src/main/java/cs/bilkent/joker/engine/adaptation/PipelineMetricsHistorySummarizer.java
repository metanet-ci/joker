package cs.bilkent.joker.engine.adaptation;

import cs.bilkent.joker.engine.metric.PipelineMetricsHistory;
import cs.bilkent.joker.engine.metric.PipelineMetricsSnapshot;

@FunctionalInterface
public interface PipelineMetricsHistorySummarizer
{

    PipelineMetricsSnapshot summarize ( PipelineMetricsHistory history );

}
