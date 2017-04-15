package cs.bilkent.joker.engine.adaptation;

import cs.bilkent.joker.engine.metric.PipelineMetrics;
import cs.bilkent.joker.engine.metric.PipelineMetricsHistory;

@FunctionalInterface
public interface PipelineMetricsHistorySummarizer
{

    PipelineMetrics summarize ( PipelineMetricsHistory history );

}
