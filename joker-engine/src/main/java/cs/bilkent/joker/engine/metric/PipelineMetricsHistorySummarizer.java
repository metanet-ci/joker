package cs.bilkent.joker.engine.metric;

@FunctionalInterface
public interface PipelineMetricsHistorySummarizer
{

    PipelineMetrics summarize ( PipelineMetricsHistory history );

}
