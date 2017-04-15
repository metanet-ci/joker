package cs.bilkent.joker.engine.adaptation.impl.pipelinemetricshistorysummarizer;

import cs.bilkent.joker.engine.adaptation.PipelineMetricsHistorySummarizer;
import cs.bilkent.joker.engine.metric.PipelineMetrics;
import cs.bilkent.joker.engine.metric.PipelineMetricsHistory;

public class ExponentialMovingAverage implements PipelineMetricsHistorySummarizer
{

    public ExponentialMovingAverage ()
    {
    }

    @Override
    public PipelineMetrics summarize ( final PipelineMetricsHistory history )
    {
        // TODO to be implemented
        throw new UnsupportedOperationException();
    }

}
