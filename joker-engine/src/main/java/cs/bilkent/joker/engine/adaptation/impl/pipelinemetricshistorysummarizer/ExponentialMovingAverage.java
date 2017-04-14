package cs.bilkent.joker.engine.adaptation.impl.pipelinemetricshistorysummarizer;

import cs.bilkent.joker.engine.adaptation.PipelineMetricsHistorySummarizer;
import cs.bilkent.joker.engine.metric.PipelineMetricsHistory;
import cs.bilkent.joker.engine.metric.PipelineMetricsSnapshot;

public class ExponentialMovingAverage implements PipelineMetricsHistorySummarizer
{

    public ExponentialMovingAverage ()
    {
    }

    @Override
    public PipelineMetricsSnapshot summarize ( final PipelineMetricsHistory history )
    {
        // TODO to be implemented
        throw new UnsupportedOperationException();
    }

}
