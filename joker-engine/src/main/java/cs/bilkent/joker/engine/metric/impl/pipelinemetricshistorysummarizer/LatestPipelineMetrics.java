package cs.bilkent.joker.engine.metric.impl.pipelinemetricshistorysummarizer;

import cs.bilkent.joker.engine.metric.PipelineMetrics;
import cs.bilkent.joker.engine.metric.PipelineMetrics.PipelineMetricsBuilder;
import cs.bilkent.joker.engine.metric.PipelineMetricsHistory;
import cs.bilkent.joker.engine.metric.PipelineMetricsHistorySummarizer;

public class LatestPipelineMetrics implements PipelineMetricsHistorySummarizer
{

    public LatestPipelineMetrics ()
    {
    }

    @Override
    public PipelineMetrics summarize ( final PipelineMetricsHistory history )
    {
        final PipelineMetrics latest = history.getLatest();
        final PipelineMetricsBuilder builder = new PipelineMetricsBuilder( latest.getPipelineId(),
                                                                           latest.getFlowVersion(),
                                                                           1,
                                                                           latest.getOperatorCount(),
                                                                           latest.getPortCount() );

        builder.setCpuUtilizationRatio( 0, latest.getAvgCpuUtilizationRatio() ).setPipelineCost( 0, latest.getAvgPipelineCost() );

        for ( int operatorIndex = 0; operatorIndex < latest.getOperatorCount(); operatorIndex++ )
        {
            builder.setOperatorCost( 0, operatorIndex, latest.getAvgOperatorCost( operatorIndex ) );
        }

        for ( int portIndex = 0; portIndex < latest.getPortCount(); portIndex++ )
        {
            builder.setThroughput( 0, portIndex, latest.getTotalThroughput( portIndex ) );
        }

        return builder.build();
    }

}
