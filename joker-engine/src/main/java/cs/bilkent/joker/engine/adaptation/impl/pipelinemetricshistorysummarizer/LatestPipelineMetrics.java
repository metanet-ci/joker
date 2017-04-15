package cs.bilkent.joker.engine.adaptation.impl.pipelinemetricshistorysummarizer;

import cs.bilkent.joker.engine.adaptation.PipelineMetricsHistorySummarizer;
import cs.bilkent.joker.engine.metric.PipelineMetrics;
import cs.bilkent.joker.engine.metric.PipelineMetrics.PipelineMetricsBuilder;
import cs.bilkent.joker.engine.metric.PipelineMetricsHistory;

public class LatestPipelineMetrics implements PipelineMetricsHistorySummarizer
{

    public LatestPipelineMetrics ()
    {
    }

    @Override
    public PipelineMetrics summarize ( final PipelineMetricsHistory history )
    {
        final PipelineMetrics latestSnapshot = history.getLatest();
        final PipelineMetricsBuilder builder = new PipelineMetricsBuilder( latestSnapshot.getPipelineId(),
                                                                           latestSnapshot.getFlowVersion(),
                                                                           1,
                                                                           latestSnapshot.getOperatorCount(),
                                                                           latestSnapshot.getInputPortCount() );

        builder.setCpuUtilizationRatio( 0, latestSnapshot.getAvgCpuUtilizationRatio() )
               .setPipelineCost( 0, latestSnapshot.getAvgPipelineCost() );

        for ( int operatorIndex = 0; operatorIndex < latestSnapshot.getOperatorCount(); operatorIndex++ )
        {
            builder.setOperatorCost( 0, operatorIndex, latestSnapshot.getAvgOperatorCost( operatorIndex ) );
        }

        for ( int portIndex = 0; portIndex < latestSnapshot.getInputPortCount(); portIndex++ )
        {
            builder.setInboundThroughput( 0, portIndex, latestSnapshot.getTotalInboundThroughput( portIndex ) );
        }

        return builder.build();
    }

}
