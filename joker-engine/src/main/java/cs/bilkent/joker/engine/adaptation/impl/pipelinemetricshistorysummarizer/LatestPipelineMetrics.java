package cs.bilkent.joker.engine.adaptation.impl.pipelinemetricshistorysummarizer;

import cs.bilkent.joker.engine.adaptation.PipelineMetricsHistorySummarizer;
import cs.bilkent.joker.engine.metric.PipelineMetricsHistory;
import cs.bilkent.joker.engine.metric.PipelineMetricsSnapshot;
import cs.bilkent.joker.engine.metric.PipelineMetricsSnapshot.PipelineMetricsSnapshotBuilder;

public class LatestPipelineMetrics implements PipelineMetricsHistorySummarizer
{

    public LatestPipelineMetrics ()
    {
    }

    @Override
    public PipelineMetricsSnapshot summarize ( final PipelineMetricsHistory history )
    {
        final PipelineMetricsSnapshot latestSnapshot = history.getLatestSnapshot();
        final PipelineMetricsSnapshotBuilder builder = new PipelineMetricsSnapshotBuilder( latestSnapshot.getPipelineId(),
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
            builder.setInboundThroughput( 0, portIndex, latestSnapshot.getAvgInboundThroughput( portIndex ) );
        }

        return builder.build();
    }

}
