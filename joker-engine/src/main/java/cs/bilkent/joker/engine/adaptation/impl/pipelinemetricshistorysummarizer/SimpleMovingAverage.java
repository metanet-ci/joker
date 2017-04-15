package cs.bilkent.joker.engine.adaptation.impl.pipelinemetricshistorysummarizer;

import cs.bilkent.joker.engine.adaptation.PipelineMetricsHistorySummarizer;
import cs.bilkent.joker.engine.metric.PipelineMetrics;
import cs.bilkent.joker.engine.metric.PipelineMetrics.PipelineMetricsBuilder;
import cs.bilkent.joker.engine.metric.PipelineMetricsHistory;

public class SimpleMovingAverage implements PipelineMetricsHistorySummarizer
{

    public SimpleMovingAverage ()
    {
    }

    @Override
    public PipelineMetrics summarize ( PipelineMetricsHistory history )
    {
        final PipelineMetricsBuilder builder = new PipelineMetricsBuilder( history.getPipelineId(),
                                                                           history.getFlowVersion(),
                                                                           1,
                                                                           history.getOperatorCount(),
                                                                           history.getInputPortCount() );

        double cpuUtilRatio = 0;
        double pipelineCost = 0;
        double[] operatorCosts = new double[ history.getOperatorCount() ];
        long[] throughputs = new long[ history.getInputPortCount() ];

        for ( PipelineMetrics metrics : history.getAll() )
        {
            cpuUtilRatio += metrics.getAvgCpuUtilizationRatio();
            pipelineCost += metrics.getAvgPipelineCost();

            for ( int i = 0; i < history.getOperatorCount(); i++ )
            {
                operatorCosts[ i ] += metrics.getAvgOperatorCost( i );
            }

            for ( int i = 0; i < history.getInputPortCount(); i++ )
            {
                throughputs[ i ] += metrics.getTotalInboundThroughput( i );
            }
        }

        final int historySize = history.getCount();

        builder.setCpuUtilizationRatio( 0, cpuUtilRatio / historySize ).setPipelineCost( 0, pipelineCost / historySize );

        for ( int i = 0; i < history.getOperatorCount(); i++ )
        {
            builder.setOperatorCost( 0, i, operatorCosts[ i ] / historySize );
        }

        for ( int i = 0; i < history.getInputPortCount(); i++ )
        {
            builder.setInboundThroughput( 0, i, throughputs[ i ] / historySize );
        }

        return builder.build();
    }

}
