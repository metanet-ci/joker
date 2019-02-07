package cs.bilkent.joker.engine.metric.impl.pipelinemetricshistorysummarizer;

import cs.bilkent.joker.engine.metric.PipelineMetrics;
import cs.bilkent.joker.engine.metric.PipelineMetrics.PipelineMetricsBuilder;
import cs.bilkent.joker.engine.metric.PipelineMetricsHistory;
import cs.bilkent.joker.engine.metric.PipelineMetricsHistorySummarizer;

public class SimpleMovingAverage implements PipelineMetricsHistorySummarizer
{

    public SimpleMovingAverage ()
    {
    }

    @Override
    public PipelineMetrics summarize ( PipelineMetricsHistory history )
    {
        double cpuUtilRatio = 0;
        double pipelineCost = 0;
        final double[] operatorCosts = new double[ history.getOperatorCount() ];
        final long[] throughputs = new long[ history.getPortCount() ];

        for ( PipelineMetrics metrics : history.getAll() )
        {
            cpuUtilRatio += metrics.getAvgCpuUtilizationRatio();
            pipelineCost += metrics.getAvgPipelineCost();

            for ( int i = 0; i < history.getOperatorCount(); i++ )
            {
                operatorCosts[ i ] += metrics.getAvgOperatorCost( i );
            }

            for ( int i = 0; i < history.getPortCount(); i++ )
            {
                throughputs[ i ] += metrics.getTotalThroughput( i );
            }
        }

        final int historySize = history.getCount();

        final PipelineMetricsBuilder builder = new PipelineMetricsBuilder( history.getPipelineId(),
                                                                           history.getFlowVersion(),
                                                                           1,
                                                                           history.getOperatorCount(), history.getPortCount() );

        builder.setCpuUtilizationRatio( 0, cpuUtilRatio / historySize ).setPipelineCost( 0, pipelineCost / historySize );

        for ( int i = 0; i < history.getOperatorCount(); i++ )
        {
            builder.setOperatorCost( 0, i, operatorCosts[ i ] / historySize );
        }

        for ( int i = 0; i < history.getPortCount(); i++ )
        {
            builder.setThroughput( 0, i, throughputs[ i ] / historySize );
        }

        return builder.build();
    }

}
