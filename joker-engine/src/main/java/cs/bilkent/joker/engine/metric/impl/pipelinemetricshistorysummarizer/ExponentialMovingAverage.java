package cs.bilkent.joker.engine.metric.impl.pipelinemetricshistorysummarizer;

import java.util.ArrayList;
import java.util.List;

import cs.bilkent.joker.engine.metric.PipelineMetrics;
import cs.bilkent.joker.engine.metric.PipelineMetrics.PipelineMetricsBuilder;
import cs.bilkent.joker.engine.metric.PipelineMetricsHistory;
import cs.bilkent.joker.engine.metric.PipelineMetricsHistorySummarizer;
import static java.util.Collections.reverse;

public class ExponentialMovingAverage implements PipelineMetricsHistorySummarizer
{

    private static final double ALPHA = 0.75;


    public ExponentialMovingAverage ()
    {
    }

    @Override
    public PipelineMetrics summarize ( final PipelineMetricsHistory history )
    {

        final List<PipelineMetrics> p = new ArrayList<>( history.getAll() );
        reverse( p );

        double cpuUtilRatio = p.get( 0 ).getAvgCpuUtilizationRatio();
        double pipelineCost = p.get( 0 ).getAvgPipelineCost();
        final double[] operatorCosts = new double[ history.getOperatorCount() ];
        final long[] throughputs = new long[ history.getInputPortCount() ];

        for ( int i = 0; i < history.getOperatorCount(); i++ )
        {
            operatorCosts[ i ] = p.get( 0 ).getAvgOperatorCost( i );
        }

        for ( int i = 0; i < history.getInputPortCount(); i++ )
        {
            throughputs[ i ] = p.get( 0 ).getTotalInboundThroughput( i );
        }

        for ( int i = 1; i < p.size(); i++ )
        {
            final PipelineMetrics pipelineMetrics = p.get( i );

            cpuUtilRatio = ema( pipelineMetrics.getAvgCpuUtilizationRatio(), cpuUtilRatio );
            pipelineCost = ema( pipelineMetrics.getAvgPipelineCost(), pipelineCost );

            for ( int j = 0; j < history.getOperatorCount(); j++ )
            {
                operatorCosts[ j ] = ema( pipelineMetrics.getAvgOperatorCost( j ), operatorCosts[ j ] );
            }

            for ( int j = 0; j < history.getInputPortCount(); j++ )
            {
                throughputs[ j ] = ema( pipelineMetrics.getTotalInboundThroughput( j ), throughputs[ j ] );
            }
        }

        final PipelineMetricsBuilder builder = new PipelineMetricsBuilder( history.getPipelineId(),
                                                                           history.getFlowVersion(),
                                                                           1,
                                                                           history.getOperatorCount(),
                                                                           history.getInputPortCount() );

        builder.setCpuUtilizationRatio( 0, cpuUtilRatio ).setPipelineCost( 0, pipelineCost );

        for ( int i = 0; i < history.getOperatorCount(); i++ )
        {
            builder.setOperatorCost( 0, i, operatorCosts[ i ] );
        }

        for ( int i = 0; i < history.getInputPortCount(); i++ )
        {
            builder.setInboundThroughput( 0, i, throughputs[ i ] );
        }

        return builder.build();
    }

    private double ema ( final double y, final double prev )
    {
        return ALPHA * y + ( 1 - ALPHA ) * prev;
    }

    private long ema ( final long y, final long prev )
    {
        return (long) ( ( ALPHA * y ) + ( 1 - ALPHA ) * prev );
    }

}
