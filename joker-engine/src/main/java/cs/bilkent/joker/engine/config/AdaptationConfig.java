package cs.bilkent.joker.engine.config;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Predicate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.typesafe.config.Config;

import cs.bilkent.joker.engine.adaptation.PipelineMetricsHistorySummarizer;
import cs.bilkent.joker.engine.flow.PipelineId;
import cs.bilkent.joker.engine.flow.RegionExecutionPlan;
import cs.bilkent.joker.engine.metric.PipelineMetrics;
import static cs.bilkent.joker.impl.com.google.common.base.Preconditions.checkState;
import static java.lang.Math.abs;
import static java.lang.Math.max;
import static java.lang.Math.min;

public class AdaptationConfig
{

    private static final Logger LOGGER = LoggerFactory.getLogger( AdaptationConfig.class );

    static final String CONFIG_NAME = "adaptation";

    static final String ENABLED = "enabled";

    static final String PIPELINE_METRICS_HISTORY_SUMMARIZER_CLASS = "pipelineMetricsHistorySummarizerClass";

    static final String CPU_UTILIZATION_BOTTLENECK_THRESHOLD = "cpuUtilBottleneckThreshold";

    static final String CPU_UTILIZATION_LOAD_CHANGE_THRESHOLD = "cpuUtilLoadChangeThreshold";

    static final String THROUGHPUT_LOAD_CHANGE_THRESHOLD = "throughputLoadChangeThreshold";

    static final String THROUGHPUT_INCREASE_THRESHOLD = "throughputIncreaseThreshold";

    static final String SPLIT_UTILITY = "splitUtility";


    private final boolean enabled;

    private final Class<PipelineMetricsHistorySummarizer> pipelineMetricsHistorySummarizerClass;

    private final double cpuUtilBottleneckThreshold;

    private final double cpuUtilLoadChangeThreshold;

    private final double throughputLoadChangeThreshold;

    private final double throughputIncreaseThreshold;

    private final double splitUtility;


    AdaptationConfig ( final Config parentConfig )
    {
        final Config config = parentConfig.getConfig( CONFIG_NAME );

        this.enabled = config.getBoolean( ENABLED );
        final String className = config.getString( PIPELINE_METRICS_HISTORY_SUMMARIZER_CLASS );
        try
        {
            this.pipelineMetricsHistorySummarizerClass = (Class<PipelineMetricsHistorySummarizer>) Class.forName( className );
        }
        catch ( ClassNotFoundException e )
        {
            throw new RuntimeException( className + " not found!", e );
        }

        this.cpuUtilBottleneckThreshold = config.getDouble( CPU_UTILIZATION_BOTTLENECK_THRESHOLD );
        this.cpuUtilLoadChangeThreshold = config.getDouble( CPU_UTILIZATION_LOAD_CHANGE_THRESHOLD );
        this.throughputLoadChangeThreshold = config.getDouble( THROUGHPUT_LOAD_CHANGE_THRESHOLD );
        this.throughputIncreaseThreshold = config.getDouble( THROUGHPUT_INCREASE_THRESHOLD );
        this.splitUtility = config.getDouble( SPLIT_UTILITY );
    }

    public boolean isEnabled ()
    {
        return enabled;
    }

    public PipelineMetricsHistorySummarizer getPipelineMetricsHistorySummarizer ()
    {
        try
        {
            final Constructor<PipelineMetricsHistorySummarizer> constructor = pipelineMetricsHistorySummarizerClass.getConstructor();
            return constructor.newInstance();
        }
        catch ( NoSuchMethodException | IllegalAccessException | InstantiationException | InvocationTargetException e )
        {
            throw new RuntimeException( "cannot create instance of " + pipelineMetricsHistorySummarizerClass.getName(), e );
        }
    }

    public double getCpuUtilBottleneckThreshold ()
    {
        return cpuUtilBottleneckThreshold;
    }

    public double getCpuUtilLoadChangeThreshold ()
    {
        return cpuUtilLoadChangeThreshold;
    }

    public double getThroughputLoadChangeThreshold ()
    {
        return throughputLoadChangeThreshold;
    }

    public double getThroughputIncreaseThreshold ()
    {
        return throughputIncreaseThreshold;
    }

    public double getSplitUtility ()
    {
        return splitUtility;
    }

    public BiPredicate<PipelineMetrics, PipelineMetrics> getLoadChangePredicate ()
    {
        return ( oldMetrics, newMetrics ) ->
        {
            if ( oldMetrics == null )
            {
                return false;
            }

            final double avgCpuUtilRatio = oldMetrics.getAvgCpuUtilizationRatio();
            final double cpuUtilDiff = abs( newMetrics.getAvgCpuUtilizationRatio() - avgCpuUtilRatio );
            if ( ( cpuUtilDiff / avgCpuUtilRatio ) >= cpuUtilLoadChangeThreshold )
            {
                return true;
            }

            for ( int portIndex = 0; portIndex < oldMetrics.getInputPortCount(); portIndex++ )
            {
                final long throughput = oldMetrics.getTotalInboundThroughput( portIndex );
                final double throughputDiff = abs( newMetrics.getTotalInboundThroughput( portIndex ) - throughput );
                if ( ( throughputDiff / throughput ) >= throughputLoadChangeThreshold )
                {
                    return true;
                }
            }

            return false;
        };
    }

    public Predicate<PipelineMetrics> getBottleneckPredicate ()
    {
        return pipelineMetrics -> pipelineMetrics.getAvgCpuUtilizationRatio() >= cpuUtilBottleneckThreshold;
    }

    public BiFunction<RegionExecutionPlan, PipelineMetrics, Integer> getPipelineSplitIndexExtractor ()
    {
        return ( regionExecutionPlan, pipelineMetrics ) ->
        {
            final PipelineId pipelineId = pipelineMetrics.getPipelineId();
            final int operatorCount = regionExecutionPlan.getOperatorCountByPipelineStartIndex( pipelineId.getPipelineStartIndex() );
            checkState( operatorCount > 1 );

            final double pipelineCost = pipelineMetrics.getAvgPipelineCost();
            final double totalOperatorCost = max( 0, 1 - pipelineCost );
            final double[] thr = new double[ operatorCount - 1 ];

            double headUtility = min( 1, pipelineMetrics.getAvgOperatorCost( 0 ) );
            double tailUtility = max( 0, totalOperatorCost - pipelineMetrics.getAvgOperatorCost( 0 ) );
            thr[ 0 ] = min( 1 / ( pipelineCost + headUtility ), 1 / ( pipelineCost + tailUtility ) );

            int max = 0;

            for ( int i = 1; i < operatorCount - 1; i++ )
            {
                final double operatorCost = pipelineMetrics.getAvgOperatorCost( i );
                headUtility = min( 1, headUtility + operatorCost );
                tailUtility = max( 0, tailUtility - operatorCost );
                thr[ i ] = min( 1 / ( pipelineCost + headUtility ), 1 / ( pipelineCost + tailUtility ) );

                if ( thr[ i ] > thr[ max ] )
                {
                    max = i;
                }
            }

            if ( ( thr[ max ] - 1 ) >= splitUtility )
            {
                LOGGER.info( "Pipeline {} can be split at index: {} with utility: {}", pipelineId, ( max + 1 ), thr[ max ] );
                return max + 1;
            }

            return 0;
        };
    }

    public BiPredicate<PipelineMetrics, PipelineMetrics> getAdaptationEvaluationPredicate ()
    {
        return ( oldMetrics, newMetrics ) ->
        {
            for ( int portIndex = 0; portIndex < oldMetrics.getInputPortCount(); portIndex++ )
            {
                final long throughput = oldMetrics.getTotalInboundThroughput( portIndex );
                if ( ( (double) newMetrics.getTotalInboundThroughput( portIndex ) - throughput ) / throughput
                     >= throughputIncreaseThreshold )
                {
                    return true;
                }
            }

            return false;
        };
    }

    @Override
    public String toString ()
    {
        return "AdaptationConfig{" + "pipelineMetricsHistorySummarizerClass=" + pipelineMetricsHistorySummarizerClass
               + ", cpuUtilBottleneckThreshold=" + cpuUtilBottleneckThreshold + ", cpuUtilLoadChangeThreshold=" + cpuUtilLoadChangeThreshold
               + ", throughputLoadChangeThreshold=" + throughputLoadChangeThreshold + ", throughputIncreaseThreshold="
               + throughputIncreaseThreshold + ", splitUtility=" + splitUtility + '}';
    }

}
