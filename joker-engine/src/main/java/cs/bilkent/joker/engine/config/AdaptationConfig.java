package cs.bilkent.joker.engine.config;

import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Predicate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.typesafe.config.Config;

import cs.bilkent.joker.engine.flow.PipelineId;
import cs.bilkent.joker.engine.flow.RegionExecPlan;
import cs.bilkent.joker.engine.metric.LatencyMetricsHistorySummarizer;
import cs.bilkent.joker.engine.metric.PipelineMetrics;
import cs.bilkent.joker.engine.metric.PipelineMetricsHistorySummarizer;
import static cs.bilkent.joker.impl.com.google.common.base.Preconditions.checkState;
import static java.lang.Math.abs;
import static java.lang.Math.max;
import static java.lang.Math.min;

public class AdaptationConfig
{

    private static final Logger LOGGER = LoggerFactory.getLogger( AdaptationConfig.class );

    static final String CONFIG_NAME = "adaptation";

    static final String ADAPTATION_ENABLED = "adaptationEnabled";

    static final String PIPELINE_SPLIT_ENABLED = "pipelineSplitEnabled";

    static final String REGION_REBALANCE_ENABLED = "regionRebalanceEnabled";

    static final String PIPELINE_SPLIT_FIRST = "pipelineSplitFirst";

    static final String VISUALIZATION_ENABLED = "visualizationEnabled";

    static final String PIPELINE_METRICS_HISTORY_SUMMARIZER_CLASS = "pipelineMetricsHistorySummarizerClass";

    static final String LATENCY_METRICS_HISTORY_SUMMARIZER_CLASS = "latencyMetricsHistorySummarizerClass";

    static final String CPU_UTILIZATION_BOTTLENECK_THRESHOLD = "cpuUtilBottleneckThreshold";

    static final String CPU_UTILIZATION_LOAD_CHANGE_THRESHOLD = "cpuUtilLoadChangeThreshold";

    static final String THROUGHPUT_LOAD_CHANGE_THRESHOLD = "throughputLoadChangeThreshold";

    static final String THROUGHPUT_INCREASE_THRESHOLD = "throughputIncreaseThreshold";

    static final String SPLIT_UTILITY = "splitUtility";

    static final String STABLE_PERIOD_COUNT_TO_STOP = "stablePeriodCountToStop";

    static final String LATENCY_THRESHOLD_NANOS = "latencyThresholdNanos";


    private final boolean adaptationEnabled;

    private final boolean pipelineSplitEnabled;

    private final boolean regionRebalanceEnabled;

    private final boolean pipelineSplitFirst;

    private final boolean visualizationEnabled;

    private final Class<PipelineMetricsHistorySummarizer> pipelineMetricsHistorySummarizerClass;

    private final Class<LatencyMetricsHistorySummarizer> latencyMetricsHistorySummarizerClass;

    private final double cpuUtilBottleneckThreshold;

    private final double cpuUtilLoadChangeThreshold;

    private final double throughputLoadChangeThreshold;

    private final double throughputIncreaseThreshold;

    private final double splitUtility;

    private final int stablePeriodCountToStop;

    private final long latencyThresholdNanos;


    AdaptationConfig ( final Config parentConfig )
    {
        final Config config = parentConfig.getConfig( CONFIG_NAME );

        this.adaptationEnabled = config.getBoolean( ADAPTATION_ENABLED );
        this.pipelineSplitEnabled = config.getBoolean( PIPELINE_SPLIT_ENABLED );
        this.regionRebalanceEnabled = config.getBoolean( REGION_REBALANCE_ENABLED );
        this.pipelineSplitFirst = config.getBoolean( PIPELINE_SPLIT_FIRST );
        this.visualizationEnabled = config.getBoolean( VISUALIZATION_ENABLED );
        this.pipelineMetricsHistorySummarizerClass = getClass( config.getString( PIPELINE_METRICS_HISTORY_SUMMARIZER_CLASS ) );
        this.latencyMetricsHistorySummarizerClass = getClass( config.getString( LATENCY_METRICS_HISTORY_SUMMARIZER_CLASS ) );
        this.cpuUtilBottleneckThreshold = config.getDouble( CPU_UTILIZATION_BOTTLENECK_THRESHOLD );
        this.cpuUtilLoadChangeThreshold = config.getDouble( CPU_UTILIZATION_LOAD_CHANGE_THRESHOLD );
        this.throughputLoadChangeThreshold = config.getDouble( THROUGHPUT_LOAD_CHANGE_THRESHOLD );
        this.throughputIncreaseThreshold = config.getDouble( THROUGHPUT_INCREASE_THRESHOLD );
        this.splitUtility = config.getDouble( SPLIT_UTILITY );
        this.stablePeriodCountToStop = config.getInt( STABLE_PERIOD_COUNT_TO_STOP );
        this.latencyThresholdNanos = config.getLong( LATENCY_THRESHOLD_NANOS );
    }

    private <T> Class<T> getClass ( final String className )
    {
        try
        {
            return (Class<T>) Class.forName( className );
        }
        catch ( ClassNotFoundException e )
        {
            throw new RuntimeException( className + " not found!", e );
        }
    }

    public boolean isAdaptationEnabled ()
    {
        return adaptationEnabled;
    }

    public boolean isPipelineSplitEnabled ()
    {
        return pipelineSplitEnabled;
    }

    public boolean isRegionRebalanceEnabled ()
    {
        return regionRebalanceEnabled;
    }

    public boolean isPipelineSplitFirst ()
    {
        return pipelineSplitFirst;
    }

    public boolean isVisualizationEnabled ()
    {
        return visualizationEnabled;
    }

    public PipelineMetricsHistorySummarizer getPipelineMetricsHistorySummarizer ()
    {
        return createInstance( pipelineMetricsHistorySummarizerClass );
    }

    public LatencyMetricsHistorySummarizer getLatencyMetricsHistorySummarizer ()
    {
        return createInstance( latencyMetricsHistorySummarizerClass );
    }

    public Class<PipelineMetricsHistorySummarizer> getPipelineMetricsHistorySummarizerClass ()
    {
        return pipelineMetricsHistorySummarizerClass;
    }

    public Class<LatencyMetricsHistorySummarizer> getLatencyMetricsHistorySummarizerClass ()
    {
        return latencyMetricsHistorySummarizerClass;
    }

    private <T> T createInstance ( Class clazz )
    {
        try
        {
            return (T) clazz.getConstructor().newInstance();
        }
        catch ( NoSuchMethodException | IllegalAccessException | InstantiationException | InvocationTargetException e )
        {
            throw new RuntimeException( "cannot create instance of " + clazz.getName(), e );
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

    public int getStablePeriodCountToStop ()
    {
        return stablePeriodCountToStop;
    }

    public long getLatencyThresholdNanos ()
    {
        return latencyThresholdNanos;
    }

    public BiPredicate<PipelineMetrics, PipelineMetrics> getLoadChangePredicate ()
    {
        return ( oldMetrics, newMetrics ) -> {
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

            for ( int portIndex = 0; portIndex < oldMetrics.getPortCount(); portIndex++ )
            {
                final long throughput = oldMetrics.getTotalThroughput( portIndex );
                final double throughputDiff = abs( newMetrics.getTotalThroughput( portIndex ) - throughput );
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

    public static void main ( String[] args )
    {
        double[] operatorCosts = new double[] { 0.49, 0.46 };
        double pipelineCost = 1 - Arrays.stream( operatorCosts ).sum();
        int operatorCount = operatorCosts.length;
        double splitUtility = 0.2;

        double totalOperatorCost = max( 0, 1 - pipelineCost );
        double[] thr = new double[ operatorCount - 1 ];

        double headUtility = min( 1, operatorCosts[ 0 ] );
        double tailUtility = max( 0, totalOperatorCost - operatorCosts[ 0 ] );
        thr[ 0 ] = min( 1 / ( pipelineCost + headUtility ), 1 / ( pipelineCost + tailUtility ) );
        System.out.println( "head util: " + headUtility + " tail util: " + tailUtility + " thr: " + thr[ 0 ] + " index: " + 0 );

        int max = 0;

        for ( int i = 1; i < operatorCount - 1; i++ )
        {
            final double operatorCost = operatorCosts[ i ];
            headUtility = min( 1, headUtility + operatorCost );
            tailUtility = max( 0, tailUtility - operatorCost );
            thr[ i ] = min( 1 / ( pipelineCost + headUtility ), 1 / ( pipelineCost + tailUtility ) );

            System.out.println( "head util: " + headUtility + " tail util: " + tailUtility + " thr: " + thr[ i ] + " index: " + 1 );

            if ( thr[ i ] > thr[ max ] )
            {
                max = i;
            }
        }

        if ( ( thr[ max ] - 1 ) >= splitUtility )
        {
            System.out.println( "SPLIT AT INDEX: " + ( max + 1 ) + " THR: " + thr[ max ] );
            return;
        }

        System.out.println( "NO SPLIT" );
    }

    public BiFunction<RegionExecPlan, PipelineMetrics, Integer> getPipelineSplitIndexExtractor ()
    {
        return ( execPlan, pipelineMetrics ) -> {
            final PipelineId pipelineId = pipelineMetrics.getPipelineId();
            final int operatorCount = execPlan.getOperatorCountByPipelineStartIndex( pipelineId.getPipelineStartIndex() );
            checkState( operatorCount > 1 );

            final double pipelineCost = pipelineMetrics.getAvgPipelineCost();
            final double totalOperatorCost = max( 0, 1 - pipelineCost );
            final double[] thr = new double[ operatorCount - 1 ];

            double headUtility = min( 1, pipelineMetrics.getAvgOperatorCost( 0 ) );
            double tailUtility = max( 0, totalOperatorCost - pipelineMetrics.getAvgOperatorCost( 0 ) );
            thr[ 0 ] = min( 1 / ( pipelineCost + headUtility ), 1 / ( pipelineCost + tailUtility ) );

            int max = 0;

            LOGGER.info( "Pipeline {} possible split at index: {} with utility: {}", pipelineId, ( 1 ), thr[ 0 ] );

            for ( int i = 1; i < operatorCount - 1; i++ )
            {
                final double operatorCost = pipelineMetrics.getAvgOperatorCost( i );
                headUtility = min( 1, headUtility + operatorCost );
                tailUtility = max( 0, tailUtility - operatorCost );
                thr[ i ] = min( 1 / ( pipelineCost + headUtility ), 1 / ( pipelineCost + tailUtility ) );

                LOGGER.info( "Pipeline {} possible split at index: {} with utility: {}", pipelineId, ( i + 1 ), thr[ i ] );

                if ( thr[ i ] > thr[ max ] )
                {
                    max = i;
                }
            }

            if ( ( thr[ max ] - 1 ) >= splitUtility )
            {
                LOGGER.info( "Pipeline {} will be split at index: {} with utility: {}", pipelineId, ( max + 1 ), thr[ max ] );
                return max + 1;
            }

            return 0;
        };
    }

    public BiPredicate<PipelineMetrics, PipelineMetrics> getAdaptationEvaluationPredicate ()
    {
        return ( oldMetrics, newMetrics ) -> {
            for ( int portIndex = 0; portIndex < oldMetrics.getPortCount(); portIndex++ )
            {
                final long bottleneckThroughput = oldMetrics.getTotalThroughput( portIndex );
                final long newThroughput = newMetrics.getTotalThroughput( portIndex );
                final double throughputIncrease = ( (double) ( newThroughput - bottleneckThroughput ) ) / bottleneckThroughput;
                LOGGER.info( "Pipeline: {} input port: {} bottleneck throughput: {} new throughput: {} change: {}",
                             newMetrics.getPipelineId(),
                             portIndex,
                             bottleneckThroughput,
                             newThroughput,
                             throughputIncrease );
                if ( throughputIncrease >= throughputIncreaseThreshold )
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
        return "AdaptationConfig{" + "adaptationEnabled=" + adaptationEnabled + ", pipelineSplitEnabled=" + pipelineSplitEnabled
               + ", regionRebalanceEnabled=" + regionRebalanceEnabled + ", pipelineSplitFirst=" + pipelineSplitFirst
               + ", visualizationEnabled=" + visualizationEnabled + ", pipelineMetricsHistorySummarizerClass="
               + pipelineMetricsHistorySummarizerClass + ", latencyMetricsHistorySummarizerClass=" + latencyMetricsHistorySummarizerClass
               + ", cpuUtilBottleneckThreshold=" + cpuUtilBottleneckThreshold + ", cpuUtilLoadChangeThreshold=" + cpuUtilLoadChangeThreshold
               + ", throughputLoadChangeThreshold=" + throughputLoadChangeThreshold + ", throughputIncreaseThreshold="
               + throughputIncreaseThreshold + ", splitUtility=" + splitUtility + ", stablePeriodCountToStop=" + stablePeriodCountToStop
               + ", latencyThresholdNanos=" + latencyThresholdNanos + '}';
    }

}
