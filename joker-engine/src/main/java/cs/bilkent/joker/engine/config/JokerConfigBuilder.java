package cs.bilkent.joker.engine.config;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import static cs.bilkent.joker.engine.config.AdaptationConfig.ADAPTATION_ENABLED;
import static cs.bilkent.joker.engine.config.AdaptationConfig.CPU_UTILIZATION_BOTTLENECK_THRESHOLD;
import static cs.bilkent.joker.engine.config.AdaptationConfig.CPU_UTILIZATION_LOAD_CHANGE_THRESHOLD;
import static cs.bilkent.joker.engine.config.AdaptationConfig.LATENCY_METRICS_HISTORY_SUMMARIZER_CLASS;
import static cs.bilkent.joker.engine.config.AdaptationConfig.PIPELINE_METRICS_HISTORY_SUMMARIZER_CLASS;
import static cs.bilkent.joker.engine.config.AdaptationConfig.PIPELINE_SPLIT_ENABLED;
import static cs.bilkent.joker.engine.config.AdaptationConfig.REGION_REBALANCE_ENABLED;
import static cs.bilkent.joker.engine.config.AdaptationConfig.SPLIT_UTILITY;
import static cs.bilkent.joker.engine.config.AdaptationConfig.STABLE_PERIOD_COUNT_TO_STOP;
import static cs.bilkent.joker.engine.config.AdaptationConfig.THROUGHPUT_INCREASE_THRESHOLD;
import static cs.bilkent.joker.engine.config.AdaptationConfig.THROUGHPUT_LOAD_CHANGE_THRESHOLD;
import static cs.bilkent.joker.engine.config.AdaptationConfig.VISUALIZATION_ENABLED;
import static cs.bilkent.joker.engine.config.FlowDefOptimizerConfig.DUPLICATE_STATELESS_REGIONS;
import static cs.bilkent.joker.engine.config.FlowDefOptimizerConfig.MERGE_REGIONS;
import static cs.bilkent.joker.engine.config.JokerConfig.ENGINE_CONFIG_NAME;
import static cs.bilkent.joker.engine.config.MetricManagerConfig.CSV_REPORT_BASE_DIR;
import static cs.bilkent.joker.engine.config.MetricManagerConfig.CSV_REPORT_ENABLED;
import static cs.bilkent.joker.engine.config.MetricManagerConfig.CSV_REPORT_PERIOD_IN_MILLIS;
import static cs.bilkent.joker.engine.config.MetricManagerConfig.HISTORY_SIZE;
import static cs.bilkent.joker.engine.config.MetricManagerConfig.OPERATOR_INVOCATION_SAMPLING_IN_MICROS;
import static cs.bilkent.joker.engine.config.MetricManagerConfig.PERIOD_SKEW_TOLERANCE_RATIO;
import static cs.bilkent.joker.engine.config.MetricManagerConfig.PIPELINE_METRICS_SCANNING_PERIOD_IN_MILLIS;
import static cs.bilkent.joker.engine.config.MetricManagerConfig.TICK_MASK;
import static cs.bilkent.joker.engine.config.MetricManagerConfig.WARMUP_ITERATIONS;
import static cs.bilkent.joker.engine.config.PartitionServiceConfig.MAX_REPLICA_COUNT;
import static cs.bilkent.joker.engine.config.PartitionServiceConfig.PARTITION_COUNT;
import static cs.bilkent.joker.engine.config.PipelineManagerConfig.INTER_ARRIVAL_TIME_TRACKING_COUNT;
import static cs.bilkent.joker.engine.config.PipelineManagerConfig.INTER_ARRIVAL_TIME_TRACKING_PERIOD;
import static cs.bilkent.joker.engine.config.PipelineManagerConfig.LATENCY_STAGE_TICK_MASK;
import static cs.bilkent.joker.engine.config.PipelineManagerConfig.LATENCY_TICK_MASK;
import static cs.bilkent.joker.engine.config.PipelineManagerConfig.RUNNER_COMMAND_TIMEOUT;
import static cs.bilkent.joker.engine.config.PipelineReplicaRunnerConfig.ENFORCE_THREAD_AFFINITY;
import static cs.bilkent.joker.engine.config.PipelineReplicaRunnerConfig.RUNNER_WAIT_TIMEOUT;
import static cs.bilkent.joker.engine.config.TupleQueueDrainerConfig.MAX_BATCH_SIZE;
import static cs.bilkent.joker.engine.config.TupleQueueManagerConfig.TUPLE_QUEUE_CAPACITY;

public class JokerConfigBuilder
{


    private final Map<String, Object> adaptationConfigVals = new HashMap<>();

    private final Map<String, Object> flowDefOptimizerConfigVals = new HashMap<>();

    private final Map<String, Object> metricManagerConfigVals = new HashMap<>();

    private final Map<String, Object> partitionServiceConfigVals = new HashMap<>();

    private final Map<String, Object> pipelineManagerConfigVals = new HashMap<>();

    private final Map<String, Object> pipelineReplicaRunnerConfigVals = new HashMap<>();

    private final Map<String, Object> tupleQueueDrainerConfigVals = new HashMap<>();

    private final Map<String, Object> tupleQueueManagerConfigVals = new HashMap<>();

    private final AdaptationConfigBuilder adaptationConfigBuilder = new AdaptationConfigBuilder();

    private final FlowDefOptimizerConfigBuilder flowDefOptimizerConfigBuilder = new FlowDefOptimizerConfigBuilder();

    private final MetricManagerConfigBuilder metricManagerConfigBuilder = new MetricManagerConfigBuilder();

    private final PartitionServiceConfigBuilder partitionServiceConfigBuilder = new PartitionServiceConfigBuilder();

    private final PipelineManagerConfigBuilder pipelineManagerConfigBuilder = new PipelineManagerConfigBuilder();

    private final PipelineReplicaRunnerConfigBuilder pipelineReplicaRunnerConfigBuilder = new PipelineReplicaRunnerConfigBuilder();

    private final TupleQueueDrainerConfigBuilder tupleQueueDrainerConfigBuilder = new TupleQueueDrainerConfigBuilder();

    private final TupleQueueManagerConfigBuilder tupleQueueManagerConfigBuilder = new TupleQueueManagerConfigBuilder();

    public JokerConfig build ()
    {
        return new JokerConfig( buildInternal() );
    }

    public JokerConfig build ( final Config config )
    {
        return new JokerConfig( config.withFallback( buildInternal() ) );
    }

    private Config buildInternal ()
    {
        final Map<String, Object> allVals = new HashMap<>();
        accumulate( allVals, AdaptationConfig.CONFIG_NAME, adaptationConfigVals );
        accumulate( allVals, FlowDefOptimizerConfig.CONFIG_NAME, flowDefOptimizerConfigVals );
        accumulate( allVals, MetricManagerConfig.CONFIG_NAME, metricManagerConfigVals );
        accumulate( allVals, PartitionServiceConfig.CONFIG_NAME, partitionServiceConfigVals );
        accumulate( allVals, PipelineManagerConfig.CONFIG_NAME, pipelineManagerConfigVals );
        accumulate( allVals, PipelineReplicaRunnerConfig.CONFIG_NAME, pipelineReplicaRunnerConfigVals );
        accumulate( allVals, TupleQueueDrainerConfig.CONFIG_NAME, tupleQueueDrainerConfigVals );
        accumulate( allVals, TupleQueueManagerConfig.CONFIG_NAME, tupleQueueManagerConfigVals );

        return ConfigFactory.parseMap( allVals ).withFallback( ConfigFactory.load() );
    }

    private void accumulate ( final Map<String, Object> allVals, final String configName, final Map<String, Object> vals )
    {
        for ( Entry<String, Object> e : vals.entrySet() )
        {
            allVals.put( toKey( configName, e.getKey() ), e.getValue() );
        }
    }

    private String toKey ( final String configName, final String param )
    {
        return ENGINE_CONFIG_NAME + "." + configName + "." + param;
    }

    public AdaptationConfigBuilder getAdaptationConfigBuilder ()
    {
        return adaptationConfigBuilder;
    }

    public FlowDefOptimizerConfigBuilder getFlowDefOptimizerConfigBuilder ()
    {
        return flowDefOptimizerConfigBuilder;
    }

    public MetricManagerConfigBuilder getMetricManagerConfigBuilder ()
    {
        return metricManagerConfigBuilder;
    }

    public PartitionServiceConfigBuilder getPartitionServiceConfigBuilder ()
    {
        return partitionServiceConfigBuilder;
    }

    public PipelineManagerConfigBuilder getPipelineManagerConfigBuilder ()
    {
        return pipelineManagerConfigBuilder;
    }

    public PipelineReplicaRunnerConfigBuilder getPipelineReplicaRunnerConfigBuilder ()
    {
        return pipelineReplicaRunnerConfigBuilder;
    }

    public TupleQueueDrainerConfigBuilder getTupleQueueDrainerConfigBuilder ()
    {
        return tupleQueueDrainerConfigBuilder;
    }

    public TupleQueueManagerConfigBuilder getTupleQueueManagerConfigBuilder ()
    {
        return tupleQueueManagerConfigBuilder;
    }

    public class AdaptationConfigBuilder
    {

        private AdaptationConfigBuilder ()
        {
        }

        public AdaptationConfigBuilder enableAdaptation ()
        {
            adaptationConfigVals.put( ADAPTATION_ENABLED, true );

            return this;
        }

        public AdaptationConfigBuilder disableAdaptation ()
        {
            adaptationConfigVals.put( ADAPTATION_ENABLED, false );

            return this;
        }

        public AdaptationConfigBuilder enablePipelineSplit ()
        {
            adaptationConfigVals.put( PIPELINE_SPLIT_ENABLED, true );

            return this;
        }

        public AdaptationConfigBuilder disablePipelineSplit ()
        {
            adaptationConfigVals.put( PIPELINE_SPLIT_ENABLED, false );

            return this;
        }

        public AdaptationConfigBuilder enableRegionRebalance ()
        {
            adaptationConfigVals.put( REGION_REBALANCE_ENABLED, true );

            return this;
        }

        public AdaptationConfigBuilder disableRegionRebalance ()
        {
            adaptationConfigVals.put( REGION_REBALANCE_ENABLED, false );

            return this;
        }

        public AdaptationConfigBuilder enablePipelineSplitFirst ()
        {
            adaptationConfigVals.put( AdaptationConfig.PIPELINE_SPLIT_FIRST, true );

            return this;
        }

        public AdaptationConfigBuilder disablePipelineSplitFirst ()
        {
            adaptationConfigVals.put( AdaptationConfig.PIPELINE_SPLIT_FIRST, false );

            return this;
        }

        public AdaptationConfigBuilder enableVisualization ()
        {
            adaptationConfigVals.put( VISUALIZATION_ENABLED, true );

            return this;
        }

        public AdaptationConfigBuilder disableVisualization ()
        {
            adaptationConfigVals.put( VISUALIZATION_ENABLED, false );

            return this;
        }

        public AdaptationConfigBuilder setPipelineMetricsHistorySummarizerClass ( final String val )
        {
            adaptationConfigVals.put( PIPELINE_METRICS_HISTORY_SUMMARIZER_CLASS, val );

            return this;
        }

        public AdaptationConfigBuilder setLatencyMetricsHistorySummarizerClass ( final String val )
        {
            adaptationConfigVals.put( LATENCY_METRICS_HISTORY_SUMMARIZER_CLASS, val );

            return this;
        }

        public AdaptationConfigBuilder setCpuUtilBottleneckThreshold ( final double val )
        {
            adaptationConfigVals.put( CPU_UTILIZATION_BOTTLENECK_THRESHOLD, val );

            return this;
        }

        public AdaptationConfigBuilder setCpuUtilLoadChangeThreshold ( final double val )
        {
            adaptationConfigVals.put( CPU_UTILIZATION_LOAD_CHANGE_THRESHOLD, val );

            return this;
        }

        public AdaptationConfigBuilder setThroughputLoadChangeThreshold ( final double val )
        {
            adaptationConfigVals.put( THROUGHPUT_LOAD_CHANGE_THRESHOLD, val );

            return this;
        }

        public AdaptationConfigBuilder setThroughputIncreaseThreshold ( final double val )
        {
            adaptationConfigVals.put( THROUGHPUT_INCREASE_THRESHOLD, val );

            return this;
        }

        public AdaptationConfigBuilder setSplitUtility ( final double val )
        {
            adaptationConfigVals.put( SPLIT_UTILITY, val );

            return this;
        }

        public AdaptationConfigBuilder setStablePeriodCountToStop ( final int stablePeriodCountToStop )
        {
            adaptationConfigVals.put( STABLE_PERIOD_COUNT_TO_STOP, stablePeriodCountToStop );

            return this;
        }

        public AdaptationConfigBuilder setLatencyThresholdNanos ( final long latencyThresholdNanos )
        {
            adaptationConfigVals.put( AdaptationConfig.LATENCY_THRESHOLD_NANOS, latencyThresholdNanos );

            return this;
        }

    }


    public class FlowDefOptimizerConfigBuilder
    {

        private FlowDefOptimizerConfigBuilder ()
        {
        }

        public FlowDefOptimizerConfigBuilder enableDuplicateStatelessRegions ()
        {
            flowDefOptimizerConfigVals.put( DUPLICATE_STATELESS_REGIONS, true );

            return this;
        }

        public FlowDefOptimizerConfigBuilder disableDuplicateStatelessRegions ()
        {
            flowDefOptimizerConfigVals.put( DUPLICATE_STATELESS_REGIONS, false );

            return this;
        }

        public FlowDefOptimizerConfigBuilder enableMergeRegions ()
        {
            flowDefOptimizerConfigVals.put( MERGE_REGIONS, true );

            return this;
        }

        public FlowDefOptimizerConfigBuilder disableMergeRegions ()
        {
            flowDefOptimizerConfigVals.put( MERGE_REGIONS, false );

            return this;
        }

    }


    public class MetricManagerConfigBuilder
    {

        private MetricManagerConfigBuilder ()
        {
        }

        public MetricManagerConfigBuilder setTickMask ( final long val )
        {
            metricManagerConfigVals.put( TICK_MASK, val );

            return this;
        }

        public MetricManagerConfigBuilder setWarmupIterations ( final int val )
        {
            metricManagerConfigVals.put( WARMUP_ITERATIONS, val );

            return this;
        }

        public MetricManagerConfigBuilder setPipelineMetricsScanningPeriodInMillis ( final long val )
        {
            metricManagerConfigVals.put( PIPELINE_METRICS_SCANNING_PERIOD_IN_MILLIS, val );

            return this;
        }

        public MetricManagerConfigBuilder setOperatorInvocationSamplingPeriodInMicros ( final long val )
        {
            metricManagerConfigVals.put( OPERATOR_INVOCATION_SAMPLING_IN_MICROS, val );

            return this;
        }

        public MetricManagerConfigBuilder setHistorySize ( final int val )
        {
            metricManagerConfigVals.put( HISTORY_SIZE, val );

            return this;
        }

        public MetricManagerConfigBuilder setPeriodSkewToleranceRatio ( final double val )
        {
            metricManagerConfigVals.put( PERIOD_SKEW_TOLERANCE_RATIO, val );

            return this;
        }

        public MetricManagerConfigBuilder enableCsvReport ()
        {
            metricManagerConfigVals.put( CSV_REPORT_ENABLED, true );

            return this;
        }

        public MetricManagerConfigBuilder disableCsvReport ()
        {
            metricManagerConfigVals.put( CSV_REPORT_ENABLED, false );

            return this;
        }

        public MetricManagerConfigBuilder setCsvReportPeriodInMillis ( final long val )
        {
            metricManagerConfigVals.put( CSV_REPORT_PERIOD_IN_MILLIS, val );

            return this;
        }

        public MetricManagerConfigBuilder setCsvReportBaseDir ( final String val )
        {
            metricManagerConfigVals.put( CSV_REPORT_BASE_DIR, val );

            return this;
        }

    }


    public class PartitionServiceConfigBuilder
    {

        private PartitionServiceConfigBuilder ()
        {
        }

        public PartitionServiceConfigBuilder setPartitionCount ( final int val )
        {
            partitionServiceConfigVals.put( PARTITION_COUNT, val );

            return this;
        }

        public PartitionServiceConfigBuilder setMaxReplicaCount ( final int val )
        {
            partitionServiceConfigVals.put( MAX_REPLICA_COUNT, val );

            return this;
        }

    }


    public class PipelineManagerConfigBuilder
    {

        private PipelineManagerConfigBuilder ()
        {
        }

        public PipelineManagerConfigBuilder setRunnerCommandTimeoutInMillis ( final long val )
        {
            pipelineManagerConfigVals.put( RUNNER_COMMAND_TIMEOUT, val );

            return this;
        }


        public PipelineManagerConfigBuilder setLatencyTickMask ( final long val )
        {
            pipelineManagerConfigVals.put( LATENCY_TICK_MASK, val );

            return this;
        }


        public PipelineManagerConfigBuilder setLatencyStageTickMask ( final long val )
        {
            pipelineManagerConfigVals.put( LATENCY_STAGE_TICK_MASK, val );

            return this;
        }

        public PipelineManagerConfigBuilder setInterArrivalTimeTrackingPeriod ( final int val )
        {
            pipelineManagerConfigVals.put( INTER_ARRIVAL_TIME_TRACKING_PERIOD, val );

            return this;
        }

        public PipelineManagerConfigBuilder setInterArrivalTimeTrackingCount ( final int val )
        {
            pipelineManagerConfigVals.put( INTER_ARRIVAL_TIME_TRACKING_COUNT, val );

            return this;
        }

    }


    public class PipelineReplicaRunnerConfigBuilder
    {

        private PipelineReplicaRunnerConfigBuilder ()
        {
        }

        public PipelineReplicaRunnerConfigBuilder setRunnerWaitTimeoutInMillis ( final long val )
        {
            pipelineReplicaRunnerConfigVals.put( RUNNER_WAIT_TIMEOUT, val );

            return this;
        }

        public PipelineReplicaRunnerConfigBuilder enforceThreadAffinity ( final boolean val )
        {
            pipelineReplicaRunnerConfigVals.put( ENFORCE_THREAD_AFFINITY, val );

            return this;
        }

    }


    public class TupleQueueDrainerConfigBuilder
    {

        private TupleQueueDrainerConfigBuilder ()
        {
        }

        public TupleQueueDrainerConfigBuilder setMaxBatchSize ( final int val )
        {
            tupleQueueDrainerConfigVals.put( MAX_BATCH_SIZE, val );

            return this;
        }

    }


    public class TupleQueueManagerConfigBuilder
    {

        private TupleQueueManagerConfigBuilder ()
        {
        }

        public TupleQueueManagerConfigBuilder setTupleQueueCapacity ( final int val )
        {
            tupleQueueManagerConfigVals.put( TUPLE_QUEUE_CAPACITY, val );

            return this;
        }

        public TupleQueueManagerConfigBuilder setMultiThreadedQueueDrainLimit ( final int val )
        {
            tupleQueueManagerConfigVals.put( TupleQueueManagerConfig.MULTI_THREADED_QUEUE_DRAIN_LIMIT, val );

            return this;
        }

    }

}
