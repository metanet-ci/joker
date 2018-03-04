package cs.bilkent.joker.engine.config;

import org.junit.Test;

import cs.bilkent.joker.engine.metric.impl.pipelinemetricshistorysummarizer.LatestPipelineMetrics;
import cs.bilkent.joker.test.AbstractJokerTest;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class JokerConfigBuilderTest extends AbstractJokerTest
{

    private final JokerConfigBuilder builder = new JokerConfigBuilder();


    @Test
    public void test_AdaptationConfig_adaptationEnabled ()
    {
        builder.getAdaptationConfigBuilder().enableAdaptation();

        assertTrue( builder.build().getAdaptationConfig().isAdaptationEnabled() );
    }

    @Test
    public void test_AdaptationConfig_adaptationDisabled ()
    {
        builder.getAdaptationConfigBuilder().disableAdaptation();

        assertFalse( builder.build().getAdaptationConfig().isAdaptationEnabled() );
    }

    @Test
    public void test_AdaptationConfig_pipelineSplitEnabled ()
    {
        builder.getAdaptationConfigBuilder().enablePipelineSplit();

        assertTrue( builder.build().getAdaptationConfig().isPipelineSplitEnabled() );
    }

    @Test
    public void test_AdaptationConfig_pipelineSplitDisabled ()
    {
        builder.getAdaptationConfigBuilder().disablePipelineSplit();

        assertFalse( builder.build().getAdaptationConfig().isPipelineSplitEnabled() );
    }

    @Test
    public void test_AdaptationConfig_regionRebalanceEnabled ()
    {
        builder.getAdaptationConfigBuilder().enableRegionRebalance();

        assertTrue( builder.build().getAdaptationConfig().isRegionRebalanceEnabled() );
    }

    @Test
    public void test_AdaptationConfig_regionRebalanceDisabled ()
    {
        builder.getAdaptationConfigBuilder().disableRegionRebalance();

        assertFalse( builder.build().getAdaptationConfig().isRegionRebalanceEnabled() );
    }

    @Test
    public void test_AdaptationConfig_enablePipelineSplitFirst ()
    {
        builder.getAdaptationConfigBuilder().enablePipelineSplitFirst();

        assertTrue( builder.build().getAdaptationConfig().isPipelineSplitFirst() );
    }

    @Test
    public void test_AdaptationConfig_disablePipelineSplitFirst ()
    {
        builder.getAdaptationConfigBuilder().disablePipelineSplitFirst();

        assertFalse( builder.build().getAdaptationConfig().isPipelineSplitFirst() );
    }

    @Test
    public void test_AdaptationConfig_visualizationDisabled ()
    {
        builder.getAdaptationConfigBuilder().disableVisualization();

        assertFalse( builder.build().getAdaptationConfig().isVisualizationEnabled() );
    }

    @Test
    public void test_AdaptationConfig_visualizationEnabled ()
    {
        builder.getAdaptationConfigBuilder().enableVisualization();

        assertTrue( builder.build().getAdaptationConfig().isVisualizationEnabled() );
    }

    @Test
    public void test_AdaptationConfig_pipelineMetricsHistorySummarizerClass ()
    {
        builder.getAdaptationConfigBuilder().setPipelineMetricsHistorySummarizerClass( LatestPipelineMetrics.class.getName() );

        assertTrue( builder.build().getAdaptationConfig().getPipelineMetricsHistorySummarizer() instanceof LatestPipelineMetrics );
    }

    @Test
    public void test_AdaptationConfig_cpuUtilBottleneckThreshold ()
    {
        final double val = 0.85;
        builder.getAdaptationConfigBuilder().setCpuUtilBottleneckThreshold( val );

        assertEquals( val, builder.build().getAdaptationConfig().getCpuUtilBottleneckThreshold(), 0.01 );
    }

    @Test
    public void test_AdaptationConfig_cpuUtilLoadChangeThreshold ()
    {
        final double val = 0.85;
        builder.getAdaptationConfigBuilder().setCpuUtilLoadChangeThreshold( val );

        assertEquals( val, builder.build().getAdaptationConfig().getCpuUtilLoadChangeThreshold(), 0.01 );
    }

    @Test
    public void test_AdaptationConfig_throughputLoadChangeThreshold ()
    {
        final double val = 0.85;
        builder.getAdaptationConfigBuilder().setThroughputLoadChangeThreshold( val );

        assertEquals( val, builder.build().getAdaptationConfig().getThroughputLoadChangeThreshold(), 0.01 );
    }

    @Test
    public void test_AdaptationConfig_throughputIncreaseThreshold ()
    {
        final double val = 0.85;
        builder.getAdaptationConfigBuilder().setThroughputIncreaseThreshold( val );

        assertEquals( val, builder.build().getAdaptationConfig().getThroughputIncreaseThreshold(), 0.01 );
    }

    @Test
    public void test_AdaptationConfig_splitUtility ()
    {
        final double val = 0.85;
        builder.getAdaptationConfigBuilder().setSplitUtility( val );

        assertEquals( val, builder.build().getAdaptationConfig().getSplitUtility(), 0.01 );
    }

    @Test
    public void test_AdaptationConfig_stablePeriodCountToStop ()
    {
        final int val = 20;
        builder.getAdaptationConfigBuilder().setStablePeriodCountToStop( val );

        assertEquals( val, builder.build().getAdaptationConfig().getStablePeriodCountToStop() );
    }

    @Test
    public void test_FlowDefOptimizerConfig_duplicateStatelessRegions_enabled ()
    {
        builder.getFlowDefOptimizerConfigBuilder().enableDuplicateStatelessRegions();

        assertTrue( builder.build().getFlowDefOptimizerConfig().isDuplicateStatelessRegionsEnabled() );
    }

    @Test
    public void test_FlowDefOptimizerConfig_duplicateStatelessRegions_disabled ()
    {
        builder.getFlowDefOptimizerConfigBuilder().disableDuplicateStatelessRegions();

        assertFalse( builder.build().getFlowDefOptimizerConfig().isDuplicateStatelessRegionsEnabled() );
    }

    @Test
    public void test_FlowDefOptimizerConfig_mergeRegionsEnabled ()
    {
        builder.getFlowDefOptimizerConfigBuilder().enableMergeRegions();

        assertTrue( builder.build().getFlowDefOptimizerConfig().isMergeRegionsEnabled() );
    }

    @Test
    public void test_FlowDefOptimizerConfig_mergeRegionsDisabled ()
    {
        builder.getFlowDefOptimizerConfigBuilder().disableMergeRegions();

        assertFalse( builder.build().getFlowDefOptimizerConfig().isMergeRegionsEnabled() );
    }

    @Test
    public void test_MetricManagerConfig_tickMask ()
    {
        final long val = 234;
        builder.getMetricManagerConfigBuilder().setTickMask( val );

        assertEquals( val, builder.build().getMetricManagerConfig().getTickMask() );
    }

    @Test
    public void test_MetricManagerConfig_warmupIterations ()
    {
        final int val = 234;
        builder.getMetricManagerConfigBuilder().setWarmupIterations( val );

        assertEquals( val, builder.build().getMetricManagerConfig().getWarmupIterations() );
    }

    @Test
    public void test_MetricManagerConfig_pipelineMetricsScanningPeriodInMillis ()
    {
        final long val = 234;
        builder.getMetricManagerConfigBuilder().setPipelineMetricsScanningPeriodInMillis( val );

        assertEquals( val, builder.build().getMetricManagerConfig().getPipelineMetricsScanningPeriodInMillis() );
    }

    @Test
    public void test_MetricManagerConfig_operatorInvocationSamplingPeriodInMicros ()
    {
        final long val = 234;
        builder.getMetricManagerConfigBuilder().setOperatorInvocationSamplingPeriodInMicros( val );

        assertEquals( val, builder.build().getMetricManagerConfig().getOperatorInvocationSamplingPeriodInMicros() );
    }

    @Test
    public void test_MetricManagerConfig_historySize ()
    {
        final int val = 234;
        builder.getMetricManagerConfigBuilder().setHistorySize( val );

        assertEquals( val, builder.build().getMetricManagerConfig().getHistorySize() );
    }

    @Test
    public void test_MetricManagerConfig_periodSkewToleranceRatio ()
    {
        final double val = 0.85;
        builder.getMetricManagerConfigBuilder().setPeriodSkewToleranceRatio( val );

        assertEquals( val, builder.build().getMetricManagerConfig().getPeriodSkewToleranceRatio(), 0.01 );
    }

    @Test
    public void test_MetricManagerConfig_csvReport_enabled ()
    {
        builder.getMetricManagerConfigBuilder().enableCsvReport();

        assertTrue( builder.build().getMetricManagerConfig().isCsvReportEnabled() );
    }

    @Test
    public void test_MetricManagerConfig_csvReport_disabled ()
    {
        builder.getMetricManagerConfigBuilder().disableCsvReport();

        assertFalse( builder.build().getMetricManagerConfig().isCsvReportEnabled() );
    }

    @Test
    public void test_MetricManagerConfig_csvReportPeriodInMillis ()
    {
        final long val = 234;
        builder.getMetricManagerConfigBuilder().setCsvReportPeriodInMillis( val );

        assertEquals( val, builder.build().getMetricManagerConfig().getCsvReportPeriodInMillis() );
    }

    @Test
    public void test_MetricManagerConfig_csvReportBaseDir ()
    {
        final String dir = "dir1/";
        builder.getMetricManagerConfigBuilder().setCsvReportBaseDir( dir );

        assertEquals( dir, builder.build().getMetricManagerConfig().getCsvReportBaseDir() );
    }

    @Test
    public void test_PartitionServiceConfig_partitionCount ()
    {
        final int val = 234;
        builder.getPartitionServiceConfigBuilder().setPartitionCount( val );

        assertEquals( val, builder.build().getPartitionServiceConfig().getPartitionCount() );
    }

    @Test
    public void test_PartitionServiceConfig_maxReplicaCount ()
    {
        final int val = 234;
        builder.getPartitionServiceConfigBuilder().setMaxReplicaCount( val );

        assertEquals( val, builder.build().getPartitionServiceConfig().getMaxReplicaCount() );
    }

    @Test
    public void test_PipelineManagerConfig_runnerCommandTimeoutInMillis ()
    {
        final long val = 234;
        builder.getPipelineManagerConfigBuilder().setRunnerCommandTimeoutInMillis( val );

        assertEquals( val, builder.build().getPipelineManagerConfig().getRunnerCommandTimeoutInMillis() );
    }

    @Test
    public void test_PipelineReplicaRunnerConfig_runnerWaitTimeoutInMillis ()
    {
        final long val = 234;
        builder.getPipelineReplicaRunnerConfigBuilder().setRunnerWaitTimeoutInMillis( val );

        assertEquals( val, builder.build().getPipelineReplicaRunnerConfig().getRunnerWaitTimeoutInMillis() );
    }

    @Test
    public void test_TupleQueueDrainerConfig_maxBatchSize ()
    {
        final int val = 234;
        builder.getTupleQueueDrainerConfigBuilder().setMaxBatchSize( val );

        assertEquals( val, builder.build().getTupleQueueDrainerConfig().getMaxBatchSize() );
    }

    @Test
    public void test_TupleQueueManagerConfig_tupleQueueCapacity ()
    {
        final int val = 234;
        builder.getTupleQueueManagerConfigBuilder().setTupleQueueCapacity( val );

        assertEquals( val, builder.build().getTupleQueueManagerConfig().getTupleQueueCapacity() );
    }

    @Test
    public void test_TupleQueueManagerConfig_multiThreadedQueueDrainLimit ()
    {
        final int val = 234;
        builder.getTupleQueueManagerConfigBuilder().setMultiThreadedQueueDrainLimit( val );

        assertEquals( val, builder.build().getTupleQueueManagerConfig().getMultiThreadedQueueDrainLimit() );
    }

}
