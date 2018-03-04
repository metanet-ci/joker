package cs.bilkent.joker.engine.metric.impl;

import java.io.File;
import java.lang.management.OperatingSystemMXBean;
import java.lang.management.RuntimeMXBean;
import java.lang.management.ThreadMXBean;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Supplier;
import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.codahale.metrics.CsvReporter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Snapshot;
import com.google.common.base.Joiner;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import static com.google.common.base.Preconditions.checkState;
import cs.bilkent.joker.engine.config.JokerConfig;
import static cs.bilkent.joker.engine.config.JokerConfig.JOKER_THREAD_GROUP_NAME;
import cs.bilkent.joker.engine.config.MetricManagerConfig;
import cs.bilkent.joker.engine.exception.JokerException;
import cs.bilkent.joker.engine.flow.PipelineId;
import cs.bilkent.joker.engine.metric.FlowMetrics;
import cs.bilkent.joker.engine.metric.MetricManager;
import cs.bilkent.joker.engine.metric.PipelineMeter;
import cs.bilkent.joker.engine.metric.PipelineMetrics;
import cs.bilkent.joker.engine.metric.PipelineMetrics.PipelineMetricsVisitor;
import cs.bilkent.joker.engine.metric.PipelineMetricsHistory;
import cs.bilkent.joker.utils.Pair;
import static java.lang.Math.abs;
import static java.util.Collections.addAll;
import static java.util.Comparator.comparing;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

@Singleton
public class MetricManagerImpl implements MetricManager
{

    private static final Logger LOGGER = LoggerFactory.getLogger( MetricManagerImpl.class );

    private static final int METRICS_SCHEDULER_CORE_POOL_SIZE = 2;


    private enum TaskStatus
    {
        INITIAL, RUNNABLE, RUNNING, PAUSED
    }


    private final MetricManagerConfig metricManagerConfig;

    private final MetricRegistry metricRegistry;

    private final ThreadMXBean threadMXBean;

    private final RuntimeMXBean runtimeMXBean;

    private final OperatingSystemMXBean osMXBean;

    private final ScheduledExecutorService scheduler;

    private final int numberOfCores;

    private final Object monitor = new Object();

    private final Map<PipelineId, PipelineMetricsContext> pipelineMetricsContextMap = new ConcurrentHashMap<>();

    private final AtomicReference<TaskStatus> metricsFlag = new AtomicReference<>( TaskStatus.INITIAL );

    private final AtomicReference<TaskStatus> samplingFlag = new AtomicReference<>( TaskStatus.INITIAL );

    private volatile CsvReporter csvReporter;

    private volatile Histogram scanOperatorsHistogram;

    private volatile Histogram scanMetricsHistogram;

    private volatile FlowMetrics metrics;

    private volatile int iteration;

    private volatile boolean pause;

    @Inject
    public MetricManagerImpl ( final JokerConfig jokerConfig,
                               final MetricRegistry metricRegistry,
                               final ThreadMXBean threadMXBean,
                               final RuntimeMXBean runtimeMXBean,
                               final OperatingSystemMXBean osMXBean,
                               @Named( JOKER_THREAD_GROUP_NAME ) final ThreadGroup threadGroup )
    {
        this.metricManagerConfig = jokerConfig.getMetricManagerConfig();
        this.metricRegistry = metricRegistry;
        this.threadMXBean = threadMXBean;
        this.runtimeMXBean = runtimeMXBean;
        this.osMXBean = osMXBean;
        this.numberOfCores = osMXBean.getAvailableProcessors();
        final ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat( threadGroup.getName() + "_MetricsCollector-%d" )
                                                                      .build();
        this.scheduler = newScheduledThreadPool( METRICS_SCHEDULER_CORE_POOL_SIZE, threadFactory );
        this.scheduler.scheduleWithFixedDelay( new CollectPipelineMetrics(),
                                               0,
                                               metricManagerConfig.getPipelineMetricsScanningPeriodInMillis(),
                                               MILLISECONDS );
        this.scheduler.scheduleAtFixedRate( new SamplePipelines(),
                                            0,
                                            metricManagerConfig.getOperatorInvocationSamplingPeriodInMicros(),
                                            MICROSECONDS );
        this.scanOperatorsHistogram = metricRegistry.histogram( "scanOperators" );
        this.scanMetricsHistogram = metricRegistry.histogram( "scanMetrics" );
    }

    @Override
    public void start ( final int flowVersion, final List<PipelineMeter> pipelineMeters )
    {
        call( new StartTasks( flowVersion, pipelineMeters ), "starting" );
    }

    @Override
    public void pause ()
    {
        call( new PauseTasks(), "pausing" );
    }

    @Override
    public void update ( int flowVersion, List<PipelineId> pipelineIdsToRemove, List<PipelineMeter> newPipelineMeters )
    {
        call( new UpdatePipelineMeters( flowVersion, pipelineIdsToRemove, newPipelineMeters ), "updating" );
    }

    @Override
    public void resume ()
    {
        call( new ResumeTasks(), "pausing" );
    }

    @Override
    public FlowMetrics getMetrics ()
    {
        return metrics;
    }

    @Override
    public void shutdown ()
    {
        synchronized ( monitor )
        {
            if ( scheduler.isShutdown() )
            {
                return;
            }

            call( new ShutdownTasks(), "shutting down" );
            scheduler.shutdown();
            try
            {
                final boolean success = scheduler.awaitTermination( 30, SECONDS );
                if ( success )
                {
                    LOGGER.info( "Shutdown completed" );
                }
                else
                {
                    LOGGER.warn( "await termination timed out" );
                }
            }
            catch ( InterruptedException e )
            {
                Thread.currentThread().interrupt();
                LOGGER.error( "Interrupted while awaiting termination" );
            }
        }
    }

    private void call ( final Callable<Void> callable, final String command )
    {
        synchronized ( monitor )
        {
            try
            {
                final Future<Void> future = scheduler.submit( callable );
                future.get();
            }
            catch ( InterruptedException e )
            {
                Thread.currentThread().interrupt();
                throw new JokerException( "Interrupted while " + command + " the metric collector thread" );
            }
            catch ( ExecutionException e )
            {
                throw new JokerException( command + " the metric collector thread failed!", e.getCause() );
            }
            catch ( RejectedExecutionException e )
            {
                throw new JokerException( command + " failed because metric collector is already shut down!", e );
            }
        }
    }

    private void setTaskPaused ( final AtomicReference<TaskStatus> flag )
    {
        while ( !flag.compareAndSet( TaskStatus.RUNNABLE, TaskStatus.PAUSED ) )
        {
            if ( flag.get() == TaskStatus.PAUSED )
            {
                break;
            }

            LockSupport.parkNanos( 1 );
        }
    }

    private void createCsvReporter ()
    {
        if ( !metricManagerConfig.isCsvReportEnabled() )
        {
            return;
        }

        final String metricsDirName = new SimpleDateFormat( "'metrics_'yyyy_MM_dd_HH_mm_ss_SSS" ).format( new Date() );
        final String baseDir = metricManagerConfig.getCsvReportBaseDir();
        final String dir = baseDir + metricsDirName;
        final File directory = new File( dir );
        checkState( !directory.exists(), "Metrics dir %s already exists!", dir );
        final boolean dirCreated = directory.mkdir();
        checkState( dirCreated, "Metrics dir %s could not be created! Please make sure base dir: %s exists", dir, baseDir );
        LOGGER.info( "Metrics directory: {} is created", dir );
        csvReporter = CsvReporter.forRegistry( metricRegistry ).build( directory );
        csvReporter.start( metricManagerConfig.getCsvReportPeriodInMillis(), MILLISECONDS );
    }

    private void register ( final PipelineMetricsContext context )
    {
        if ( !metricManagerConfig.isCsvReportEnabled() )
        {
            return;
        }

        final PipelineMeter pipelineMeter = context.getPipelineMeter();
        final PipelineId pipelineId = pipelineMeter.getPipelineId();
        final int period = metrics != null ? metrics.getPeriod() + 1 : 0;

        for ( int replicaIndex = 0; replicaIndex < pipelineMeter.getReplicaCount(); replicaIndex++ )
        {
            final int r = replicaIndex;
            final String cpuMetricName = getMetricName( pipelineId, context.getFlowVersion(), replicaIndex, "cpu" );
            final Supplier<Double> cpuGauge = () -> {
                final PipelineMetrics latest = getLatestPipelineMetrics( pipelineId, period );
                return latest != null ? latest.getCpuUtilizationRatio( r ) : 0d;
            };
            metricRegistry.register( cpuMetricName, new PipelineGauge<>( pipelineId, cpuGauge ) );

            final String pipelineCostMetricName = getMetricName( pipelineId, context.getFlowVersion(), replicaIndex, "cost", "p" );
            final Supplier<Double> pipelineCostGauge = () -> {
                final PipelineMetrics latest = getLatestPipelineMetrics( pipelineId, period );
                return latest != null ? latest.getPipelineCost( r ) : 0d;
            };
            metricRegistry.register( pipelineCostMetricName, new PipelineGauge<>( pipelineId, pipelineCostGauge ) );

            for ( int operatorIndex = 0; operatorIndex < pipelineMeter.getOperatorCount(); operatorIndex++ )
            {
                final String operatorCostMetricName = getMetricName( pipelineId, context.getFlowVersion(),
                                                                     replicaIndex,
                                                                     "cost",
                                                                     "op",
                                                                     operatorIndex );
                final int o = operatorIndex;
                final Supplier<Double> operatorCostGauge = () -> {
                    final PipelineMetrics latest = getLatestPipelineMetrics( pipelineId, period );
                    return latest != null ? latest.getOperatorCost( r, o ) : 0d;
                };
                metricRegistry.register( operatorCostMetricName, new PipelineGauge<>( pipelineId, operatorCostGauge ) );
            }

            for ( int portIndex = 0; portIndex < pipelineMeter.getInputPortCount(); portIndex++ )
            {
                final String metricName = getMetricName( pipelineId, context.getFlowVersion(), replicaIndex, "thr", "cs", portIndex );
                final int p = portIndex;
                final Supplier<Long> throughputGauge = () -> {
                    final PipelineMetrics latest = getLatestPipelineMetrics( pipelineId, period );
                    return latest != null ? latest.getInboundThroughput( r, p ) : 0;
                };
                metricRegistry.register( metricName, new PipelineGauge<>( pipelineId, throughputGauge ) );
            }
        }
    }

    private void deregister ( final PipelineId pipelineId )
    {
        if ( !metricManagerConfig.isCsvReportEnabled() )
        {
            return;
        }

        metricRegistry.removeMatching( ( name, metric ) -> {
            if ( metric instanceof PipelineGauge )
            {
                final PipelineGauge gauge = (PipelineGauge) metric;
                final boolean match = gauge.getId().equals( pipelineId );
                if ( match )
                {
                    LOGGER.debug( "PipelineGauge: {} will be removed.", gauge.getId() );
                }
                return match;
            }

            return false;
        } );
    }

    private PipelineMetrics getLatestPipelineMetrics ( final PipelineId pipelineId, final int period )
    {
        if ( metrics == null || metrics.getPeriod() < period )
        {
            return null;
        }

        return metrics.getLatestPipelineMetrics( pipelineId );
    }

    private String getMetricName ( final PipelineId pipelineId, final int flowVersion, final int replicaIndex, Object... vals )
    {
        final List<Object> parts = new ArrayList<>();
        parts.add( "r" );
        parts.add( pipelineId.getRegionId() );
        parts.add( "p" );
        parts.add( pipelineId.getPipelineStartIndex() );
        parts.add( "r" );
        parts.add( replicaIndex );
        parts.add( "f" );
        parts.add( flowVersion );
        if ( vals != null )
        {
            addAll( parts, vals );
        }

        return Joiner.on( "_" ).join( parts );
    }

    private class SamplePipelines implements Runnable
    {

        @Override
        public void run ()
        {
            if ( pause || !samplingFlag.compareAndSet( TaskStatus.RUNNABLE, TaskStatus.RUNNING ) )
            {
                return;
            }

            try
            {
                if ( iteration <= metricManagerConfig.getWarmupIterations() )
                {
                    return;
                }

                final long samplingStartTimeInNanos = System.nanoTime();
                for ( PipelineMetricsContext context : pipelineMetricsContextMap.values() )
                {
                    context.sample( threadMXBean );
                }

                scanOperatorsHistogram.update( System.nanoTime() - samplingStartTimeInNanos );
            }
            catch ( Exception e )
            {
                LOGGER.error( "Sample pipelines failed", e );
            }
            finally
            {
                samplingFlag.set( TaskStatus.RUNNABLE );
            }
        }

    }


    private class CollectPipelineMetrics implements Runnable
    {

        long lastSystemNanoTime;

        @Override
        public void run ()
        {
            if ( pause || !metricsFlag.compareAndSet( TaskStatus.RUNNABLE, TaskStatus.RUNNING ) )
            {
                return;
            }

            try
            {
                if ( iteration < metricManagerConfig.getWarmupIterations() )
                {
                    LOGGER.info( "Warming up..." );
                    return;
                }
                else if ( iteration == metricManagerConfig.getWarmupIterations() )
                {
                    updateLastSystemTime( false );
                    initializePipelineMetricsContexts();

                    LOGGER.info( "Initialized..." );

                    return;
                }

                final long scanStartTimeInNanos = System.nanoTime();
                final Pair<Boolean, Long> result = updateLastSystemTime( true );
                final boolean publish = result._1;
                long systemTimeDiff = result._2;

                final Map<PipelineId, long[]> pipelineIdToThreadCpuTimes = collectThreadCpuTimes();
                systemTimeDiff += ( System.nanoTime() - scanStartTimeInNanos ) / 2;

                updatePipelineMetrics( pipelineIdToThreadCpuTimes, systemTimeDiff, publish );

                final long timeSpent = System.nanoTime() - scanStartTimeInNanos;
                scanMetricsHistogram.update( timeSpent );

                if ( publish )
                {
                    logPipelineMetrics( timeSpent );
                }
            }
            catch ( Exception e )
            {
                LOGGER.error( "Scan pipeline metrics failed", e );
            }
            finally
            {
                iteration++;
                metricsFlag.set( TaskStatus.RUNNABLE );
            }
        }

        private Pair<Boolean, Long> updateLastSystemTime ( final boolean log )
        {
            final long systemNanoTime = System.nanoTime();
            if ( systemNanoTime <= lastSystemNanoTime )
            {
                return Pair.of( false, -1L );
            }

            final long systemTimeDiff = systemNanoTime - lastSystemNanoTime;
            lastSystemNanoTime = systemNanoTime;

            if ( shouldUpdateMetrics( systemTimeDiff ) )
            {
                return Pair.of( true, systemTimeDiff );
            }

            if ( log )
            {
                LOGGER.warn( "It is too early for measuring pipelines. Time diff (ns): {}", systemTimeDiff );
            }

            return Pair.of( false, systemTimeDiff );
        }

        private boolean shouldUpdateMetrics ( final long systemTimeDiff )
        {
            final long periodInNanos = MILLISECONDS.toNanos( metricManagerConfig.getPipelineMetricsScanningPeriodInMillis() );
            final double skew = abs( ( (double) ( systemTimeDiff - periodInNanos ) ) / periodInNanos );

            return skew < metricManagerConfig.getPeriodSkewToleranceRatio();
        }

        private void initializePipelineMetricsContexts ()
        {
            for ( PipelineMetricsContext context : pipelineMetricsContextMap.values() )
            {
                context.initialize( threadMXBean );
            }
        }

        private Map<PipelineId, long[]> collectThreadCpuTimes ()
        {
            final Map<PipelineId, long[]> threadCpuTimes = new HashMap<>();
            for ( Entry<PipelineId, PipelineMetricsContext> e : pipelineMetricsContextMap.entrySet() )
            {
                final PipelineMetricsContext context = e.getValue();
                final long[] t = context.getThreadCpuTimes( threadMXBean );
                threadCpuTimes.put( e.getKey(), t );
            }

            return threadCpuTimes;
        }

        private void updatePipelineMetrics ( final Map<PipelineId, long[]> pipelineIdToThreadCpuTimes,
                                             final long systemTimeDiff,
                                             final boolean publish )
        {
            final Map<PipelineId, PipelineMetricsHistory> pipelineMetricsHistories = new HashMap<>();

            for ( Entry<PipelineId, long[]> e : pipelineIdToThreadCpuTimes.entrySet() )
            {
                final PipelineId pipelineId = e.getKey();
                final long[] newThreadCpuTimes = e.getValue();
                final PipelineMetricsContext context = pipelineMetricsContextMap.get( pipelineId );

                final PipelineMetrics pipelineMetrics = context.update( newThreadCpuTimes, systemTimeDiff );
                PipelineMetricsHistory newHistory = null;
                if ( metrics != null )
                {
                    final PipelineMetricsHistory currentHistory = metrics.getPipelineMetricsHistory( pipelineId );
                    if ( currentHistory != null )
                    {
                        newHistory = currentHistory.add( pipelineMetrics );
                    }
                }

                if ( newHistory == null )
                {
                    newHistory = new PipelineMetricsHistory( pipelineMetrics, metricManagerConfig.getHistorySize() );
                }

                pipelineMetricsHistories.put( pipelineId, newHistory );
            }

            if ( publish )
            {
                metrics = new FlowMetrics( getNewPeriod(), pipelineMetricsHistories );
            }
        }

        private int getNewPeriod ()
        {
            return metrics != null ? metrics.getPeriod() + 1 : 0;
        }

        private void logPipelineMetrics ( final long timeSpent )
        {
            final PipelineMetricsVisitor logVisitor = ( pipelineReplicaId, flowVersion, inboundThroughput, threadUtilizationRatio, pipelineCost, operatorCosts ) -> {
                final double cpuUsage = threadUtilizationRatio / numberOfCores;

                final String log = String.format(
                        "%s -> flow version: %d thread utilization: %.3f cpu usage: %.3f throughput: %s pipeline cost: %s operator costs:"
                        + " %s",
                        pipelineReplicaId,
                        flowVersion, threadUtilizationRatio, cpuUsage, Arrays.toString( inboundThroughput ),
                        pipelineCost,
                        Arrays.toString( operatorCosts ) );
                LOGGER.info( log );
            };

            final List<PipelineId> pipelineIds = new ArrayList<>( pipelineMetricsContextMap.keySet() );
            pipelineIds.sort( PipelineId::compareTo );

            for ( PipelineId pipelineId : pipelineIds )
            {
                final PipelineMetricsHistory pipelineMetricsHistory = metrics.getPipelineMetricsHistory( pipelineId );
                pipelineMetricsHistory.getLatest().visit( logVisitor );
            }

            final Snapshot scanMetricsSnapshot = scanMetricsHistogram.getSnapshot();
            final Snapshot scanOperatorsSnapshot = scanOperatorsHistogram.getSnapshot();
            LOGGER.debug( "SCAN METRICS   -> min: {} max: {} mean: {} std dev: {} median: {} .75: {} .95: {} .99: {} .999: {}",
                          scanMetricsSnapshot.getMin(),
                          scanMetricsSnapshot.getMax(),
                          scanMetricsSnapshot.getMean(),
                          scanMetricsSnapshot.getStdDev(),
                          scanMetricsSnapshot.getMedian(),
                          scanMetricsSnapshot.get75thPercentile(),
                          scanMetricsSnapshot.get95thPercentile(),
                          scanMetricsSnapshot.get99thPercentile(),
                          scanMetricsSnapshot.get999thPercentile() );

            LOGGER.debug( "SCAN OPERATORS -> min: {} max: {} mean: {} std dev: {} median: {} .75: {} .95: {} .99: {} .999: {}",
                          scanOperatorsSnapshot.getMin(),
                          scanOperatorsSnapshot.getMax(),
                          scanOperatorsSnapshot.getMean(),
                          scanOperatorsSnapshot.getStdDev(),
                          scanOperatorsSnapshot.getMedian(),
                          scanOperatorsSnapshot.get75thPercentile(),
                          scanOperatorsSnapshot.get95thPercentile(),
                          scanOperatorsSnapshot.get99thPercentile(),
                          scanOperatorsSnapshot.get999thPercentile() );

            final int period = metrics != null ? metrics.getPeriod() : -1;
            LOGGER.info( "Time spent (ns): {}. New flow period: {}", timeSpent, period );
        }

    }


    private class StartTasks implements Callable<Void>
    {

        final int flowVersion;

        final List<PipelineMeter> pipelineMeters;

        StartTasks ( final int flowVersion, final List<PipelineMeter> pipelineMeters )
        {
            this.flowVersion = flowVersion;
            this.pipelineMeters = pipelineMeters;
        }

        @Override
        public Void call ()
        {
            final TaskStatus metricsFlagStatus = metricsFlag.get();
            final TaskStatus samplingFlagStatus = samplingFlag.get();
            checkState( ( metricsFlagStatus == TaskStatus.INITIAL && samplingFlagStatus == TaskStatus.INITIAL ),
                        "cannot start metric collector since metrics flag is %s and sampling flag is %s",
                        metricsFlagStatus,
                        samplingFlagStatus );

            checkState( threadMXBean.isThreadCpuTimeSupported(), "cannot start metric collector since thread cpu time not supported!" );
            checkState( threadMXBean.isThreadCpuTimeEnabled(), "cannot start metric collector since thread cpu time not enabled!" );

            LOGGER.info( "Starting metrics collector..." );

            scanMetricsHistogram = metricRegistry.histogram( "scanMetrics" );
            scanOperatorsHistogram = metricRegistry.histogram( "scanOperators" );

            LOGGER.info( "JVM: {}", runtimeMXBean.getVmName() );
            LOGGER.info( "JVM Version: {}", runtimeMXBean.getVmVersion() );
            LOGGER.info( "JVM Vendor: {}", runtimeMXBean.getVmVendor() );
            LOGGER.info( "JVM Spec: {}", runtimeMXBean.getSpecName() );
            LOGGER.info( "JVM Spec Version: {}", runtimeMXBean.getSpecVersion() );
            LOGGER.info( "JVM Spec Vendor: {}", runtimeMXBean.getSpecVendor() );
            LOGGER.info( "JVM Start Time: {}", new Date( runtimeMXBean.getStartTime() ) );
            LOGGER.info( "JVM Uptime Millis: {}", runtimeMXBean.getUptime() );
            LOGGER.info( "OS: {}", osMXBean.getName() );
            LOGGER.info( "OS Version: {}", osMXBean.getVersion() );
            LOGGER.info( "OS Architecture: {}", osMXBean.getArch() );
            LOGGER.info( "Number of available processors: {}", osMXBean.getAvailableProcessors() );

            pipelineMeters.sort( comparing( PipelineMeter::getPipelineId, PipelineId::compareTo ) );

            for ( PipelineMeter pipelineMeter : pipelineMeters )
            {
                final PipelineId pipelineId = pipelineMeter.getPipelineId();
                final PipelineMetricsContext pipelineMetrics = new PipelineMetricsContext( flowVersion, pipelineMeter );
                pipelineMetricsContextMap.put( pipelineId, pipelineMetrics );
                register( pipelineMetrics );
                LOGGER.info( "Started tracking Pipeline {} with {} replicas and flow version {}",
                             pipelineId,
                             pipelineMeter.getReplicaCount(),
                             flowVersion );
            }

            LOGGER.info( "Metrics collector started." );

            metricsFlag.set( TaskStatus.RUNNABLE );
            samplingFlag.set( TaskStatus.RUNNABLE );

            createCsvReporter();

            return null;
        }

    }


    private class PauseTasks implements Callable<Void>
    {

        @Override
        public Void call ()
        {
            final TaskStatus metricsFlagStatus = metricsFlag.get();
            final TaskStatus samplingFlagStatus = samplingFlag.get();
            LOGGER.info( "Metric collector is pausing. {} {}", metricsFlagStatus, samplingFlagStatus );
            checkState( !( metricsFlagStatus == TaskStatus.PAUSED || samplingFlagStatus == TaskStatus.PAUSED
                           || metricsFlagStatus == TaskStatus.INITIAL || samplingFlagStatus == TaskStatus.INITIAL ),
                        "cannot pause metric collector since metrics flag is %s and sampling flag is %s",
                        metricsFlagStatus,
                        samplingFlagStatus );

            pause = true;
            setTaskPaused( metricsFlag );
            setTaskPaused( samplingFlag );

            LOGGER.info( "Metric collector is paused." );

            return null;
        }

    }


    private class UpdatePipelineMeters implements Callable<Void>
    {

        private final int flowVersion;

        private final List<PipelineId> pipelineIdsToRemove;

        private final List<PipelineMeter> newPipelineMeters;

        UpdatePipelineMeters ( final int flowVersion,
                               final List<PipelineId> pipelineIdsToRemove,
                               final List<PipelineMeter> newPipelineMeters )
        {
            this.flowVersion = flowVersion;
            this.pipelineIdsToRemove = pipelineIdsToRemove;
            this.newPipelineMeters = newPipelineMeters;
        }

        @Override
        public Void call ()
        {
            final TaskStatus metricsFlagStatus = metricsFlag.get();
            final TaskStatus samplingFlagStatus = samplingFlag.get();
            checkState( ( metricsFlagStatus == TaskStatus.PAUSED && samplingFlagStatus == TaskStatus.PAUSED ),
                        "cannot resume metric collector since metrics flag is %s and sampling flag is %s",
                        metricsFlagStatus,
                        samplingFlagStatus );

            pipelineIdsToRemove.sort( PipelineId::compareTo );

            for ( PipelineId pipelineId : pipelineIdsToRemove )
            {
                final PipelineMetricsContext context = pipelineMetricsContextMap.remove( pipelineId );
                deregister( pipelineId );
                checkState( context != null, "Pipeline %s not found in the tracked pipelines!", pipelineId );
                LOGGER.info( "Removed pipeline metrics of Pipeline {}", pipelineId );
            }

            newPipelineMeters.sort( comparing( PipelineMeter::getPipelineId, PipelineId::compareTo ) );

            for ( PipelineMeter pipelineMeter : newPipelineMeters )
            {
                final PipelineId pipelineId = pipelineMeter.getPipelineId();
                final PipelineMetricsContext context = new PipelineMetricsContext( flowVersion, pipelineMeter );
                pipelineMetricsContextMap.put( pipelineId, context );
                register( context );
                LOGGER.info( "Started tracking new Pipeline {} with {} replicas and flow version {}",
                             pipelineId,
                             pipelineMeter.getReplicaCount(),
                             flowVersion );
            }

            return null;
        }

    }


    private class ResumeTasks implements Callable<Void>
    {

        @Override
        public Void call ()
        {
            final TaskStatus metricsFlagStatus = metricsFlag.get();
            final TaskStatus samplingFlagStatus = samplingFlag.get();
            checkState( ( metricsFlagStatus == TaskStatus.PAUSED && samplingFlagStatus == TaskStatus.PAUSED ),
                        "cannot resume metric collector since metrics flag is %s and sampling flag is %s",
                        metricsFlagStatus,
                        samplingFlagStatus );

            LOGGER.info( "Metric collector is resumed" );

            iteration = 0;
            metricsFlag.set( TaskStatus.RUNNABLE );
            samplingFlag.set( TaskStatus.RUNNABLE );
            pause = false;

            return null;
        }

    }


    private class ShutdownTasks implements Callable<Void>
    {

        @Override
        public Void call ()
        {
            if ( metricsFlag.get() == TaskStatus.INITIAL && samplingFlag.get() == TaskStatus.INITIAL )
            {
                return null;
            }

            setTaskPaused( metricsFlag );
            setTaskPaused( samplingFlag );

            pipelineMetricsContextMap.clear();
            metricRegistry.removeMatching( MetricFilter.ALL );

            if ( csvReporter != null )
            {
                csvReporter.stop();
            }

            iteration = 0;

            LOGGER.info( "Metric collector is shut down." );

            return null;
        }

    }

}
