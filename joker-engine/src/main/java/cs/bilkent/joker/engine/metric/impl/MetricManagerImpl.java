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
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
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
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import static com.google.common.base.Preconditions.checkState;
import cs.bilkent.joker.engine.config.JokerConfig;
import static cs.bilkent.joker.engine.config.JokerConfig.JOKER_THREAD_GROUP_NAME;
import cs.bilkent.joker.engine.config.MetricManagerConfig;
import cs.bilkent.joker.engine.exception.JokerException;
import cs.bilkent.joker.engine.flow.PipelineId;
import cs.bilkent.joker.engine.metric.MetricManager;
import cs.bilkent.joker.engine.metric.PipelineMeter;
import cs.bilkent.joker.engine.metric.impl.PipelineMetrics.PipelineMetricsVisitor;
import static java.util.Comparator.comparing;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

@Singleton
public class MetricManagerImpl implements MetricManager
{

    private static final Logger LOGGER = LoggerFactory.getLogger( MetricManagerImpl.class );

    private static final int METRICS_SCHEDULER_CORE_POOL_SIZE = 2;

    private static final int TASK_INITIAL = -2, TASK_RUNNING = 1, TASK_PAUSED = -1, TASK_RUNNABLE = 0;


    private final MetricManagerConfig metricManagerConfig;

    private final MetricRegistry metricRegistry;

    private final ThreadMXBean threadMXBean;

    private final RuntimeMXBean runtimeMXBean;

    private final OperatingSystemMXBean osMXBean;

    private final ScheduledExecutorService scheduler;

    private final int numberOfCores;

    private final Object monitor = new Object();

    private final ConcurrentMap<PipelineId, PipelineMetrics> pipelineMetricsMap = new ConcurrentHashMap<>();

    private final AtomicInteger metricsFlag = new AtomicInteger( TASK_INITIAL );

    private final AtomicInteger samplingFlag = new AtomicInteger( TASK_INITIAL );

    private volatile CsvReporter csvReporter;

    private volatile Histogram scanOperatorsHistogram;

    private volatile Histogram scanMetricsHistogram;

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
        this.scheduler.scheduleWithFixedDelay( new ScanPipelineMetrics(),
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
    public void resume ( final int flowVersion, final List<PipelineId> pipelineIdsToRemove, final List<PipelineMeter> newPipelineMeters )
    {
        call( new ResumeTasks( flowVersion, pipelineIdsToRemove, newPipelineMeters ), "pausing" );
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

    private void setTaskPaused ( final AtomicInteger flag )
    {
        while ( !flag.compareAndSet( TASK_RUNNABLE, TASK_PAUSED ) )
        {
            if ( flag.get() == TASK_PAUSED )
            {
                break;
            }

            LockSupport.parkNanos( 1 );
        }
    }

    private void createCsvReporter ()
    {
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

    private class SamplePipelines implements Runnable
    {

        @Override
        public void run ()
        {
            if ( pause || !samplingFlag.compareAndSet( TASK_RUNNABLE, TASK_RUNNING ) )
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
                for ( PipelineMetrics metrics : pipelineMetricsMap.values() )
                {
                    metrics.sample( threadMXBean );
                }

                scanOperatorsHistogram.update( System.nanoTime() - samplingStartTimeInNanos );
            }
            finally
            {
                samplingFlag.set( TASK_RUNNABLE );
            }
        }

    }


    private class ScanPipelineMetrics implements Runnable
    {

        long lastSystemNanoTime;

        @Override
        public void run ()
        {
            if ( pause || !metricsFlag.compareAndSet( TASK_RUNNABLE, TASK_RUNNING ) )
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
                    updateLastSystemTime();
                    initializePipelineMetrics();

                    LOGGER.info( "Initialized..." );

                    return;
                }

                final long scanStartTimeInNanos = System.nanoTime();
                long systemTimeDiff = updateLastSystemTime();
                if ( systemTimeDiff == -1 )
                {
                    return;
                }

                final Map<PipelineId, long[]> pipelineIdToThreadCpuTimes = collectThreadCpuTimes();
                systemTimeDiff += ( System.nanoTime() - scanStartTimeInNanos ) / 2;

                updatePipelineMetrics( pipelineIdToThreadCpuTimes, systemTimeDiff );

                final long timeSpent = System.nanoTime() - scanStartTimeInNanos;
                scanMetricsHistogram.update( timeSpent );

                logPipelineMetrics( timeSpent );
            }
            finally
            {
                iteration++;
                metricsFlag.set( TASK_RUNNABLE );
            }
        }

        private long updateLastSystemTime ()
        {
            final long systemNanoTime = System.nanoTime();
            final long systemTimeDiff = systemNanoTime - lastSystemNanoTime;

            final long periodInMillis = metricManagerConfig.getPipelineMetricsScanningPeriodInMillis();
            if ( ( (double) systemTimeDiff ) / MILLISECONDS.toNanos( periodInMillis ) < 0.25 )
            {
                LOGGER.warn( "It is too early for measuring pipelines. Time diff (ms): " + NANOSECONDS.toMillis( systemTimeDiff ) );
                return -1;
            }

            lastSystemNanoTime = systemNanoTime;
            return systemTimeDiff;
        }

        private void initializePipelineMetrics ()
        {
            for ( PipelineId pipelineId : pipelineMetricsMap.keySet() )
            {
                final PipelineMetrics pipelineMetrics = pipelineMetricsMap.get( pipelineId );
                pipelineMetrics.initialize( threadMXBean );
                pipelineMetricsMap.put( pipelineId, pipelineMetrics );
            }
        }

        private Map<PipelineId, long[]> collectThreadCpuTimes ()
        {
            final Map<PipelineId, long[]> threadCpuTimes = new HashMap<>();
            for ( Entry<PipelineId, PipelineMetrics> e : pipelineMetricsMap.entrySet() )
            {
                final PipelineMetrics pipelineMetrics = e.getValue();
                final long[] t = pipelineMetrics.getThreadCpuTimes( threadMXBean );
                threadCpuTimes.put( e.getKey(), t );
            }

            return threadCpuTimes;
        }

        private void updatePipelineMetrics ( final Map<PipelineId, long[]> pipelineIdToThreadCpuTimes, final long systemTimeDiff )
        {
            for ( Entry<PipelineId, long[]> e : pipelineIdToThreadCpuTimes.entrySet() )
            {
                final PipelineId pipelineId = e.getKey();
                final long[] newThreadCpuTimes = e.getValue();
                final PipelineMetrics pipelineMetrics = pipelineMetricsMap.get( pipelineId );

                pipelineMetrics.update( newThreadCpuTimes, systemTimeDiff );
            }
        }

        private void logPipelineMetrics ( final long timeSpent )
        {
            final PipelineMetricsVisitor logVisitor = ( pipelineReplicaId, flowVersion, consumeThroughputs, produceThroughputs,
                                                        threadUtilizationRatio, pipelineCost, operatorCosts ) ->
            {
                final double cpuUsage = threadUtilizationRatio / numberOfCores;

                final String log = String.format(
                        "%s -> flow version: %d thread utilization: %.3f cpu usage: %.3f consume: %s produce: " + "%s pipeline"
                        + " cost: %s " + "operator costs: %s",
                        pipelineReplicaId,
                        flowVersion,
                        threadUtilizationRatio,
                        cpuUsage,
                        Arrays.toString( consumeThroughputs ),
                        Arrays.toString( produceThroughputs ),
                        pipelineCost,
                        Arrays.toString( operatorCosts ) );
                LOGGER.info( log );
            };

            final List<PipelineId> pipelineIds = new ArrayList<>( pipelineMetricsMap.keySet() );
            pipelineIds.sort( PipelineId::compareTo );

            for ( PipelineId pipelineId : pipelineIds )
            {
                final PipelineMetrics pipelineMetrics = pipelineMetricsMap.get( pipelineId );
                pipelineMetrics.visit( logVisitor );
            }

            final Snapshot scanMetricsSnapshot = scanMetricsHistogram.getSnapshot();
            final Snapshot scanOperatorsSnapshot = scanOperatorsHistogram.getSnapshot();
            LOGGER.info( "SCAN METRICS   -> min: {} max: {} mean: {} std dev: {} median: {} .75: {} .95: {} .99: {} .999: {}",
                         scanMetricsSnapshot.getMin(),
                         scanMetricsSnapshot.getMax(),
                         scanMetricsSnapshot.getMean(),
                         scanMetricsSnapshot.getStdDev(),
                         scanMetricsSnapshot.getMedian(),
                         scanMetricsSnapshot.get75thPercentile(),
                         scanMetricsSnapshot.get95thPercentile(),
                         scanMetricsSnapshot.get99thPercentile(),
                         scanMetricsSnapshot.get999thPercentile() );

            LOGGER.info( "SCAN OPERATORS -> min: {} max: {} mean: {} std dev: {} median: {} .75: {} .95: {} .99: {} .999: {}",
                         scanOperatorsSnapshot.getMin(),
                         scanOperatorsSnapshot.getMax(),
                         scanOperatorsSnapshot.getMean(),
                         scanOperatorsSnapshot.getStdDev(),
                         scanOperatorsSnapshot.getMedian(),
                         scanOperatorsSnapshot.get75thPercentile(),
                         scanOperatorsSnapshot.get95thPercentile(),
                         scanOperatorsSnapshot.get99thPercentile(),
                         scanOperatorsSnapshot.get999thPercentile() );

            LOGGER.info( "Time spent (ns): {}", timeSpent );
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
            final int metricsFlagState = metricsFlag.get();
            final int samplingFlagState = samplingFlag.get();
            checkState( ( metricsFlagState == TASK_INITIAL && samplingFlagState == TASK_INITIAL ),
                        "cannot start metric collector since metrics flag is %s and sampling flag is %s",
                        samplingFlagState );

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
                final PipelineMetrics metrics = new PipelineMetrics( flowVersion, pipelineMeter, metricManagerConfig.getHistorySize() );
                metrics.register( metricRegistry );
                pipelineMetricsMap.put( pipelineId, metrics );
                LOGGER.info( "Started tracking Pipeline {} with {} replicas and flow version {}",
                             pipelineId,
                             pipelineMeter.getReplicaCount(),
                             flowVersion );
            }

            LOGGER.info( "Metrics collector started." );

            metricsFlag.set( TASK_RUNNABLE );
            samplingFlag.set( TASK_RUNNABLE );

            if ( metricManagerConfig.isCsvReportEnabled() )
            {
                createCsvReporter();
            }

            return null;
        }

    }


    private class PauseTasks implements Callable<Void>
    {

        @Override
        public Void call ()
        {
            final int metricsFlagState = metricsFlag.get();
            final int samplingFlagState = samplingFlag.get();
            LOGGER.info( "Metric collector is pausing. {} {}", metricsFlagState, samplingFlagState );
            checkState( !( metricsFlagState == TASK_PAUSED || samplingFlagState == TASK_PAUSED || metricsFlagState == TASK_INITIAL
                           || samplingFlagState == TASK_INITIAL ),
                        "cannot pause metric collector since metrics flag is %s and sampling flag is %s",
                        metricsFlagState,
                        samplingFlagState );

            pause = true;
            setTaskPaused( metricsFlag );
            setTaskPaused( samplingFlag );

            LOGGER.info( "Metric collector is paused." );

            return null;
        }

    }


    private class ResumeTasks implements Callable<Void>
    {

        private final int flowVersion;

        private final List<PipelineId> pipelineIdsToRemove;

        private final List<PipelineMeter> newPipelineMeters;

        ResumeTasks ( final int flowVersion, final List<PipelineId> pipelineIdsToRemove, final List<PipelineMeter> newPipelineMeters )
        {
            this.flowVersion = flowVersion;
            this.pipelineIdsToRemove = pipelineIdsToRemove;
            this.newPipelineMeters = newPipelineMeters;
        }

        @Override
        public Void call ()
        {
            final int metricsFlagState = metricsFlag.get();
            final int samplingFlagState = samplingFlag.get();
            checkState( ( metricsFlagState == TASK_PAUSED && samplingFlagState == TASK_PAUSED ),
                        "cannot resume metric collector since metrics flag is %s and sampling flag is %s",
                        metricsFlagState,
                        samplingFlagState );

            pipelineIdsToRemove.sort( PipelineId::compareTo );

            for ( PipelineId pipelineId : pipelineIdsToRemove )
            {
                final PipelineMetrics pipelineMetrics = pipelineMetricsMap.remove( pipelineId );
                pipelineMetrics.deregister( metricRegistry );
                checkState( pipelineMetrics != null, "Pipeline %s not found in the tracked pipelines!", pipelineId );
                LOGGER.info( "Removed pipeline metrics of Pipeline {}", pipelineId );
            }

            newPipelineMeters.sort( comparing( PipelineMeter::getPipelineId, PipelineId::compareTo ) );

            for ( PipelineMeter pipelineMeter : newPipelineMeters )
            {
                final PipelineId pipelineId = pipelineMeter.getPipelineId();
                final PipelineMetrics metrics = new PipelineMetrics( flowVersion, pipelineMeter, metricManagerConfig.getHistorySize() );
                metrics.register( metricRegistry );
                pipelineMetricsMap.put( pipelineId, metrics );
                LOGGER.info( "Started tracking new Pipeline {} with {} replicas and flow version {}",
                             pipelineId,
                             pipelineMeter.getReplicaCount(),
                             flowVersion );
            }

            LOGGER.info( "Metric collector is resumed" );

            iteration = 0;
            metricsFlag.set( TASK_RUNNABLE );
            samplingFlag.set( TASK_RUNNABLE );
            pause = false;

            return null;
        }

    }


    private class ShutdownTasks implements Callable<Void>
    {

        @Override
        public Void call ()
        {
            if ( metricsFlag.get() == TASK_INITIAL && samplingFlag.get() == TASK_INITIAL )
            {
                return null;
            }

            setTaskPaused( metricsFlag );
            setTaskPaused( samplingFlag );

            pipelineMetricsMap.clear();
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
