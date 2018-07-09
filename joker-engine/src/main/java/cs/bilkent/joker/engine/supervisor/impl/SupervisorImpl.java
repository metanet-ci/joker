package cs.bilkent.joker.engine.supervisor.impl;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static cs.bilkent.joker.JokerModule.JOINT_ADAPTATION_MANAGER_NAME;
import cs.bilkent.joker.engine.FlowStatus;
import static cs.bilkent.joker.engine.FlowStatus.SHUT_DOWN;
import cs.bilkent.joker.engine.adaptation.AdaptationAction;
import cs.bilkent.joker.engine.adaptation.AdaptationManager;
import cs.bilkent.joker.engine.adaptation.AdaptationTracker;
import cs.bilkent.joker.engine.config.JokerConfig;
import static cs.bilkent.joker.engine.config.JokerConfig.JOKER_THREAD_GROUP_NAME;
import cs.bilkent.joker.engine.config.MetricManagerConfig;
import cs.bilkent.joker.engine.exception.InitializationException;
import cs.bilkent.joker.engine.exception.JokerException;
import cs.bilkent.joker.engine.flow.FlowExecPlan;
import cs.bilkent.joker.engine.flow.PipelineId;
import cs.bilkent.joker.engine.flow.RegionExecPlan;
import cs.bilkent.joker.engine.metric.FlowMetrics;
import cs.bilkent.joker.engine.metric.MetricManager;
import cs.bilkent.joker.engine.metric.PipelineMeter;
import cs.bilkent.joker.engine.pipeline.DownstreamCollector;
import cs.bilkent.joker.engine.pipeline.PipelineManager;
import cs.bilkent.joker.engine.pipeline.PipelineReplicaId;
import cs.bilkent.joker.engine.pipeline.UpstreamCtx;
import cs.bilkent.joker.engine.supervisor.Supervisor;
import static cs.bilkent.joker.engine.util.ExceptionUtils.checkInterruption;
import cs.bilkent.joker.flow.FlowDef;
import cs.bilkent.joker.operator.utils.Pair;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;

@Singleton
@ThreadSafe
public class SupervisorImpl implements Supervisor
{

    private static final Logger LOGGER = LoggerFactory.getLogger( SupervisorImpl.class );

    private static final long HEARTBEAT_LOG_PERIOD = SECONDS.toMillis( 15 );

    private final JokerConfig config;

    private final MetricManager metricManager;

    private final AdaptationManager adaptationManager;

    private final AdaptationTracker adaptationTracker;

    private PipelineManager pipelineManager;

    private final Thread supervisorThread;

    private final Object monitor = new Object();

    private final BlockingQueue<Runnable> queue = new LinkedBlockingQueue<>( Integer.MAX_VALUE );

    private CompletableFuture<Void> shutdownFuture;

    private int flowPeriod;

    @Inject
    public SupervisorImpl ( final JokerConfig config,
                            final MetricManager metricManager,
                            @Named( JOINT_ADAPTATION_MANAGER_NAME ) final AdaptationManager adaptationManager,
                            final AdaptationTracker adaptationTracker,
                            @Named( JOKER_THREAD_GROUP_NAME ) final ThreadGroup jokerThreadGroup )
    {
        this.config = config;
        this.metricManager = metricManager;
        this.adaptationManager = adaptationManager;
        this.adaptationTracker = adaptationTracker;
        this.supervisorThread = new Thread( jokerThreadGroup, new TaskRunner(), jokerThreadGroup.getName() + "-Supervisor" );
    }

    @Inject
    public void setPipelineManager ( final PipelineManager pipelineManager )
    {
        this.pipelineManager = pipelineManager;
    }

    public FlowStatus getFlowStatus ()
    {
        return pipelineManager.getFlowStatus();
    }

    public FlowExecPlan start ( final FlowDef flow, final List<RegionExecPlan> regionExecPlans ) throws InitializationException
    {
        synchronized ( monitor )
        {
            try
            {
                pipelineManager.start( flow, regionExecPlans );
                final FlowExecPlan flowExecPlan = pipelineManager.getFlowExecPlan();

                adaptationTracker.init( this::shutdown, flowExecPlan );

                metricManager.start( flowExecPlan.getVersion(), pipelineManager.getAllPipelineMetersOrFail() );

                if ( isAdaptationEnabled() )
                {
                    adaptationManager.initialize( flow, flowExecPlan.getRegionExecPlans() );
                }

                supervisorThread.start();
                LOGGER.info( "Initial flow execution plan: {}", flowExecPlan.toSummaryString() );

                return flowExecPlan;
            }
            catch ( InitializationException e )
            {
                checkInterruption( e );
                LOGGER.error( "Flow start failed", e );
                throw e;
            }
        }
    }

    public Future<Void> shutdown ()
    {
        synchronized ( monitor )
        {
            checkState( isInitialized(), "cannot shutdown since %s", pipelineManager.getFlowStatus() );

            if ( shutdownFuture == null )
            {
                shutdownFuture = new CompletableFuture<>();
                final boolean result = queue.offer( this::doShutdown );
                assert result : "offer failed for trigger shutdown";
                LOGGER.info( "trigger shutdown task offered" );
            }

            return shutdownFuture;
        }
    }

    public Future<Void> disableAdaptation ()
    {
        final CompletableFuture<Void> future = new CompletableFuture<>();
        if ( !isAdaptationEnabled() )
        {
            future.complete( null );

            return future;
        }

        synchronized ( monitor )
        {
            checkState( isDeploymentChangeable(),
                        "cannot disable adaptation since %s and shutdown future is %s",
                        pipelineManager.getFlowStatus(),
                        shutdownFuture );

            final boolean result = queue.offer( () -> doDisableAdaptation( future ) );
            assert result : "offer failed for disable adaptation";
            LOGGER.info( "disable adaptation task offered" );
        }

        return future;
    }

    private void doShutdown ()
    {
        metricManager.shutdown();
        pipelineManager.triggerShutdown();
    }

    public Future<FlowExecPlan> mergePipelines ( final int flowVersion, final List<PipelineId> pipelineIdsToMerge )
    {
        final CompletableFuture<FlowExecPlan> future = new CompletableFuture<>();
        synchronized ( monitor )
        {
            checkState( isDeploymentChangeable(),
                        "cannot merge pipelines %s with flow version %s since %s and shutdown future is %s",
                        pipelineIdsToMerge,
                        flowVersion,
                        pipelineManager.getFlowStatus(),
                        shutdownFuture );
            checkState( !isAdaptationEnabled(), "cannot merge pipelines manually when adaptation is enabled" );

            final boolean result = queue.offer( () -> doMergePipelines( future, flowVersion, pipelineIdsToMerge ) );
            assert result : "offer failed for merge pipelines " + pipelineIdsToMerge + " with flow version " + flowVersion;
            LOGGER.info( "merge pipelines {} with flow version {} task offered", pipelineIdsToMerge, flowVersion );
        }

        return future;
    }

    public Future<FlowExecPlan> splitPipeline ( final int flowVersion,
                                                final PipelineId pipelineId,
                                                final List<Integer> pipelineOperatorIndices )
    {
        final CompletableFuture<FlowExecPlan> future = new CompletableFuture<>();
        synchronized ( monitor )
        {
            checkState( isDeploymentChangeable(),
                        "cannot split pipeline %s into %s with flow version %s since %s and shutdown future is %s",
                        pipelineId,
                        pipelineOperatorIndices,
                        flowVersion,
                        pipelineManager.getFlowStatus(),
                        shutdownFuture );
            checkState( !isAdaptationEnabled(), "cannot split pipeline manually when adaptation is enabled" );

            final boolean result = queue.offer( () -> doSplitPipeline( future, flowVersion, pipelineId, pipelineOperatorIndices ) );
            assert result : "offer failed for split pipeline " + pipelineId + " into " + pipelineOperatorIndices + " with flow version "
                            + flowVersion;
            LOGGER.info( "split pipeline {} into {} with flow version {} task offered", pipelineId, pipelineOperatorIndices, flowVersion );
        }

        return future;
    }

    public Future<FlowExecPlan> rebalanceRegion ( final int flowVersion, final int regionId, final int newReplicaCount )
    {
        final CompletableFuture<FlowExecPlan> future = new CompletableFuture<>();
        synchronized ( monitor )
        {
            checkState( isDeploymentChangeable(),
                        "cannot rebalance region %s to new replica count %s with flow version %s since %s and shutdown future is %s",
                        regionId,
                        newReplicaCount,
                        flowVersion,
                        pipelineManager.getFlowStatus(),
                        shutdownFuture );
            checkState( !isAdaptationEnabled(), "cannot rebalance region manually when adaptation is enabled" );

            final boolean result = queue.offer( () -> doRebalanceRegion( future, flowVersion, regionId, newReplicaCount ) );
            assert result :
                    "offer failed for rebalance region " + regionId + " to new replica count: " + newReplicaCount + " with flow version "
                    + flowVersion;
            LOGGER.info( "rebalance region {} to new replica count {} with flow version {} task offered",
                         regionId,
                         newReplicaCount,
                         flowVersion );
        }

        return future;
    }

    private boolean isDeploymentChangeable ()
    {
        return isInitialized() && ( shutdownFuture == null );
    }

    private void doMergePipelines ( final CompletableFuture<FlowExecPlan> future,
                                    final int flowVersion,
                                    final List<PipelineId> pipelineIdsToMerge )
    {
        try
        {
            metricManager.pause();

            pipelineManager.mergePipelines( flowVersion, pipelineIdsToMerge );

            final FlowExecPlan flowExecPlan = pipelineManager.getFlowExecPlan();
            final PipelineMeter pipelineMeter = pipelineManager.getPipelineMeterOrFail( pipelineIdsToMerge.get( 0 ) );
            metricManager.update( flowExecPlan.getVersion(), pipelineIdsToMerge, singletonList( pipelineMeter ) );
            metricManager.resume();
            future.complete( flowExecPlan );
        }
        catch ( IllegalArgumentException e )
        {
            LOGGER.error( "Merge pipelines " + pipelineIdsToMerge + " with flow version: " + flowVersion + " failed", e );
            future.completeExceptionally( e );
        }
        catch ( JokerException e )
        {
            LOGGER.error( "Merge pipelines " + pipelineIdsToMerge + " with flow version: " + flowVersion + " failed", e );
            future.completeExceptionally( e );
            throw e;
        }
    }

    private void doSplitPipeline ( final CompletableFuture<FlowExecPlan> future, final int flowVersion,
                                   final PipelineId pipelineId,
                                   final List<Integer> pipelineOperatorIndices )
    {
        try
        {
            final RegionExecPlan regionExecPlan = pipelineManager.getFlowExecPlan().getRegionExecPlan( pipelineId.getRegionId() );
            checkArgument( regionExecPlan != null, "Region of PipelineId %s not found to split", pipelineId );
            final List<PipelineId> unchangedPipelineIds = regionExecPlan.getPipelineIds();
            final boolean validPipelineId = unchangedPipelineIds.remove( pipelineId );
            checkArgument( validPipelineId, "Invalid PipelineId %s to split", pipelineId );

            metricManager.pause();
            pipelineManager.splitPipeline( flowVersion, pipelineId, pipelineOperatorIndices );

            final FlowExecPlan flowExecPlan = pipelineManager.getFlowExecPlan();
            final List<PipelineMeter> newPipelineMeters = pipelineManager.getRegionPipelineMetersOrFail( pipelineId.getRegionId() )
                                                                         .stream()
                                                                         .filter( p -> !unchangedPipelineIds.contains( p.getPipelineId() ) )
                                                                         .collect( toList() );
            metricManager.update( flowExecPlan.getVersion(), singletonList( pipelineId ), newPipelineMeters );
            metricManager.resume();
            future.complete( flowExecPlan );
        }
        catch ( IllegalArgumentException e )
        {
            LOGGER.error(
                    "Split pipeline " + pipelineId + " into " + pipelineOperatorIndices + " with flow version: " + flowVersion + " failed",
                    e );
            future.completeExceptionally( e );
        }
        catch ( JokerException e )
        {
            LOGGER.error(
                    "Split pipeline " + pipelineId + " into " + pipelineOperatorIndices + " with flow version: " + flowVersion + " failed",
                    e );
            future.completeExceptionally( e );
            throw e;
        }
    }

    private void doRebalanceRegion ( final CompletableFuture<FlowExecPlan> future,
                                     final int flowVersion,
                                     final int regionId,
                                     final int newReplicaCount )
    {
        try
        {
            metricManager.pause();

            pipelineManager.rebalanceRegion( flowVersion, regionId, newReplicaCount );

            final FlowExecPlan flowExecPlan = pipelineManager.getFlowExecPlan();
            final RegionExecPlan regionExecPlan = flowExecPlan.getRegionExecPlan( regionId );
            final List<PipelineMeter> pipelineMeters = pipelineManager.getRegionPipelineMetersOrFail( regionId );
            metricManager.update( flowExecPlan.getVersion(), regionExecPlan.getPipelineIds(), pipelineMeters );
            metricManager.resume();
            future.complete( flowExecPlan );
        }
        catch ( IllegalArgumentException e )
        {
            LOGGER.error( "Rebalance region " + regionId + " to new replica count: " + newReplicaCount + " with flow version " + flowVersion
                          + " failed", e );
            future.completeExceptionally( e );
        }
        catch ( JokerException e )
        {
            LOGGER.error( "Rebalance region " + regionId + " to new replica count: " + newReplicaCount + " with flow version " + flowVersion
                          + " failed", e );
            future.completeExceptionally( e );
            throw e;
        }
    }

    private void doDisableAdaptation ( final CompletableFuture<Void> future )
    {
        adaptationManager.disableAdaptation();
        LOGGER.warn( "Adaptation is disabled manually!" );
        future.complete( null );
    }

    private boolean isInitialized ()
    {
        final FlowStatus status = pipelineManager.getFlowStatus();
        return !( status == FlowStatus.INITIAL || status == FlowStatus.INITIALIZATION_FAILED );
    }

    @Override
    public UpstreamCtx getUpstreamCtx ( final PipelineReplicaId id )
    {
        return pipelineManager.getUpstreamCtx( id );
    }

    @Override
    public DownstreamCollector getDownstreamCollector ( final PipelineReplicaId id )
    {
        return pipelineManager.getDownstreamCollector( id );
    }

    @Override
    public void notifyPipelineReplicaCompleted ( final PipelineReplicaId id )
    {
        synchronized ( monitor )
        {
            checkState( isInitialized(), "cannot notify pipeline replica %s completed since %s", id, pipelineManager.getFlowStatus() );
            checkState( shutdownFuture != null, "cannot notify pipeline replica %s completed since shutdown is not triggered", id );

            if ( !shutdownFuture.isDone() )
            {
                final boolean result = queue.offer( () -> doNotifyPipelineReplicaCompleted( id ) );
                assert result : "offer failed for notify pipeline replica " + id + " completed";
                LOGGER.info( "notify pipeline replica {} completed task offered", id );
            }
            else
            {
                LOGGER.warn( "not offered pipeline replica {} completed since shutdown shutdown future is set.", id );
            }
        }
    }

    @Override
    public void notifyPipelineReplicaFailed ( final PipelineReplicaId id, final Throwable failure )
    {
        synchronized ( monitor )
        {
            checkState( isInitialized(),
                        "cannot notify pipeline replica %s failed with %s since %s",
                        id,
                        failure,
                        pipelineManager.getFlowStatus() );

            if ( shutdownFuture == null || !shutdownFuture.isDone() )
            {
                final boolean result = queue.offer( () -> doNotifyPipelineReplicaFailed( id, failure ) );
                assert result : "offer failed for notify pipeline replica " + id + " failed: " + failure;
                LOGGER.info( "notify pipeline replica {} failed with {} task offered", id, failure );
            }
            else
            {
                LOGGER.warn( "not offered pipeline replica " + id + " failed since shutdown future is set.", failure );
            }
        }
    }

    private void doNotifyPipelineReplicaCompleted ( final PipelineReplicaId id )
    {
        if ( pipelineManager.handlePipelineReplicaCompleted( id ) )
        {
            completeShutdown( null );
        }
    }

    private void doNotifyPipelineReplicaFailed ( final PipelineReplicaId id, final Throwable failure )
    {
        metricManager.shutdown();
        pipelineManager.handlePipelineReplicaFailed( id, failure );
        completeShutdown( failure );
    }

    private void completeShutdown ( final Throwable reason )
    {
        try
        {
            if ( reason != null )
            {
                LOGGER.error( "Shutting down flow because of failure...", reason );
            }
            else
            {
                LOGGER.info( "Shutting down flow..." );
            }

            synchronized ( monitor )
            {
                if ( shutdownFuture == null )
                {
                    shutdownFuture = new CompletableFuture<>();
                }

                if ( reason != null )
                {
                    shutdownFuture.completeExceptionally( reason );
                }
                else
                {
                    shutdownFuture.complete( null );
                }
            }
        }
        catch ( Exception e )
        {
            LOGGER.error( "Shutdown failed", e );
        }
        finally
        {
            final int remaining = queue.size();
            queue.clear();
            if ( remaining > 0 )
            {
                LOGGER.error( "Cleared {} pending tasks because of task failure", remaining );
            }
        }
    }

    private void checkAdaptation ()
    {
        final FlowMetrics metrics = metricManager.getMetrics();

        if ( !shouldCheckAdaptation( metrics ) )
        {
            return;
        }

        FlowExecPlan flowExecPlan = pipelineManager.getFlowExecPlan();

        adaptationTracker.onPeriod( flowExecPlan, metrics );

        // adaptation tracker may have initiated shutdown...
        if ( shutdownFuture != null )
        {
            return;
        }

        final List<AdaptationAction> actions = adaptationManager.adapt( flowExecPlan.getRegionExecPlans(), metrics );
        if ( actions.isEmpty() )
        {
            flowPeriod = metrics.getPeriod();
            LOGGER.info( "Flow period after adaptation check: {}", flowPeriod );
            return;
        }

        metricManager.pause();

        for ( AdaptationAction action : actions )
        {
            LOGGER.info( "Performing: {}", action );
            // new adaptation performer for each iteration is needed to get the latest flow execution version
            final DefaultAdaptationPerformer performer = new DefaultAdaptationPerformer( pipelineManager );
            action.apply( performer );
            final Pair<List<PipelineId>, List<PipelineId>> pipelineIdChanges = performer.getPipelineIdChanges();

            final int regionId = action.getCurrentExecPlan().getRegionId();
            flowExecPlan = pipelineManager.getFlowExecPlan();
            final RegionExecPlan newRegionExecPlan = flowExecPlan.getRegionExecPlan( regionId );
            checkState( newRegionExecPlan.equals( action.getNewExecPlan() ) );

            LOGGER.info( "Region execution plan after adaptation action: {}", newRegionExecPlan.toPlanSummaryString() );

            final List<PipelineId> removedPipelineIds = pipelineIdChanges._1;
            final List<PipelineMeter> addedPipelineMeters = pipelineIdChanges._2.stream()
                                                                                .map( pipelineManager::getPipelineMeterOrFail )
                                                                                .collect( toList() );

            LOGGER.info( "Flow execution plan after adaptation action: {}", flowExecPlan.toSummaryString() );
            metricManager.update( flowExecPlan.getVersion(), removedPipelineIds, addedPipelineMeters );
        }

        adaptationTracker.onExecPlanChange( flowExecPlan );

        flowPeriod = metricManager.getMetrics().getPeriod();
        LOGGER.info( "Flow period after adaptation: {}", flowPeriod );

        metricManager.resume();
    }

    private boolean shouldCheckAdaptation ( final FlowMetrics metrics )
    {
        if ( !( isAdaptationEnabled() && shutdownFuture == null ) )
        {
            return false;
        }

        final MetricManagerConfig metricManagerConfig = config.getMetricManagerConfig();
        return metrics != null && ( metrics.getPeriod() - flowPeriod ) > metricManagerConfig.getHistorySize();
    }

    private boolean isAdaptationEnabled ()
    {
        return config.getAdaptationConfig().isAdaptationEnabled();
    }

    private class TaskRunner implements Runnable
    {

        private long lastReportTime = 0;

        @Override
        public void run ()
        {
            try
            {
                while ( true )
                {
                    final Runnable task = pollTask();
                    if ( run( task ) )
                    {
                        break;
                    }

                    log();
                }

                clearTaskQueue();
            }
            catch ( InterruptedException e )
            {
                LOGGER.error( "Supervisor thread is interrupted!" );
                Thread.currentThread().interrupt();
            }
        }

        private Runnable pollTask () throws InterruptedException
        {
            Runnable task = queue.poll( 100, MILLISECONDS );
            if ( task == null )
            {
                task = SupervisorImpl.this::checkAdaptation;
            }

            return task;
        }

        private boolean run ( final Runnable task )
        {
            try
            {
                task.run();
            }
            catch ( Exception e )
            {
                completeShutdown( e );
            }

            return ( pipelineManager.getFlowStatus() == SHUT_DOWN );
        }

        private void log ()
        {
            final long now = System.currentTimeMillis();
            if ( ( now - lastReportTime ) > HEARTBEAT_LOG_PERIOD )
            {
                LOGGER.info( "Supervisor is up..." );
                lastReportTime = now;
            }
        }

        private void clearTaskQueue ()
        {
            if ( queue.size() > 0 )
            {
                LOGGER.error( "There are {} missed tasks in supervisor queue!", queue.size() );
                queue.clear();
            }
        }

    }

}
