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
import cs.bilkent.joker.engine.FlowStatus;
import static cs.bilkent.joker.engine.FlowStatus.SHUT_DOWN;
import static cs.bilkent.joker.engine.config.JokerConfig.JOKER_THREAD_GROUP_NAME;
import cs.bilkent.joker.engine.exception.InitializationException;
import cs.bilkent.joker.engine.exception.JokerException;
import cs.bilkent.joker.engine.flow.FlowExecutionPlan;
import cs.bilkent.joker.engine.flow.PipelineId;
import cs.bilkent.joker.engine.flow.RegionExecutionPlan;
import cs.bilkent.joker.engine.metric.MetricManager;
import cs.bilkent.joker.engine.metric.PipelineMeter;
import cs.bilkent.joker.engine.pipeline.DownstreamTupleSender;
import cs.bilkent.joker.engine.pipeline.PipelineManager;
import cs.bilkent.joker.engine.pipeline.PipelineReplicaId;
import cs.bilkent.joker.engine.pipeline.UpstreamContext;
import cs.bilkent.joker.engine.supervisor.Supervisor;
import static cs.bilkent.joker.engine.util.ExceptionUtils.checkInterruption;
import cs.bilkent.joker.flow.FlowDef;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;

@Singleton
@ThreadSafe
public class SupervisorImpl implements Supervisor
{

    private static final Logger LOGGER = LoggerFactory.getLogger( SupervisorImpl.class );

    private static final long HEARTBEAT_LOG_PERIOD = SECONDS.toMillis( 15 );


    private final MetricManager metricManager;

    private PipelineManager pipelineManager;

    private final Thread supervisorThread;

    private final Object monitor = new Object();

    private final BlockingQueue<Runnable> queue = new LinkedBlockingQueue<>( Integer.MAX_VALUE );

    private CompletableFuture<Void> shutdownFuture;


    @Inject
    public SupervisorImpl ( final MetricManager metricManager, @Named( JOKER_THREAD_GROUP_NAME ) final ThreadGroup jokerThreadGroup )
    {
        this.metricManager = metricManager;
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

    public FlowExecutionPlan start ( final FlowDef flow,
                                     final List<RegionExecutionPlan> regionExecutionPlans ) throws InitializationException
    {
        synchronized ( monitor )
        {
            try
            {
                pipelineManager.start( flow, regionExecutionPlans );
                final FlowExecutionPlan flowExecutionPlan = pipelineManager.getFlowExecutionPlan();
                metricManager.start( flowExecutionPlan.getVersion(), pipelineManager.getAllPipelineMetersOrFail() );
                supervisorThread.start();
                return flowExecutionPlan;
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

    private void doShutdown ()
    {
        metricManager.shutdown();
        pipelineManager.triggerShutdown();
    }

    public Future<FlowExecutionPlan> mergePipelines ( final int flowVersion, final List<PipelineId> pipelineIdsToMerge )
    {
        final CompletableFuture<FlowExecutionPlan> future = new CompletableFuture<>();
        synchronized ( monitor )
        {
            checkState( isDeploymentChangeable(),
                        "cannot merge pipelines %s with flow version %s since %s and shutdown future is %s",
                        pipelineIdsToMerge,
                        flowVersion,
                        pipelineManager.getFlowStatus(),
                        shutdownFuture );

            final boolean result = queue.offer( () -> doMergePipelines( future, flowVersion, pipelineIdsToMerge ) );
            assert result : "offer failed for merge pipelines " + pipelineIdsToMerge + " with flow version " + flowVersion;
            LOGGER.info( "merge pipelines {} with flow version {} task offered", pipelineIdsToMerge, flowVersion );
        }

        return future;
    }

    public Future<FlowExecutionPlan> splitPipeline ( final int flowVersion,
                                                     final PipelineId pipelineId,
                                                     final List<Integer> pipelineOperatorIndices )
    {
        final CompletableFuture<FlowExecutionPlan> future = new CompletableFuture<>();
        synchronized ( monitor )
        {
            checkState( isDeploymentChangeable(),
                        "cannot split pipeline %s into %s with flow version %s since %s and shutdown future is %s",
                        pipelineId,
                        pipelineOperatorIndices,
                        flowVersion,
                        pipelineManager.getFlowStatus(),
                        shutdownFuture );

            final boolean result = queue.offer( () -> doSplitPipeline( future, flowVersion, pipelineId, pipelineOperatorIndices ) );
            assert result : "offer failed for split pipeline " + pipelineId + " into " + pipelineOperatorIndices + " with flow version "
                            + flowVersion;
            LOGGER.info( "split pipeline {} into {} with flow version {} task offered", pipelineId, pipelineOperatorIndices, flowVersion );
        }

        return future;
    }

    public Future<FlowExecutionPlan> rebalanceRegion ( final int flowVersion, final int regionId, final int newReplicaCount )
    {
        final CompletableFuture<FlowExecutionPlan> future = new CompletableFuture<>();
        synchronized ( monitor )
        {
            checkState( isDeploymentChangeable(),
                        "cannot rebalance region %s to new replica count %s with flow version %s since %s and shutdown future is %s",
                        regionId,
                        newReplicaCount,
                        flowVersion,
                        pipelineManager.getFlowStatus(),
                        shutdownFuture );

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

    private void doMergePipelines ( final CompletableFuture<FlowExecutionPlan> future,
                                    final int flowVersion,
                                    final List<PipelineId> pipelineIdsToMerge )
    {
        try
        {
            metricManager.pause();

            pipelineManager.mergePipelines( flowVersion, pipelineIdsToMerge );

            final FlowExecutionPlan flowExecutionPlan = pipelineManager.getFlowExecutionPlan();
            final PipelineMeter pipelineMeter = pipelineManager.getPipelineMeterOrFail( pipelineIdsToMerge.get( 0 ) );
            metricManager.resume( flowExecutionPlan.getVersion(), pipelineIdsToMerge, singletonList( pipelineMeter ) );
            future.complete( flowExecutionPlan );
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

    private void doSplitPipeline ( final CompletableFuture<FlowExecutionPlan> future, final int flowVersion,
                                   final PipelineId pipelineId,
                                   final List<Integer> pipelineOperatorIndices )
    {
        try
        {
            final RegionExecutionPlan regionExecutionPlan = pipelineManager.getFlowExecutionPlan()
                                                                           .getRegionExecutionPlan( pipelineId.getRegionId() );
            checkArgument( regionExecutionPlan != null, "Region of PipelineId %s not found to split", pipelineId );
            final List<PipelineId> unchangedPipelineIds = regionExecutionPlan.getPipelineIds();
            final boolean validPipelineId = unchangedPipelineIds.remove( pipelineId );
            checkArgument( validPipelineId, "Invalid PipelineId %s to split", pipelineId );

            metricManager.pause();
            pipelineManager.splitPipeline( flowVersion, pipelineId, pipelineOperatorIndices );

            final FlowExecutionPlan flowExecutionPlan = pipelineManager.getFlowExecutionPlan();
            final List<PipelineMeter> newPipelineMeters = pipelineManager.getRegionPipelineMetersOrFail( pipelineId.getRegionId() )
                                                                         .stream()
                                                                         .filter( p -> !unchangedPipelineIds.contains( p.getPipelineId() ) )
                                                                         .collect( toList() );
            metricManager.resume( flowExecutionPlan.getVersion(), singletonList( pipelineId ), newPipelineMeters );
            future.complete( flowExecutionPlan );
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

    private void doRebalanceRegion ( final CompletableFuture<FlowExecutionPlan> future,
                                     final int flowVersion,
                                     final int regionId,
                                     final int newReplicaCount )
    {
        try
        {
            metricManager.pause();

            pipelineManager.rebalanceRegion( flowVersion, regionId, newReplicaCount );

            final FlowExecutionPlan flowExecutionPlan = pipelineManager.getFlowExecutionPlan();
            final RegionExecutionPlan regionExecutionPlan = flowExecutionPlan.getRegionExecutionPlan( regionId );
            final List<PipelineMeter> pipelineMeters = pipelineManager.getRegionPipelineMetersOrFail( regionId );
            metricManager.resume( flowExecutionPlan.getVersion(), regionExecutionPlan.getPipelineIds(), pipelineMeters );
            future.complete( flowExecutionPlan );
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

    private boolean isInitialized ()
    {
        final FlowStatus status = pipelineManager.getFlowStatus();
        return !( status == FlowStatus.INITIAL || status == FlowStatus.INITIALIZATION_FAILED );
    }

    @Override
    public UpstreamContext getUpstreamContext ( final PipelineReplicaId id )
    {
        return pipelineManager.getUpstreamContext( id );
    }

    @Override
    public DownstreamTupleSender getDownstreamTupleSender ( final PipelineReplicaId id )
    {
        return pipelineManager.getDownstreamTupleSender( id );
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
                    final Runnable task = queue.poll( 1, SECONDS );
                    if ( task != null )
                    {
                        try
                        {
                            task.run();
                        }
                        catch ( Exception e )
                        {
                            completeShutdown( e );
                        }
                    }

                    if ( pipelineManager.getFlowStatus() == SHUT_DOWN )
                    {
                        break;
                    }

                    final long now = System.currentTimeMillis();
                    if ( ( now - lastReportTime ) > HEARTBEAT_LOG_PERIOD )
                    {
                        LOGGER.info( "Supervisor is up..." );
                        lastReportTime = now;
                    }
                }

                if ( queue.size() > 0 )
                {
                    LOGGER.error( "There are {} missed tasks in supervisor queue!", queue.size() );
                    queue.clear();
                }
            }
            catch ( InterruptedException e )
            {
                LOGGER.error( "Supervisor thread is interrupted!" );
                Thread.currentThread().interrupt();
            }
        }

    }

}
