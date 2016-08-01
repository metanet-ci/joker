package cs.bilkent.zanza.engine.supervisor.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import cs.bilkent.zanza.engine.exception.InitializationException;
import cs.bilkent.zanza.engine.pipeline.OperatorReplicaStatus;
import static cs.bilkent.zanza.engine.pipeline.OperatorReplicaStatus.COMPLETING;
import cs.bilkent.zanza.engine.pipeline.Pipeline;
import cs.bilkent.zanza.engine.pipeline.PipelineId;
import cs.bilkent.zanza.engine.pipeline.PipelineManager;
import cs.bilkent.zanza.engine.pipeline.PipelineReplica;
import cs.bilkent.zanza.engine.pipeline.PipelineReplicaId;
import cs.bilkent.zanza.engine.pipeline.PipelineReplicaRunner;
import cs.bilkent.zanza.engine.pipeline.SupervisorNotifier;
import cs.bilkent.zanza.engine.pipeline.UpstreamConnectionStatus;
import static cs.bilkent.zanza.engine.pipeline.UpstreamConnectionStatus.NO_CONNECTION;
import cs.bilkent.zanza.engine.pipeline.UpstreamContext;
import cs.bilkent.zanza.engine.region.RegionConfig;
import cs.bilkent.zanza.engine.supervisor.Supervisor;
import static cs.bilkent.zanza.engine.supervisor.impl.SupervisorImpl.FlowStatus.INITIAL;
import static cs.bilkent.zanza.engine.supervisor.impl.SupervisorImpl.FlowStatus.INITIALIZATION_FAILED;
import static cs.bilkent.zanza.engine.supervisor.impl.SupervisorImpl.FlowStatus.RUNNING;
import static cs.bilkent.zanza.engine.supervisor.impl.SupervisorImpl.FlowStatus.SHUTTING_DOWN;
import static cs.bilkent.zanza.engine.supervisor.impl.SupervisorImpl.FlowStatus.SHUT_DOWN;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueueContext;
import cs.bilkent.zanza.engine.tuplequeue.impl.context.DefaultTupleQueueContext;
import cs.bilkent.zanza.flow.FlowDef;
import cs.bilkent.zanza.flow.OperatorDef;
import cs.bilkent.zanza.flow.Port;
import cs.bilkent.zanza.operator.scheduling.SchedulingStrategy;
import cs.bilkent.zanza.utils.Pair;
import static java.util.Collections.reverse;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;

@Singleton
@ThreadSafe
public class SupervisorImpl implements Supervisor
{

    private static final Logger LOGGER = LoggerFactory.getLogger( SupervisorImpl.class );

    private static final long HEARTBEAT_LOG_PERIOD = SECONDS.toMillis( 15 );


    enum FlowStatus
    {
        INITIAL, RUNNING, INITIALIZATION_FAILED, SHUTTING_DOWN, SHUT_DOWN
    }


    private final Object monitor = new Object();

    private final PipelineManager pipelineManager;

    private final ThreadGroup zanzaThreadGroup;

    private volatile FlowStatus status = INITIAL;

    @GuardedBy( "monitor" )
    private FlowDef flow;

    @GuardedBy( "monitor" )
    private final Map<PipelineId, Pipeline> pipelines = new ConcurrentHashMap<>();

    @GuardedBy( "monitor" )
    private final Map<PipelineId, Thread[]> pipelineThreads = new HashMap<>();

    private final BlockingQueue<Runnable> queue = new LinkedBlockingQueue<>( Integer.MAX_VALUE );

    private final Thread supervisorThread;

    private CompletableFuture<Void> shutdownFuture;


    @Inject
    public SupervisorImpl ( final PipelineManager pipelineManager, @Named( "ZanzaThreadGroup" ) final ThreadGroup zanzaThreadGroup )
    {
        this.pipelineManager = pipelineManager;
        this.zanzaThreadGroup = zanzaThreadGroup;
        this.supervisorThread = new Thread( zanzaThreadGroup, new TaskRunner(), zanzaThreadGroup.getName() + "-Supervisor" );
    }

    public FlowStatus getStatus ()
    {
        return status;
    }

    public void start ( final FlowDef flow, final List<RegionConfig> regionConfigs ) throws InitializationException
    {
        synchronized ( monitor )
        {
            doStart( flow, regionConfigs );
        }
        supervisorThread.start();
    }

    private void doStart ( final FlowDef flow, final List<RegionConfig> regionConfigs ) throws InitializationException
    {
        try
        {
            checkState( status == INITIAL, "cannot start since status is %s", status );
            this.flow = flow;
            final List<Pipeline> pipelines = pipelineManager.createPipelines( this, flow, regionConfigs );
            initPipelineReplicas( pipelines );
            for ( Pipeline pipeline : pipelines )
            {
                this.pipelines.put( pipeline.getId(), pipeline );
            }
            status = RUNNING;
            startPipelineReplicaRunnerThreads( pipelines );
        }
        catch ( Exception e )
        {
            status = INITIALIZATION_FAILED;
            throw new InitializationException( "Flow start failed", e );
        }
    }

    private void initPipelineReplicas ( final List<Pipeline> pipelines )
    {
        final List<PipelineReplica> pipelineReplicas = new ArrayList<>();
        try
        {
            for ( Pipeline pipeline : pipelines )
            {
                final UpstreamContext upstreamContext = pipeline.getUpstreamContext();
                for ( int replicaIndex = 0; replicaIndex < pipeline.getReplicaCount(); replicaIndex++ )
                {
                    final PipelineReplica pipelineReplica = pipeline.getPipelineReplica( replicaIndex );
                    final SupervisorNotifier supervisorNotifier = pipeline.getPipelineReplicaRunner( replicaIndex ).getSupervisorNotifier();
                    LOGGER.info( "Initializing Replica {} of Pipeline {}", replicaIndex, pipeline.getId() );
                    pipelineReplica.init( upstreamContext, supervisorNotifier );
                    pipelineReplicas.add( pipelineReplica );
                }

                final SchedulingStrategy initialSchedulingStrategy = pipeline.getPipelineReplica( 0 )
                                                                             .getOperator( 0 )
                                                                             .getInitialSchedulingStrategy();
                for ( int replicaIndex = 1; replicaIndex < pipeline.getReplicaCount(); replicaIndex++ )
                {
                    checkState( initialSchedulingStrategy.equals( pipeline.getPipelineReplica( replicaIndex )
                                                                          .getOperator( 0 )
                                                                          .getInitialSchedulingStrategy() ),
                                "Pipeline %s Replica %s has different Scheduling Strategy: %s than 0th Replica's Scheduling Strategy: ",
                                pipeline.getId(),
                                replicaIndex,
                                initialSchedulingStrategy );
                }
                pipeline.setInitialSchedulingStrategy( initialSchedulingStrategy );
            }
        }
        catch ( Exception e1 )
        {
            LOGGER.error( "Pipeline initialization failed.", e1 );
            reverse( pipelineReplicas );
            LOGGER.error( "Shutting down {} pipeline replicas.", pipelineReplicas.size() );
            for ( PipelineReplica p : pipelineReplicas )
            {
                try
                {
                    LOGGER.info( "Shutting down Pipeline Replica {}", p.id() );
                    p.shutdown();
                }
                catch ( Exception e2 )
                {
                    LOGGER.error( "Shutdown of Pipeline " + p.id() + " failed", e2 );
                }
            }

            pipelineManager.shutdown();

            throw e1;
        }
    }

    private void startPipelineReplicaRunnerThreads ( final List<Pipeline> pipelines )
    {
        for ( Pipeline pipeline : pipelines )
        {
            final int replicaCount = pipeline.getReplicaCount();
            final Thread[] threads = pipelineThreads.computeIfAbsent( pipeline.getId(), ( pipelineId ) -> new Thread[ replicaCount ] );
            for ( int replicaIndex = 0; replicaIndex < replicaCount; replicaIndex++ )
            {
                LOGGER.info( "Starting thread for Pipeline {} Replica {}", pipeline.getId(), replicaIndex );
                threads[ replicaIndex ] = new Thread( zanzaThreadGroup,
                                                      pipeline.getPipelineReplicaRunner( replicaIndex ),
                                                      zanzaThreadGroup.getName() + "-" + pipeline.getPipelineReplica( replicaIndex ).id() );

            }
        }

        for ( Thread[] threads : pipelineThreads.values() )
        {
            for ( Thread thread : threads )
            {
                thread.start();
            }
        }
    }

    public Future<Void> shutdown ()
    {
        final CompletableFuture<Void> future;
        synchronized ( monitor )
        {
            if ( status == RUNNING )
            {
                if ( shutdownFuture == null )
                {
                    shutdownFuture = new CompletableFuture<>();
                    assert queue.offer( this::triggerShutdown );
                }

                future = shutdownFuture;
            }
            else if ( status != INITIAL )
            {
                checkState( shutdownFuture != null, "cannot shutdown because shutdown feature is set in %s status", status );
                future = shutdownFuture;
            }
            else
            {
                throw new IllegalStateException( "Flow is not initialized yet!" );
            }
        }

        return future;
    }

    private void triggerShutdown ()
    {
        checkState( status == FlowStatus.RUNNING, "cannot trigger shutdown since status is %s", status );
        LOGGER.info( "Shutdown request is being handled." );

        status = SHUTTING_DOWN;

        for ( Pipeline pipeline : pipelines.values() )
        {
            final OperatorDef operatorDef = pipeline.getFirstOperatorDef();
            if ( flow.getUpstreamConnections( operatorDef.id() ).isEmpty() )
            {
                handlePipelineUpstreamUpdated( pipeline, new UpstreamContext( 1, new UpstreamConnectionStatus[] {} ) );
            }
        }
    }

    @Override
    public UpstreamContext getUpstreamContext ( final PipelineReplicaId id )
    {
        final Pipeline pipeline = pipelines.get( id.pipelineId );
        checkArgument( pipeline != null, "PipelineReplica %s not found", id );
        return pipeline.getUpstreamContext();
    }

    @Override
    public void notifyPipelineReplicaCompleted ( final PipelineReplicaId id )
    {
        synchronized ( monitor )
        {
            if ( status == SHUTTING_DOWN )
            {
                assert queue.offer( () -> doNotifyPipelineReplicaCompleted( id ) );
            }
            else
            {
                checkState( status == SHUT_DOWN, "Cannot notify for Pipeline Replica %s completion because status is %s", id, status );
                LOGGER.warn( "Skipping Pipeline Replica {} completion notification", id );
            }
        }
    }

    @Override
    public void notifyPipelineReplicaFailed ( final PipelineReplicaId id, final Throwable failure )
    {
        synchronized ( monitor )
        {
            if ( status == RUNNING || status == SHUTTING_DOWN )
            {
                assert queue.offer( () -> doNotifyPipelineReplicaFailed( id, failure ) );
            }
            else
            {
                checkState( status == SHUT_DOWN, "Pipeline replica %s failed with %s but flow is in %s status ", id, failure, status );
                LOGGER.warn( "Pipeline replica " + id + " failed but flow is in " + status + " status", failure );
            }
        }
    }

    private void doNotifyPipelineReplicaCompleted ( final PipelineReplicaId id )
    {
        checkState( status == SHUTTING_DOWN, "cannot notify Pipeline Replica %s completion since status is %s", id, status );

        final Pipeline pipeline = getPipelineOrFail( id.pipelineId );
        final OperatorReplicaStatus pipelineStatus = pipeline.getPipelineStatus();
        checkState( pipelineStatus == COMPLETING,
                    "Pipeline %s can not receive completion notification in %s status",
                    id.pipelineId,
                    pipelineStatus );

        LOGGER.info( "Pipeline replica {} is completed.", id );

        if ( pipeline.setPipelineReplicaCompleted( id.replicaIndex ) )
        {
            for ( Pipeline downstreamPipeline : getDownstreamPipelines( pipeline ) )
            {
                final UpstreamContext updatedUpstreamContext = getUpdatedUpstreamContext( downstreamPipeline );
                if ( updatedUpstreamContext != null )
                {
                    handlePipelineUpstreamUpdated( downstreamPipeline, updatedUpstreamContext );
                }
                else
                {
                    LOGGER.info( "Upstream Pipeline {} is completed but {} of Downstream Pipeline {} is same.",
                                 id,
                                 downstreamPipeline.getUpstreamContext(),
                                 downstreamPipeline.getId() );
                }
            }
        }

        if ( checkAllPipelinesCompleted() )
        {
            LOGGER.info( "All pipelines completed." );
            shutdownGracefully( null );
        }
    }

    private void doNotifyPipelineReplicaFailed ( final PipelineReplicaId id, final Throwable failure )
    {
        LOGGER.error( "Pipeline Replica " + id + " failed" );
        shutdownGracefully( failure );
    }

    private void handlePipelineUpstreamUpdated ( final Pipeline pipeline, final UpstreamContext upstreamContext )
    {
        LOGGER.info( "Updating upstream context of Pipeline {} to {}", pipeline.getId(), upstreamContext );

        final OperatorReplicaStatus pipelineStatus = pipeline.getPipelineStatus();
        checkState( pipelineStatus == OperatorReplicaStatus.RUNNING || pipelineStatus == OperatorReplicaStatus.COMPLETING,
                    "Cannot handle updated pipeline upstream since Pipeline %s is in %s status",
                    pipeline.getId(),
                    pipelineStatus );

        pipeline.setUpstreamContext( upstreamContext );
        if ( pipelineStatus == OperatorReplicaStatus.RUNNING && !upstreamContext.isInvokable( pipeline.getFirstOperatorDef(),
                                                                                              pipeline.getInitialSchedulingStrategy() ) )
        {
            LOGGER.info( "Pipeline {} is not invokable anymore. Setting pipeline status to {}", pipeline.getId(), COMPLETING );
            pipeline.setPipelineCompleting();
        }

        notifyPipelineReplicaRunners( pipeline );
    }

    private boolean checkAllPipelinesCompleted ()
    {
        for ( Pipeline pipeline : pipelines.values() )
        {
            if ( pipeline.getPipelineStatus() != OperatorReplicaStatus.COMPLETED )
            {
                return false;
            }
        }

        return true;
    }

    private void notifyPipelineReplicaRunners ( final Pipeline pipeline )
    {
        LOGGER.info( "Notifying runners about new upstream context of Pipeline {}", pipeline.getId() );
        for ( int i = 0; i < pipeline.getReplicaCount(); i++ )
        {
            final PipelineReplicaRunner runner = pipeline.getPipelineReplicaRunner( i );
            runner.updatePipelineUpstreamContext();
            // TODO handle future object
        }
    }

    private UpstreamContext getUpdatedUpstreamContext ( final Pipeline pipeline )
    {
        final UpstreamConnectionStatus[] upstreamConnectionStatuses = getUpstreamConnectionStatuses( pipeline );
        final UpstreamContext current = pipeline.getUpstreamContext();
        boolean update = false;
        for ( int i = 0; i < upstreamConnectionStatuses.length; i++ )
        {
            if ( upstreamConnectionStatuses[ i ] != current.getUpstreamConnectionStatus( i ) )
            {
                update = true;
                break;
            }
        }

        UpstreamContext upstreamContext = null;
        if ( update )
        {
            upstreamContext = new UpstreamContext( current.getVersion() + 1, upstreamConnectionStatuses );
        }

        return upstreamContext;
    }

    private UpstreamConnectionStatus[] getUpstreamConnectionStatuses ( final Pipeline pipeline )
    {
        final OperatorDef firstOperator = pipeline.getFirstOperatorDef();
        final UpstreamConnectionStatus[] statuses = new UpstreamConnectionStatus[ firstOperator.inputPortCount() ];
        for ( Entry<Port, Collection<Port>> entry : flow.getUpstreamConnections( firstOperator.id() ).entrySet() )
        {
            final Collection<Port> upstreamPorts = entry.getValue();
            final UpstreamConnectionStatus status;
            if ( upstreamPorts.isEmpty() )
            {
                status = NO_CONNECTION;
            }
            else
            {
                final List<Pair<OperatorDef, Pipeline>> upstream = upstreamPorts.stream()
                                                                                .map( port -> flow.getOperator( port.operatorId ) )
                                                                                .map( operator -> Pair.of( operator,
                                                                                                           getPipelineOrFail( operator ) ) )
                                                                                .collect( toList() );

                upstream.forEach( p -> checkState( p._2.getOperatorIndex( p._1 ) == p._2.getOperatorCount() - 1 ) );

                final boolean aliveConnectionPresent = upstream.stream().filter( p ->
                                                                                 {
                                                                                     final OperatorReplicaStatus pipelineStatus = p._2.getPipelineStatus();
                                                                                     return pipelineStatus == OperatorReplicaStatus.INITIAL
                                                                                            || pipelineStatus
                                                                                               == OperatorReplicaStatus.RUNNING
                                                                                            || pipelineStatus == COMPLETING;
                                                                                 } ).findFirst().isPresent();
                status = aliveConnectionPresent ? UpstreamConnectionStatus.ACTIVE : UpstreamConnectionStatus.CLOSED;
            }

            statuses[ entry.getKey().portIndex ] = status;
        }

        return statuses;
    }

    private Pipeline getPipelineOrFail ( final PipelineId id )
    {
        final Pipeline pipeline = pipelines.get( id );
        checkArgument( pipeline != null, "no pipeline runtime context found for pipeline instance id: " + id );
        return pipeline;
    }

    private Collection<Pipeline> getDownstreamPipelines ( final Pipeline upstreamPipeline )
    {
        return flow.getDownstreamConnections( upstreamPipeline.getLastOperatorDef().id() )
                   .values()
                   .stream()
                   .flatMap( ports -> ports.stream().map( port -> port.operatorId ) )
                   .distinct()
                   .map( flow::getOperator )
                   .map( operatorDef -> getPipelineOrFail( operatorDef, 0 ) ).collect( toList() );
    }

    private Pipeline getPipelineOrFail ( final OperatorDef operator )
    {
        return pipelines.values()
                        .stream()
                        .filter( p -> p.getOperatorIndex( operator ) != -1 )
                        .findFirst()
                        .orElseThrow( (Supplier<RuntimeException>) () -> new IllegalArgumentException( "No pipeline found for operator "
                                                                                                       + operator.id() ) );
    }

    private Pipeline getPipelineOrFail ( final OperatorDef operator, final int expectedOperatorIndex )
    {
        final Pipeline pipeline = getPipelineOrFail( operator );
        final int operatorIndex = pipeline.getOperatorIndex( operator );
        checkArgument( operatorIndex == expectedOperatorIndex,
                       "Pipeline %s has operator %s with index %s but expected index is %s",
                       pipeline.getId(),
                       operator.id(),
                       operatorIndex,
                       expectedOperatorIndex );
        return pipeline;
    }

    private void stopPipelineReplicaRunners ()
    {
        final List<Future<Boolean>> futures = new ArrayList<>();
        for ( Pipeline pipeline : pipelines.values() )
        {
            for ( int replicaIndex = 0; replicaIndex < pipeline.getReplicaCount(); replicaIndex++ )
            {
                final PipelineReplica pipelineReplica = pipeline.getPipelineReplica( replicaIndex );
                final TupleQueueContext upstreamTupleQueueContext = pipelineReplica.getUpstreamTupleQueueContext();
                if ( upstreamTupleQueueContext instanceof DefaultTupleQueueContext )
                {
                    final DefaultTupleQueueContext d = (DefaultTupleQueueContext) upstreamTupleQueueContext;
                    for ( int portIndex = 0; portIndex < d.getInputPortCount(); portIndex++ )
                    {
                        LOGGER.warn( "Pipeline {} upstream tuple queue capacity check is disabled", pipelineReplica.id() );
                        d.disableCapacityCheck( portIndex );
                    }
                }

                final PipelineReplicaRunner pipelineReplicaRunner = pipeline.getPipelineReplicaRunner( replicaIndex );
                futures.add( pipelineReplicaRunner.stop() );
            }
        }

        for ( Future<Boolean> future : futures )
        {
            try
            {
                future.get( 2, TimeUnit.MINUTES );
            }
            catch ( InterruptedException e )
            {
                LOGGER.error( "TaskRunner is interrupted while waiting for pipeline replica runner to stop", e );
                Thread.currentThread().interrupt();
            }
            catch ( ExecutionException e )
            {
                LOGGER.error( "Stop pipeline replica runner failed", e );
            }
            catch ( TimeoutException e )
            {
                LOGGER.error( "Stop pipeline replica runner timed out", e );
            }
        }
    }

    private void awaitPipelineThreads ()
    {
        LOGGER.warn( "Joining pipeline threads" );
        for ( Thread[] threads : pipelineThreads.values() )
        {
            for ( Thread thread : threads )
            {
                try
                {
                    thread.join( TimeUnit.MINUTES.toMillis( 2 ) );
                }
                catch ( InterruptedException e )
                {
                    LOGGER.error( "TaskRunner is interrupted while it is joined to pipeline runner thread", e );
                    Thread.currentThread().interrupt();
                }
            }
        }
        LOGGER.warn( "All pipeline threads are finished." );
    }

    private void shutdownPipelines ()
    {
        for ( Pipeline pipeline : pipelines.values() )
        {
            for ( int replicaIndex = 0; replicaIndex < pipeline.getReplicaCount(); replicaIndex++ )
            {
                final PipelineReplica replica = pipeline.getPipelineReplica( replicaIndex );
                try
                {
                    replica.shutdown();
                }
                catch ( Exception e )
                {
                    LOGGER.error( "PipelineReplica " + replica.id() + " shutdown failed", e );
                }
            }
        }
        pipelineManager.shutdown();
        this.pipelines.clear();
        this.pipelineThreads.clear();
    }

    private void shutdownGracefully ( final Throwable reason )
    {
        try
        {
            if ( reason != null )
            {
                LOGGER.error( "Shutting down flow", reason );
            }
            else
            {
                LOGGER.info( "Shutting down flow..." );
            }

            setShutDownStatus();

            stopPipelineReplicaRunners();
            awaitPipelineThreads();
            shutdownPipelines();

            CompletableFuture<Void> shutdownFuture;
            synchronized ( monitor )
            {
                shutdownFuture = this.shutdownFuture;
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
        catch ( Exception e )
        {
            LOGGER.error( "Shutdown failed", e );
        }
    }

    private void setShutDownStatus ()
    {
        synchronized ( monitor )
        {
            if ( shutdownFuture == null )
            {
                shutdownFuture = new CompletableFuture<>();
            }

            status = SHUT_DOWN;
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
                            shutdownGracefully( e );
                        }
                    }

                    if ( status == SHUT_DOWN )
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
                LOGGER.error( "Supervisor thread is interrupted" );
                Thread.currentThread().interrupt();
            }
        }

    }

}
