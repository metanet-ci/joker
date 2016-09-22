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

import static com.google.common.base.Preconditions.checkState;
import cs.bilkent.joker.engine.FlowStatus;
import static cs.bilkent.joker.engine.FlowStatus.SHUT_DOWN;
import cs.bilkent.joker.engine.exception.InitializationException;
import cs.bilkent.joker.engine.pipeline.PipelineManager;
import cs.bilkent.joker.engine.pipeline.PipelineReplicaId;
import cs.bilkent.joker.engine.pipeline.UpstreamContext;
import cs.bilkent.joker.engine.region.FlowDeploymentDef;
import cs.bilkent.joker.engine.region.RegionConfig;
import cs.bilkent.joker.engine.supervisor.Supervisor;
import static java.util.concurrent.TimeUnit.SECONDS;

@Singleton
@ThreadSafe
public class SupervisorImpl implements Supervisor
{

    private static final Logger LOGGER = LoggerFactory.getLogger( SupervisorImpl.class );

    private static final long HEARTBEAT_LOG_PERIOD = SECONDS.toMillis( 15 );


    private final Object monitor = new Object();

    private final PipelineManager pipelineManager;

    private final BlockingQueue<Runnable> queue = new LinkedBlockingQueue<>( Integer.MAX_VALUE );

    private final Thread supervisorThread;

    private CompletableFuture<Void> shutdownFuture;


    @Inject
    public SupervisorImpl ( final PipelineManager pipelineManager, @Named( "jokerThreadGroup" ) final ThreadGroup jokerThreadGroup )
    {
        this.pipelineManager = pipelineManager;
        this.supervisorThread = new Thread( jokerThreadGroup, new TaskRunner(), jokerThreadGroup.getName() + "-Supervisor" );
    }

    public FlowStatus getFlowStatus ()
    {
        return pipelineManager.getFlowStatus();
    }

    public void start ( final FlowDeploymentDef flowDeployment, final List<RegionConfig> regionConfigs ) throws InitializationException
    {
        synchronized ( monitor )
        {
            try
            {
                pipelineManager.start( this, flowDeployment, regionConfigs );
            }
            catch ( InitializationException e )
            {
                if ( e.getCause() instanceof InterruptedException )
                {
                    Thread.currentThread().interrupt();
                }
                LOGGER.error( "Flow start failed", e );
                throw e;
            }
        }

        supervisorThread.start();
    }

    public Future<Void> shutdown ()
    {
        synchronized ( monitor )
        {
            checkState( isInitialized(), "cannot shutdown since {}", pipelineManager.getFlowStatus() );

            if ( shutdownFuture == null )
            {
                shutdownFuture = new CompletableFuture<>();
                final boolean result = queue.offer( pipelineManager::triggerShutdown );
                assert result : "offer failed for trigger shutdown";
                LOGGER.info( "trigger shutdown task offered" );
            }

            return shutdownFuture;
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
    public void notifyPipelineReplicaCompleted ( final PipelineReplicaId id )
    {
        synchronized ( monitor )
        {
            checkState( isInitialized(), "cannot notify pipeline replica {} completed since {}", id, pipelineManager.getFlowStatus() );
            checkState( shutdownFuture != null, "cannot notify pipeline replica {} completed since shutdown is not triggered", id );

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
            checkState( isInitialized(), "cannot notify pipeline replica {} failed with {} since {}",
                        id,
                        failure, pipelineManager.getFlowStatus() );

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
        if ( pipelineManager.notifyPipelineReplicaCompleted( id ) )
        {
            completeShutdown( null );
        }
    }

    private void doNotifyPipelineReplicaFailed ( final PipelineReplicaId id, final Throwable failure )
    {
        pipelineManager.notifyPipelineReplicaFailed( id, failure );
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
