package cs.bilkent.joker.engine.pipeline;

import java.io.Closeable;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import cs.bilkent.joker.engine.config.JokerConfig;
import static cs.bilkent.joker.engine.pipeline.PipelineReplicaRunner.PipelineReplicaRunnerCommandType.PAUSE;
import static cs.bilkent.joker.engine.pipeline.PipelineReplicaRunner.PipelineReplicaRunnerCommandType.REFRESH;
import static cs.bilkent.joker.engine.pipeline.PipelineReplicaRunner.PipelineReplicaRunnerCommandType.RESUME;
import static cs.bilkent.joker.engine.pipeline.PipelineReplicaRunner.PipelineReplicaRunnerCommandType.STOP;
import static cs.bilkent.joker.engine.pipeline.PipelineReplicaRunner.PipelineReplicaRunnerStatus.COMPLETED;
import static cs.bilkent.joker.engine.pipeline.PipelineReplicaRunner.PipelineReplicaRunnerStatus.PAUSED;
import static cs.bilkent.joker.engine.pipeline.PipelineReplicaRunner.PipelineReplicaRunnerStatus.RUNNING;
import cs.bilkent.joker.engine.supervisor.Supervisor;
import cs.bilkent.joker.operator.impl.TuplesImpl;
import static java.lang.Boolean.TRUE;
import net.openhft.affinity.AffinityLock;
import static net.openhft.affinity.AffinityLock.acquireLock;

public class PipelineReplicaRunner implements Runnable
{

    private static final Logger LOGGER = LoggerFactory.getLogger( PipelineReplicaRunner.class );


    public enum PipelineReplicaRunnerStatus
    {
        RUNNING, PAUSED, COMPLETED
    }


    private final Object monitor = new Object();

    private final PipelineReplica pipeline;

    private final PipelineReplicaId id;

    private final long waitTimeoutInMillis;

    private final Supervisor supervisor;

    private DownstreamCollector downstream;

    private final Supplier<Closeable> affinityLockSupplier;

    private PipelineReplicaRunnerStatus status = RUNNING;

    private volatile PipelineReplicaRunnerCommand command;


    PipelineReplicaRunner ( final JokerConfig config,
                            final PipelineReplica pipeline,
                            final Supervisor supervisor,
                            final DownstreamCollector downstreamCollector )
    {
        this.pipeline = pipeline;
        this.id = pipeline.id();
        this.waitTimeoutInMillis = config.getPipelineReplicaRunnerConfig().getRunnerWaitTimeoutInMillis();
        this.supervisor = supervisor;
        this.downstream = downstreamCollector;
        this.affinityLockSupplier = config.getPipelineReplicaRunnerConfig().shouldEnforceThreadAffinity() ? () -> {
            final AffinityLock lock = acquireLock();
            LOGGER.warn( "{} is BIND to {}", id, lock );
            return lock;
        } : () -> {
            LOGGER.warn( "{} is NOT BIND to any socket / core / cpu." );
            return () -> {
            };
        };
    }

    public PipelineReplicaRunnerStatus getStatus ()
    {
        synchronized ( monitor )
        {
            return status;
        }
    }

    UpstreamCtx getPipelineUpstreamCtx ()
    {
        synchronized ( monitor )
        {
            return pipeline.getUpstreamCtx();
        }
    }

    CompletableFuture<Boolean> pause ()
    {
        final CompletableFuture<Boolean> result;
        synchronized ( monitor )
        {
            PipelineReplicaRunnerCommand command = this.command;
            final PipelineReplicaRunnerStatus status = this.status;
            if ( command != null )
            {
                final PipelineReplicaRunnerCommandType type = command.type;
                if ( type == PAUSE )
                {
                    checkState( status == RUNNING,
                                "%s: cannot be paused since its status is %s and pending command is %s",
                                id,
                                status,
                                type );
                    LOGGER.debug( "{}: {} command is already set", id, PAUSE );
                    result = command.future;
                }
                else if ( type == RESUME )
                {
                    checkState( status == PAUSED,
                                "Pipeline %s cannot be paused since its status is %s and pending command is %s",
                                id,
                                status,
                                type );
                    LOGGER.debug( "{}: Completing pending {} command because of new {} command.", id, RESUME, PAUSE );
                    command.complete();
                    this.command = null;
                    result = CompletableFuture.completedFuture( TRUE );
                }
                else if ( type == REFRESH )
                {
                    checkState( status == PAUSED || status == RUNNING,
                                "Pipeline %s cannot be paused since its status is %s and pending command is %s",
                                id,
                                status,
                                type );
                    LOGGER.debug( "{}: switching pending {} command to {}", id, REFRESH, PAUSE );
                    command = new PipelineReplicaRunnerCommand( PAUSE, command.future );
                    this.command = command;
                    result = command.future;
                }
                else
                {
                    // STOP OR UNKNOWN COMMAND
                    LOGGER.error( "{}: {} failed since there is a pending {} command", id, PAUSE, type );
                    result = new CompletableFuture<>();
                    command.future.thenRun( () -> result.completeExceptionally( new IllegalStateException(
                            id + ": " + PAUSE + " failed since there " + "is a pending " + type + " command" ) ) );
                }
            }
            else if ( status == PAUSED )
            {
                LOGGER.debug( "{} is already {}", id, PAUSED );
                result = CompletableFuture.completedFuture( TRUE );
            }
            else if ( status == RUNNING )
            {
                LOGGER.debug( "{}: {} command is set", id, PAUSE );
                command = new PipelineReplicaRunnerCommand( PAUSE );
                this.command = command;
                result = command.future;
            }
            else
            {
                // COMPLETED OR UNKNOWN STATE
                LOGGER.error( "{}: {} failed since status is {}", id, PAUSE, status );
                result = new CompletableFuture<>();
                result.completeExceptionally( new IllegalStateException( id + ": " + PAUSE + " pause failed since status is " + status ) );
            }
        }

        return result;
    }

    CompletableFuture<Boolean> resume ()
    {
        final CompletableFuture<Boolean> result;
        synchronized ( monitor )
        {
            PipelineReplicaRunnerCommand command = this.command;
            final PipelineReplicaRunnerStatus status = this.status;
            if ( command != null )
            {
                final PipelineReplicaRunnerCommandType type = command.type;
                if ( type == RESUME )
                {
                    checkState( status == PAUSED,
                                "%s: cannot be resumed since its status is %s and pending command is %s",
                                id,
                                status,
                                type );
                    LOGGER.debug( "{}: {} command is already set", id, RESUME );
                    monitor.notify();
                    result = command.future;
                }
                else if ( type == PAUSE )
                {
                    checkState( status == RUNNING,
                                "Pipeline %s cannot be resumed since its status is %s and pending command is %s",
                                id,
                                status,
                                type );
                    LOGGER.debug( "{}: Completing pending {} command because of new {} command.", id, PAUSE, RESUME );
                    command.complete();
                    this.command = null;
                    result = CompletableFuture.completedFuture( TRUE );
                }
                else if ( type == REFRESH )
                {
                    LOGGER.debug( "{}: switching pending {} command to {}", id, REFRESH, RESUME );
                    command = new PipelineReplicaRunnerCommand( RESUME, command.future );
                    this.command = command;
                    result = command.future;
                }
                else
                {
                    // STOP, DRAIN_STATELESS_OPERATORS OR UNKNOWN COMMAND
                    LOGGER.error( "{}: {} failed since there is a pending {} command", id, RESUME, type );
                    result = new CompletableFuture<>();
                    command.future.thenRun( () -> result.completeExceptionally( new IllegalStateException(
                            id + ": " + RESUME + " failed since there " + "is a pending " + type + " command" ) ) );
                }
            }
            else if ( status == RUNNING )
            {
                LOGGER.debug( "{} is already {}", id, RUNNING );
                result = CompletableFuture.completedFuture( TRUE );
            }
            else if ( status == PAUSED )
            {
                LOGGER.debug( "{}: {} command is set", id, RESUME );
                command = new PipelineReplicaRunnerCommand( RESUME );
                this.command = command;
                result = command.future;
            }
            else
            {
                // COMPLETED OR UNKNOWN STATE
                LOGGER.error( "{}: {} failed since status is {}", id, RESUME, status );
                result = new CompletableFuture<>();
                result.completeExceptionally( new IllegalStateException( id + ": " + RESUME + " failed since status is " + status ) );
            }
        }

        return result;
    }

    CompletableFuture<Boolean> stop ()
    {
        final CompletableFuture<Boolean> result;
        synchronized ( monitor )
        {
            PipelineReplicaRunnerCommand command = this.command;
            final PipelineReplicaRunnerStatus status = this.status;
            if ( command != null )
            {
                final PipelineReplicaRunnerCommandType type = command.type;
                if ( type == RESUME )
                {
                    checkState( status == PAUSED,
                                "%s: cannot be stopped since its status is %s and pending command is %s",
                                id,
                                status,
                                type );
                    LOGGER.debug( "{}: Completing pending {} command because of new {} command.", id, RESUME, STOP );
                    command.complete();
                    command = new PipelineReplicaRunnerCommand( STOP );
                    this.command = command;
                    result = command.future;
                }
                else if ( type == PAUSE )
                {
                    checkState( status == RUNNING,
                                "Pipeline %s cannot be stopped since its status is %s and pending command is %s",
                                id,
                                status,
                                type );
                    LOGGER.debug( "{}: switching pending {} command to {}", id, PAUSE, STOP );
                    command = new PipelineReplicaRunnerCommand( STOP, command.future );
                    this.command = command;
                    result = command.future;
                }
                else if ( type == REFRESH )
                {
                    checkState( status == RUNNING || status == PAUSED,
                                "Pipeline %s cannot be stopped since its status is %s and pending command is %s",
                                id,
                                status,
                                type );
                    LOGGER.debug( "{}: switching pending {} command to {}", id, type, STOP );
                    command = new PipelineReplicaRunnerCommand( STOP, command.future );
                    this.command = command;
                    result = command.future;
                }
                else if ( type == STOP )
                {
                    checkState( status == RUNNING || status == PAUSED,
                                "Pipeline %s cannot be stopped since its status is %s and pending command is %s",
                                id,
                                status,
                                type );
                    LOGGER.debug( "{}: there is pending {} command already", id, type, STOP );
                    result = command.future;
                }
                else
                {
                    // UNKNOWN COMMAND
                    LOGGER.error( "{}: {} failed since there is a pending {} command", id, STOP, type );
                    result = new CompletableFuture<>();
                    command.future.thenRun( () -> result.completeExceptionally( new IllegalStateException(
                            id + ": " + STOP + " failed since there " + "is a pending " + type + " command" ) ) );
                }
            }
            else if ( status == PAUSED || status == RUNNING )
            {
                LOGGER.debug( "{}: {} command is set in {} status", id, STOP, status );
                command = new PipelineReplicaRunnerCommand( STOP );
                this.command = command;
                result = command.future;
            }
            else if ( status == COMPLETED )
            {
                LOGGER.debug( "{} is already {}", id, COMPLETED );
                result = CompletableFuture.completedFuture( TRUE );
            }
            else
            {
                // UNKNOWN STATE
                LOGGER.error( "{}: {} failed since status is {}", id, STOP, status );
                result = new CompletableFuture<>();
                result.completeExceptionally( new IllegalStateException( id + ": " + STOP + " failed since status is " + status ) );
            }
        }

        return result;
    }

    CompletableFuture<Boolean> refresh ()
    {
        final CompletableFuture<Boolean> result;
        synchronized ( monitor )
        {
            PipelineReplicaRunnerCommand command = this.command;
            final PipelineReplicaRunnerStatus status = this.status;
            if ( command == null )
            {
                if ( status != COMPLETED )
                {
                    LOGGER.debug( "{}: {} command is set", id, REFRESH );
                    command = new PipelineReplicaRunnerCommand( REFRESH );
                    this.command = command;
                    result = command.future;
                }
                else
                {
                    LOGGER.error( "{}: {} failed since status is {}", id, REFRESH, COMPLETED );
                    result = new CompletableFuture<>();
                    result.completeExceptionally( new IllegalStateException(
                            id + ": " + REFRESH + " failed since status is " + COMPLETED ) );
                }
            }
            else
            {
                LOGGER.debug( "{}: there is already pending command {}", id, command.type );
                result = command.future;
            }
        }

        return result;
    }

    @Override
    public void run ()
    {
        final Closeable closeable = affinityLockSupplier.get();

        try
        {
            while ( true )
            {
                final PipelineReplicaRunnerStatus status = checkStatus();
                if ( status == RUNNING )
                {
                    sendToDownstream( pipeline.invoke() );

                    if ( pipeline.isCompleted() )
                    {
                        LOGGER.info( "All operators of Pipeline {} are completed.", id );
                        completeRun();
                        break;
                    }
                }
                else if ( status == PAUSED )
                {
                    synchronized ( monitor )
                    {
                        monitor.wait( waitTimeoutInMillis );
                    }
                }
                else if ( status == COMPLETED )
                {
                    sendToDownstream( pipeline.invoke() );
                    if ( !pipeline.isInvoked() )
                    {
                        completeRun();
                        break;
                    }
                }
                else
                {
                    throw new IllegalStateException( "Illegal status: " + status );
                }
            }
        }
        catch ( Exception e )
        {
            completeRunWithFailure( e );
        }
        finally
        {
            try
            {
                closeable.close();
            }
            catch ( Exception e )
            {
                LOGGER.warn( "could not close the affinity lock!", e );
            }
        }

        if ( status == COMPLETED )
        {
            LOGGER.info( "{}: completed the run", id );
        }
        else
        {
            LOGGER.error( "{}: completed the run with status: ", id, status );
        }
    }

    private PipelineReplicaRunnerStatus checkStatus ()
    {
        PipelineReplicaRunnerStatus status = this.status;
        PipelineReplicaRunnerCommand command = this.command;
        if ( command != null )
        {
            synchronized ( monitor )
            {
                // we are re-reading the command here because it can be updated by one of the API methods before we acquire the lock
                command = this.command;

                if ( command == null )
                {
                    // status should be changed
                    return this.status;
                }

                final UpstreamCtx pipelineUpstreamCtx = supervisor.getUpstreamCtx( id );
                final DownstreamCollector downstreamCollector = supervisor.getDownstreamCollector( id );

                checkNotNull( pipelineUpstreamCtx, "Pipeline %s has null upstream context!", pipeline.id() );
                final PipelineReplicaRunnerCommandType commandType = command.type;
                if ( commandType == REFRESH )
                {
                    update( pipelineUpstreamCtx, downstreamCollector );
                    LOGGER.debug( "{}: refresh {} command is handled", id, pipeline.getUpstreamCtx() );
                    this.command = null;
                    command.complete();
                }
                else if ( commandType == STOP )
                {
                    update( pipelineUpstreamCtx, downstreamCollector );
                    LOGGER.debug( "{}: stopping while {}", id, status );
                    status = COMPLETED;
                }
                else if ( status == RUNNING )
                {
                    if ( commandType == PAUSE )
                    {
                        update( pipelineUpstreamCtx, downstreamCollector );
                        LOGGER.debug( "{}: pausing", id );
                        command.complete();
                        this.command = null;
                        this.status = PAUSED;
                        status = PAUSED;
                    }
                    else
                    {
                        LOGGER.error( "{}: RESETTING WRONG COMMAND WITH TYPE: {} WHILE RUNNING", id, commandType );
                        command.completeExceptionally( new IllegalStateException(
                                id + ": RESETTING WRONG COMMAND WITH TYPE: " + commandType + " WHILE RUNNING" ) );
                        this.command = null;
                    }
                }
                else if ( status == PAUSED )
                {
                    if ( commandType == RESUME )
                    {
                        update( pipelineUpstreamCtx, downstreamCollector );
                        LOGGER.debug( "{}: resuming", id );
                        command.complete();
                        this.command = null;
                        this.status = RUNNING;
                        status = RUNNING;
                    }
                    else
                    {
                        LOGGER.error( "{}: RESETTING WRONG COMMAND WITH TYPE: {} WHILE PAUSED", id, commandType );
                        command.completeExceptionally( new IllegalStateException(
                                id + ": RESETTING WRONG COMMAND WITH TYPE: " + commandType + " WHILE PAUSED" ) );
                        this.command = null;
                    }
                }
            }
        }

        return status;
    }

    private void update ( final UpstreamCtx pipelineUpstreamCtx, final DownstreamCollector downstreamCollector )
    {
        pipeline.setUpstreamCtx( pipelineUpstreamCtx );
        this.downstream = downstreamCollector;
    }

    private void completeRun ()
    {
        LOGGER.info( "{}: completing the run", id );
        LOGGER.info( "{}: all downstream tuples are sent", id );

        if ( pipeline.isCompleted() )
        {
            supervisor.notifyPipelineReplicaCompleted( pipeline.id() );
        }

        synchronized ( monitor )
        {
            final PipelineReplicaRunnerCommand command = this.command;
            if ( command != null )
            {
                final PipelineReplicaRunnerCommandType type = command.type;
                if ( type == STOP )
                {
                    LOGGER.info( "{}: completing command with type: {}", id, type );
                    command.complete();
                }
                else
                {
                    LOGGER.warn( "{}: completing command with type: {} exceptionally", id, type );
                    command.completeExceptionally( new IllegalStateException( id + " completed running!" ) );
                }
                this.command = null;
            }
            this.status = COMPLETED;
        }
    }

    private void sendToDownstream ( final TuplesImpl output )
    {
        if ( output != null && output.isNonEmpty() )
        {
            downstream.accept( output );
        }
    }

    private void completeRunWithFailure ( final Exception e )
    {
        LOGGER.error( id + ": runner failed", e );
        supervisor.notifyPipelineReplicaFailed( id, e );

        synchronized ( monitor )
        {
            final PipelineReplicaRunnerCommand command = this.command;
            if ( command != null )
            {
                final PipelineReplicaRunnerCommandType type = command.type;
                LOGGER.error( id + ": completing command with type: " + type + " exceptionally", e );
                command.completeExceptionally( new IllegalStateException( id + " completed running! ", e ) );
                this.command = null;
            }
            this.status = COMPLETED;
        }
    }


    enum PipelineReplicaRunnerCommandType
    {
        PAUSE, RESUME, STOP, REFRESH
    }


    private static class PipelineReplicaRunnerCommand
    {

        private final PipelineReplicaRunnerCommandType type;

        private final CompletableFuture<Boolean> future;

        private PipelineReplicaRunnerCommand ( final PipelineReplicaRunnerCommandType type )
        {
            this( type, new CompletableFuture<>() );
        }

        private PipelineReplicaRunnerCommand ( final PipelineReplicaRunnerCommandType type, final CompletableFuture<Boolean> future )
        {
            this.type = type;
            this.future = future;
        }

        void complete ()
        {
            future.complete( TRUE );
        }

        void completeExceptionally ( final Throwable throwable )
        {
            future.completeExceptionally( throwable );
        }

    }

}
