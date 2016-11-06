package cs.bilkent.joker.engine.pipeline;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import cs.bilkent.joker.engine.config.JokerConfig;
import static cs.bilkent.joker.engine.pipeline.PipelineReplicaRunner.PipelineReplicaRunnerCommandType.DRAIN_STATELESS_OPERATORS;
import static cs.bilkent.joker.engine.pipeline.PipelineReplicaRunner.PipelineReplicaRunnerCommandType.PAUSE;
import static cs.bilkent.joker.engine.pipeline.PipelineReplicaRunner.PipelineReplicaRunnerCommandType.RESUME;
import static cs.bilkent.joker.engine.pipeline.PipelineReplicaRunner.PipelineReplicaRunnerCommandType.STOP;
import static cs.bilkent.joker.engine.pipeline.PipelineReplicaRunner.PipelineReplicaRunnerCommandType.UPDATE_PIPELINE_UPSTREAM_CONTEXT;
import static cs.bilkent.joker.engine.pipeline.PipelineReplicaRunner.PipelineReplicaRunnerStatus.COMPLETED;
import static cs.bilkent.joker.engine.pipeline.PipelineReplicaRunner.PipelineReplicaRunnerStatus.PAUSED;
import static cs.bilkent.joker.engine.pipeline.PipelineReplicaRunner.PipelineReplicaRunnerStatus.RUNNING;
import cs.bilkent.joker.engine.supervisor.Supervisor;
import cs.bilkent.joker.engine.tuplequeue.OperatorTupleQueue;
import cs.bilkent.joker.operator.impl.TuplesImpl;
import cs.bilkent.joker.operator.scheduling.ScheduleNever;
import cs.bilkent.joker.operator.spec.OperatorType;
import static cs.bilkent.joker.operator.spec.OperatorType.STATELESS;
import static java.lang.Boolean.TRUE;

/**
 * Execution model of the operators is such that it invokes all of the invokable operators until they set their scheduling strategy to
 * {@link ScheduleNever}. Therefore, some of the operators may be invokable while others complete their execution.
 */
public class PipelineReplicaRunner implements Runnable
{

    private static final Logger LOGGER = LoggerFactory.getLogger( PipelineReplicaRunner.class );


    public enum PipelineReplicaRunnerStatus
    {
        RUNNING, PAUSED, COMPLETED
    }


    private final Object monitor = new Object();

    private final JokerConfig config;

    private final PipelineReplica pipeline;

    private final PipelineReplicaId id;

    private final long waitTimeoutInMillis;

    private final Supervisor supervisor;

    private DownstreamTupleSender downstreamTupleSender;

    private Future<Void> downstreamTuplesFuture;

    private PipelineReplicaRunnerStatus status = RUNNING;

    private boolean drainStatelessOperators;

    private volatile PipelineReplicaRunnerCommand command;


    public PipelineReplicaRunner ( final JokerConfig config, final PipelineReplica pipeline, final Supervisor supervisor,
                                   final DownstreamTupleSender downstreamTupleSender )
    {
        this.config = config;
        this.pipeline = pipeline;
        this.id = pipeline.id();
        this.waitTimeoutInMillis = config.getPipelineReplicaRunnerConfig().getWaitTimeoutInMillis();
        this.supervisor = supervisor;
        this.downstreamTupleSender = downstreamTupleSender;
    }

    public PipelineReplicaRunnerStatus getStatus ()
    {
        synchronized ( monitor )
        {
            return status;
        }
    }

    public UpstreamContext getPipelineUpstreamContext ()
    {
        synchronized ( monitor )
        {
            return pipeline.getPipelineUpstreamContext();
        }
    }

    public CompletableFuture<Boolean> pause ()
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
                    LOGGER.info( "{}: {} command is already set", id, PAUSE );
                    result = command.future;
                }
                else if ( type == RESUME )
                {
                    checkState( status == PAUSED,
                                "Pipeline %s cannot be paused since its status is %s and pending command is %s",
                                id,
                                status,
                                type );
                    LOGGER.info( "{}: Completing pending {} command because of new {} command.", id, RESUME, PAUSE );
                    command.complete();
                    this.command = null;
                    result = CompletableFuture.completedFuture( TRUE );
                }
                else if ( type == UPDATE_PIPELINE_UPSTREAM_CONTEXT )
                {
                    checkState( status == PAUSED || status == RUNNING,
                                "Pipeline %s cannot be paused since its status is %s and pending command is %s",
                                id,
                                status,
                                type );
                    LOGGER.info( "{}: switching pending {} command to {}", id, UPDATE_PIPELINE_UPSTREAM_CONTEXT, PAUSE );
                    command = new PipelineReplicaRunnerCommand( PAUSE, command.future );
                    this.command = command;
                    result = command.future;
                }
                else
                {
                    // STOP, DRAIN_STATELESS_OPERATORS OR UNKNOWN COMMAND
                    LOGGER.error( "{}: {} failed since there is a pending {} command", id, PAUSE, type );
                    result = new CompletableFuture<>();
                    command.future.thenRun( () -> result.completeExceptionally( new IllegalStateException( id + ": " + PAUSE
                                                                                                           + " failed since there "
                                                                                                           + "is a pending " + type
                                                                                                           + " command" ) ) );
                }
            }
            else if ( status == PAUSED )
            {
                LOGGER.info( "{} is already {}", id, PAUSED );
                result = CompletableFuture.completedFuture( TRUE );
            }
            else if ( status == RUNNING )
            {
                LOGGER.info( "{}: {} command is set", id, PAUSE );
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

    public CompletableFuture<Boolean> resume ()
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
                    LOGGER.info( "{}: {} command is already set", id, RESUME );
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
                    LOGGER.info( "{}: Completing pending {} command because of new {} command.", id, PAUSE, RESUME );
                    command.complete();
                    this.command = null;
                    result = CompletableFuture.completedFuture( TRUE );
                }
                else if ( type == UPDATE_PIPELINE_UPSTREAM_CONTEXT )
                {
                    LOGGER.info( "{}: switching pending {} command to {}", id, UPDATE_PIPELINE_UPSTREAM_CONTEXT, RESUME );
                    command = new PipelineReplicaRunnerCommand( RESUME, command.future );
                    this.command = command;
                    result = command.future;
                }
                else
                {
                    // STOP, DRAIN_STATELESS_OPERATORS OR UNKNOWN COMMAND
                    LOGGER.error( "{}: {} failed since there is a pending {} command", id, RESUME, type );
                    result = new CompletableFuture<>();
                    command.future.thenRun( () -> result.completeExceptionally( new IllegalStateException( id + ": " + RESUME
                                                                                                           + " failed since there "
                                                                                                           + "is a pending " + type
                                                                                                           + " command" ) ) );
                }
            }
            else if ( status == RUNNING )
            {
                LOGGER.info( "{} is already {}", id, RUNNING );
                result = CompletableFuture.completedFuture( TRUE );
            }
            else if ( status == PAUSED )
            {
                LOGGER.info( "{}: {} command is set", id, RESUME );
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

    public CompletableFuture<Boolean> stop ()
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
                    LOGGER.info( "{}: Completing pending {} command because of new {} command.", id, RESUME, STOP );
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
                    LOGGER.info( "{}: switching pending {} command to {}", id, PAUSE, STOP );
                    command = new PipelineReplicaRunnerCommand( STOP, command.future );
                    this.command = command;
                    result = command.future;
                }
                else if ( type == UPDATE_PIPELINE_UPSTREAM_CONTEXT )
                {
                    checkState( status == RUNNING || status == PAUSED,
                                "Pipeline %s cannot be stopped since its status is %s and pending command is %s",
                                id,
                                status,
                                type );
                    LOGGER.info( "{}: switching pending {} command to {}", id, type, STOP );
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
                    LOGGER.info( "{}: there is pending {} command already", id, type, STOP );
                    result = command.future;
                }
                else if ( type == DRAIN_STATELESS_OPERATORS )
                {
                    checkState( status == RUNNING,
                                "Pipeline %s cannot be stopped since its status is %s and pending command is %s",
                                id,
                                status,
                                type );
                    LOGGER.info( "{}: using pending {} command's future for {}", id, type, STOP );
                    result = command.future;
                }
                else
                {
                    // UNKNOWN COMMAND
                    LOGGER.error( "{}: {} failed since there is a pending {} command", id, STOP, type );
                    result = new CompletableFuture<>();
                    command.future.thenRun( () -> result.completeExceptionally( new IllegalStateException( id + ": " + STOP
                                                                                                           + " failed since there "
                                                                                                           + "is a pending " + type
                                                                                                           + " command" ) ) );
                }
            }
            else if ( status == PAUSED || status == RUNNING )
            {
                LOGGER.info( "{}: {} command is set in {} status", id, STOP, status );
                command = new PipelineReplicaRunnerCommand( STOP );
                this.command = command;
                result = command.future;
            }
            else if ( status == COMPLETED )
            {
                LOGGER.info( "{} is already {}", id, COMPLETED );
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

    public CompletableFuture<Boolean> updatePipelineUpstreamContext ()
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
                    LOGGER.info( "{}: {} command is set", id, UPDATE_PIPELINE_UPSTREAM_CONTEXT );
                    command = new PipelineReplicaRunnerCommand( UPDATE_PIPELINE_UPSTREAM_CONTEXT );
                    this.command = command;
                    result = command.future;
                }
                else
                {
                    LOGGER.error( "{}: {} failed since status is {}", id, UPDATE_PIPELINE_UPSTREAM_CONTEXT, COMPLETED );
                    result = new CompletableFuture<>();
                    result.completeExceptionally( new IllegalStateException( id + ": " + UPDATE_PIPELINE_UPSTREAM_CONTEXT
                                                                             + " failed since status is " + COMPLETED ) );
                }
            }
            else
            {
                LOGGER.info( "{}: there is already pending command {}", id, command.type );
                result = command.future;
            }
        }

        return result;
    }

    public CompletableFuture<Boolean> drainStatelessOperatorsAndStop ()
    {
        final CompletableFuture<Boolean> result;
        synchronized ( monitor )
        {
            PipelineReplicaRunnerCommand command = this.command;
            final PipelineReplicaRunnerStatus status = this.status;
            if ( command != null )
            {
                final PipelineReplicaRunnerCommandType type = command.type;
                if ( type == PAUSE || type == UPDATE_PIPELINE_UPSTREAM_CONTEXT || type == STOP )
                {
                    checkState( status == RUNNING,
                                "Pipeline %s cannot be drained since its status is %s and pending command is %s",
                                id,
                                status,
                                type );
                    LOGGER.info( "{}: switching pending {} command to {}", id, type, DRAIN_STATELESS_OPERATORS );
                    command = new PipelineReplicaRunnerCommand( STOP, command.future );
                    this.command = command;
                    result = command.future;
                }
                else
                {
                    // RESUME OR UNKNOWN COMMAND
                    LOGGER.error( "{}: {} failed since there is a pending {} command", id, DRAIN_STATELESS_OPERATORS, type );
                    result = new CompletableFuture<>();
                    command.future.thenRun( () -> result.completeExceptionally( new IllegalStateException( id + ": "
                                                                                                           + DRAIN_STATELESS_OPERATORS
                                                                                                           + " failed since there "
                                                                                                           + "is a pending " + type
                                                                                                           + " command" ) ) );
                }
            }
            else if ( status == RUNNING )
            {
                LOGGER.info( "{}: {} command is set", id, DRAIN_STATELESS_OPERATORS );
                command = new PipelineReplicaRunnerCommand( DRAIN_STATELESS_OPERATORS );
                this.command = command;
                result = command.future;
            }
            else
            {
                LOGGER.error( "{}: {} failed since status is {}", id, DRAIN_STATELESS_OPERATORS, status );
                result = new CompletableFuture<>();
                result.completeExceptionally( new IllegalStateException( id + ": " + DRAIN_STATELESS_OPERATORS + " failed since status is "
                                                                         + status ) );
            }
        }

        return result;
    }

    public void run ()
    {
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
                    awaitDownstreamTuplesFuture();
                    synchronized ( monitor )
                    {
                        monitor.wait( waitTimeoutInMillis );
                    }
                }
                else if ( status == COMPLETED )
                {
                    completeRun();
                    break;
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

        if ( status == COMPLETED )
        {
            LOGGER.info( "{}: completed the run", id );
        }
        else
        {
            LOGGER.error( "{}: completed the run with status: ", id, status );
        }
    }

    private PipelineReplicaRunnerStatus checkStatus () throws InterruptedException
    {
        PipelineReplicaRunnerStatus status = this.status;
        PipelineReplicaRunnerCommand command = this.command;
        if ( command != null )
        {
            synchronized ( monitor )
            {
                // we are re-reading the command here because it can be updated by one of the API methods before we acquire the lock
                command = this.command;

                final UpstreamContext pipelineUpstreamContext = supervisor.getUpstreamContext( id );
                final DownstreamTupleSender downstreamTupleSender = supervisor.getDownstreamTupleSender( id );

                checkNotNull( pipelineUpstreamContext, "Pipeline %s has null upstream context!", pipeline.id() );
                final PipelineReplicaRunnerCommandType commandType = command.type;
                if ( commandType == UPDATE_PIPELINE_UPSTREAM_CONTEXT )
                {
                    update( pipelineUpstreamContext, downstreamTupleSender );
                    LOGGER.info( "{}: update {} command is handled", id, pipeline.getPipelineUpstreamContext() );
                    this.command = null;
                    command.complete();
                }
                else if ( commandType == STOP )
                {
                    update( pipelineUpstreamContext, downstreamTupleSender );
                    LOGGER.info( "{}: stopping while {}", id, status );
                    status = COMPLETED;
                }
                else if ( commandType == DRAIN_STATELESS_OPERATORS )
                {
                    update( pipelineUpstreamContext, downstreamTupleSender );
                    LOGGER.info( "{}: drain-stopping while {}", id, status );
                    drainStatelessOperators = true;
                    status = COMPLETED;
                }
                else if ( status == RUNNING )
                {
                    if ( commandType == PAUSE )
                    {
                        update( pipelineUpstreamContext, downstreamTupleSender );
                        LOGGER.info( "{}: pausing", id );
                        command.complete();
                        this.command = null;
                        this.status = PAUSED;
                        status = PAUSED;
                    }
                    else
                    {
                        LOGGER.error( "{}: RESETTING WRONG COMMAND WITH TYPE: {} WHILE RUNNING", id, commandType );
                        command.completeExceptionally( new IllegalStateException( id + ": RESETTING WRONG COMMAND WITH TYPE: " + commandType
                                                                                  + " WHILE RUNNING" ) );
                        this.command = null;
                    }
                }
                else if ( status == PAUSED )
                {
                    if ( commandType == RESUME )
                    {
                        update( pipelineUpstreamContext, downstreamTupleSender );
                        LOGGER.info( "{}: resuming", id );
                        command.complete();
                        this.command = null;
                        this.status = RUNNING;
                        status = RUNNING;
                    }
                    else
                    {
                        LOGGER.error( "{}: RESETTING WRONG COMMAND WITH TYPE: {} WHILE PAUSED", id, commandType );
                        command.completeExceptionally( new IllegalStateException( id + ": RESETTING WRONG COMMAND WITH TYPE: " + commandType
                                                                                  + " WHILE PAUSED" ) );
                        this.command = null;
                    }
                }
            }
        }

        return status;
    }

    private void update ( final UpstreamContext pipelineUpstreamContext, final DownstreamTupleSender downstreamTupleSender )
    {
        pipeline.setPipelineUpstreamContext( pipelineUpstreamContext );
        this.downstreamTupleSender = downstreamTupleSender;
    }

    private void awaitDownstreamTuplesFuture ()
    {
        try
        {
            if ( downstreamTuplesFuture != null )
            {
                downstreamTuplesFuture.get();
                downstreamTuplesFuture = null;
            }
        }
        catch ( InterruptedException e )
        {
            LOGGER.error( "{}: runner thread interrupted", id );
            downstreamTuplesFuture = null;
            Thread.currentThread().interrupt();
            supervisor.notifyPipelineReplicaFailed( id, e );
            // TODO NOT SURE ABOUT THIS PART
        }
        catch ( ExecutionException e )
        {
            LOGGER.error( id + ": await downstream tuple future failed", e );
            downstreamTuplesFuture = null;
            supervisor.notifyPipelineReplicaFailed( id, e );
        }
    }

    private void completeRun ()
    {
        LOGGER.info( "{}: completing the run", id );

        if ( !pipeline.isCompleted() && drainStatelessOperators )
        {
            doDrainStatelessOperators();
        }

        awaitDownstreamTuplesFuture();
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
                if ( type == STOP || type == DRAIN_STATELESS_OPERATORS )
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

    private void doDrainStatelessOperators ()
    {
        LOGGER.info( "{}: Starting draining stateless operators", id );
        final List<OperatorTupleQueue> statelessOperatorTupleQueues = collectOperatorTupleQueues( STATELESS );

        while ( checkOperatorTupleQueuesNonEmpty( statelessOperatorTupleQueues ) )
        {
            sendToDownstream( pipeline.invoke( STATELESS ) );
        }

        LOGGER.info( "{}: Completed draining stateless operators", id );
    }

    private List<OperatorTupleQueue> collectOperatorTupleQueues ( final OperatorType operatorType )
    {
        final List<OperatorTupleQueue> operatorTupleQueues = new ArrayList<>();
        for ( OperatorReplica operatorReplica : pipeline.getOperators() )
        {
            if ( operatorReplica.getOperatorType() == operatorType )
            {
                operatorTupleQueues.add( operatorReplica.getQueue() );
            }
        }

        return operatorTupleQueues;
    }

    private boolean checkOperatorTupleQueuesNonEmpty ( final List<OperatorTupleQueue> operatorTupleQueues )
    {
        for ( int i = 0; i < operatorTupleQueues.size(); i++ )
        {
            if ( !operatorTupleQueues.get( i ).isEmpty() )
            {
                return true;
            }
        }

        return false;
    }

    private void sendToDownstream ( final TuplesImpl output )
    {
        if ( output != null && output.isNonEmpty() )
        {
            awaitDownstreamTuplesFuture();
            downstreamTuplesFuture = downstreamTupleSender.send( output );
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
        PAUSE, RESUME, STOP, UPDATE_PIPELINE_UPSTREAM_CONTEXT, DRAIN_STATELESS_OPERATORS
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
