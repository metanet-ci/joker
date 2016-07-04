package cs.bilkent.zanza.engine.pipeline;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import cs.bilkent.zanza.engine.config.ZanzaConfig;
import static cs.bilkent.zanza.engine.pipeline.PipelineInstanceRunner.PipelineInstanceRunnerCommandType.PAUSE;
import static cs.bilkent.zanza.engine.pipeline.PipelineInstanceRunner.PipelineInstanceRunnerCommandType.RESUME;
import static cs.bilkent.zanza.engine.pipeline.PipelineInstanceRunner.PipelineInstanceRunnerCommandType.UPDATE_PIPELINE_UPSTREAM_CONTEXT;
import static cs.bilkent.zanza.engine.pipeline.PipelineInstanceRunnerStatus.COMPLETED;
import static cs.bilkent.zanza.engine.pipeline.PipelineInstanceRunnerStatus.INITIAL;
import static cs.bilkent.zanza.engine.pipeline.PipelineInstanceRunnerStatus.PAUSED;
import static cs.bilkent.zanza.engine.pipeline.PipelineInstanceRunnerStatus.RUNNING;
import cs.bilkent.zanza.engine.supervisor.Supervisor;
import cs.bilkent.zanza.operator.impl.TuplesImpl;
import cs.bilkent.zanza.operator.scheduling.ScheduleNever;

/**
 * Execution model of the operators is such that it invokes all of the invokable operators until they set their scheduling strategy to
 * {@link ScheduleNever}. Therefore, some of the operators may be invokable while others complete their execution.
 */
public class PipelineInstanceRunner implements Runnable
{

    private static final Logger LOGGER = LoggerFactory.getLogger( PipelineInstanceRunner.class );


    private final Object monitor = new Object();

    private final ZanzaConfig config;

    private final PipelineInstance pipeline;

    private final PipelineInstanceId id;

    private final long waitTimeoutInMillis;

    private final Supervisor supervisor;

    private final SupervisorNotifier supervisorNotifier;


    private DownstreamTupleSender downstreamTupleSender;

    private Future<Void> downstreamTuplesFuture;

    private PipelineInstanceRunnerStatus status = INITIAL;

    private volatile PipelineInstanceRunnerCommand command;


    public PipelineInstanceRunner ( final ZanzaConfig config,
                                    final PipelineInstance pipeline,
                                    final Supervisor supervisor,
                                    final SupervisorNotifier supervisorNotifier )
    {
        this.config = config;
        this.pipeline = pipeline;
        this.id = pipeline.id();
        this.waitTimeoutInMillis = config.getPipelineInstanceRunnerConfig().waitTimeoutInMillis;
        synchronized ( monitor )
        {
            status = INITIAL;
        }
        this.supervisor = supervisor;
        this.supervisorNotifier = supervisorNotifier;
    }

    public void setDownstreamTupleSender ( final DownstreamTupleSender downstreamTupleSender )
    {
        this.downstreamTupleSender = downstreamTupleSender;
    }

    public PipelineInstanceRunnerStatus getStatus ()
    {
        synchronized ( monitor )
        {
            return status;
        }
    }

    public CompletableFuture<Void> pause ()
    {
        final CompletableFuture<Void> result;
        synchronized ( monitor )
        {
            final PipelineInstanceRunnerStatus status = this.status;
            if ( status == PAUSED )
            {
                LOGGER.info( "{}: shortcutting pause feature since already paused", id );
                result = new CompletableFuture<>();
                result.complete( null );
            }
            else if ( status != RUNNING )
            {
                LOGGER.error( "{}: pause failed since not running! status: {}", id, status );
                result = new CompletableFuture<>();
                result.completeExceptionally( new IllegalStateException( id + ": pause failed since status: " + status ) );
            }
            else
            {
                PipelineInstanceRunnerCommand command = this.command;
                if ( command != null )
                {
                    if ( command.getType() == PAUSE )
                    {
                        LOGGER.info( "{}: pause command is already set", id );
                        result = command.getFuture();
                    }
                    else if ( command.getType() == UPDATE_PIPELINE_UPSTREAM_CONTEXT )
                    {
                        updatePipelineUpstreamContextInternal();
                        LOGGER.info( "{}: handling update pipeline upstream context {} during pause request",
                                     pipeline.getPipelineUpstreamContext(),
                                     id );
                        command.complete();
                        command = PipelineInstanceRunnerCommand.pause();
                        this.command = command;
                        result = command.getFuture();
                    }
                    else
                    {
                        LOGGER.error( "{}: pause failed since there is another pending command with type: {}", id, command.getType() );
                        result = new CompletableFuture<>();
                        result.completeExceptionally( new IllegalStateException( id
                                                                                 + ": pause failed since there is another pending command"
                                                                                 + " with type: " + command.getType() ) );
                    }
                }
                else
                {
                    LOGGER.info( "{}: pause command is set", id );
                    command = PipelineInstanceRunnerCommand.pause();
                    this.command = command;
                    result = command.getFuture();
                }
            }
        }

        return result;
    }

    public CompletableFuture<Void> resume ()
    {
        final CompletableFuture<Void> result;
        synchronized ( monitor )
        {
            final PipelineInstanceRunnerStatus status = this.status;
            if ( status == RUNNING )
            {
                LOGGER.info( "{}: shortcutting resume since already running", id );
                result = new CompletableFuture<>();
                result.complete( null );
            }
            else if ( status != PAUSED )
            {
                LOGGER.error( "{}: resume failed since not paused. status: {}", id, status );
                result = new CompletableFuture<>();
                result.completeExceptionally( new IllegalStateException( id + ": resume failed since not paused. status: " + status ) );
            }
            else
            {
                PipelineInstanceRunnerCommand command = this.command;
                if ( command != null )
                {
                    if ( command.getType() == RESUME )
                    {
                        LOGGER.info( "{}: resume command is already set. notifying anyway", id );
                        monitor.notify();
                        result = command.getFuture();
                    }
                    else if ( command.getType() == UPDATE_PIPELINE_UPSTREAM_CONTEXT )
                    {
                        updatePipelineUpstreamContextInternal();
                        LOGGER.info( "{}: handling update pipeline upstream context {} during resume request",
                                     pipeline.getPipelineUpstreamContext(),
                                     id );
                        command.complete();
                        command = PipelineInstanceRunnerCommand.resume();
                        this.command = command;
                        monitor.notify();
                        result = command.getFuture();
                    }
                    else
                    {
                        LOGGER.error( "{}: resume failed since there is another pending command with type: {}", id, command.getType() );
                        result = new CompletableFuture<>();
                        result.completeExceptionally( new IllegalStateException( id + ": resume failed since there is another pending "
                                                                                 + "command with type: " + command.getType() ) );
                    }
                }
                else
                {
                    LOGGER.info( "{}: resume command is set", id );
                    command = PipelineInstanceRunnerCommand.resume();
                    this.command = command;
                    monitor.notify();
                    result = command.getFuture();
                }
            }
        }

        return result;
    }

    public CompletableFuture<Void> updatePipelineUpstreamContext ()
    {
        final CompletableFuture<Void> result;
        synchronized ( monitor )
        {
            final PipelineInstanceRunnerStatus status = this.status;
            if ( status == PAUSED || status == RUNNING )
            {
                PipelineInstanceRunnerCommand command = this.command;
                if ( command == null )
                {
                    LOGGER.info( "{}: update pipeline upstream context command is set", id );
                    command = PipelineInstanceRunnerCommand.updatePipelineUpstreamContext();
                    this.command = command;
                    result = command.getFuture();
                    monitor.notify();
                }
                else if ( command.getType() == UPDATE_PIPELINE_UPSTREAM_CONTEXT )
                {
                    LOGGER.info( "{}: update pipeline upstream context command is already set", id );
                    result = command.getFuture();
                }
                else if ( command.hasType( PAUSE ) || command.hasType( RESUME ) )
                {
                    updatePipelineUpstreamContextInternal();
                    LOGGER.info( "{}: updating pipeline upstream context {} immediately since there is pending command: {}",
                                 pipeline.getPipelineUpstreamContext(),
                                 id,
                                 command.getType() );
                    result = new CompletableFuture<>();
                    result.complete( null );
                }
                else
                {
                    LOGGER.error( "{}: update pipeline upstream context failed since there is another pending command with type: {}",
                                  id,
                                  command.getType() );
                    result = new CompletableFuture<>();
                    result.completeExceptionally( new IllegalStateException( id + ": update pipeline upstream context tail sender failed "
                                                                             + "since " + "there is another pending "
                                                                             + "command with type: " + command.getType() ) );
                }
            }
            else
            {
                LOGGER.error( "{}: update pipeline upstream context failed since not running or paused. status: {}", id, status );
                result = new CompletableFuture<>();
                result.completeExceptionally( new IllegalStateException( id
                                                                         + ": update pipeline upstream context failed since not running or "
                                                                         + "paused. status: " + status ) );
            }
        }

        return result;
    }

    public UpstreamContext getPipelineUpstreamContext ()
    {
        synchronized ( monitor )
        {
            return pipeline.getPipelineUpstreamContext();
        }
    }

    public void run ()
    {
        checkState( status == INITIAL );
        synchronized ( monitor )
        {
            status = RUNNING;
        }

        try
        {
            while ( true )
            {
                final PipelineInstanceRunnerStatus status = checkStatus();
                if ( status == PAUSED )
                {
                    awaitDownstreamTuplesFuture();
                    synchronized ( monitor )
                    {
                        monitor.wait( waitTimeoutInMillis );
                    }
                    continue;
                }

                final TuplesImpl output = pipeline.invoke();
                if ( output != null && output.isNonEmpty() )
                {
                    awaitDownstreamTuplesFuture();
                    downstreamTuplesFuture = downstreamTupleSender.send( output );
                }

                if ( supervisorNotifier.isPipelineCompleted() )
                {
                    completeRun();
                    break;
                }
            }
        }
        catch ( InterruptedException e )
        {
            LOGGER.error( "{}: runner thread interrupted", id );
            Thread.currentThread().interrupt();
            // TODO stop operators and clean its internal state here
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

    private PipelineInstanceRunnerStatus checkStatus () throws InterruptedException
    {
        PipelineInstanceRunnerStatus result = RUNNING;
        final PipelineInstanceRunnerStatus status = this.status;
        final PipelineInstanceRunnerCommand command = this.command;
        if ( command != null )
        {
            final PipelineInstanceRunnerCommandType commandType = command.getType();
            synchronized ( monitor )
            {
                if ( commandType == UPDATE_PIPELINE_UPSTREAM_CONTEXT )
                {
                    updatePipelineUpstreamContextInternal();
                    LOGGER.info( "{}: update {} command is noticed", id, pipeline.getPipelineUpstreamContext() );
                    this.command = null;
                    this.status = RUNNING;
                    command.complete();
                }
                else if ( status == RUNNING )
                {
                    if ( commandType == PAUSE )
                    {
                        LOGGER.info( "{}: pausing", id );
                        command.complete();
                        this.command = null;
                        this.status = PAUSED;
                        result = PAUSED;
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
                        LOGGER.info( "{}: resuming", id );
                        command.complete();
                        this.command = null;
                        this.status = RUNNING;
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
        else
        {
            result = status;
        }

        return result;
    }

    // this method is invoked within a lock. normally, it is a bad practice since we are calling an alien object inside the method.
    // we are still doing it anyway since it simplifies the logic a lot and the alien method is a very simple query method.
    private void updatePipelineUpstreamContextInternal ()
    {
        final UpstreamContext pipelineUpstreamContext = supervisor.getUpstreamContext( id );
        pipeline.setPipelineUpstreamContext( pipelineUpstreamContext );
    }

    private void awaitDownstreamTuplesFuture () throws InterruptedException
    {
        try
        {
            if ( downstreamTuplesFuture != null )
            {
                downstreamTuplesFuture.get();
                downstreamTuplesFuture = null;
            }
        }
        catch ( ExecutionException e )
        {
            LOGGER.error( id + ": await downstream tuple future failed", e );
        }
    }

    private void completeRun () throws InterruptedException
    {
        LOGGER.info( "{}: completing the run", id );
        awaitDownstreamTuplesFuture();
        LOGGER.info( "{}: all downstream tuples are sent", id );

        synchronized ( monitor )
        {
            status = COMPLETED;
            final PipelineInstanceRunnerCommand command = this.command;
            if ( command != null )
            {
                final PipelineInstanceRunnerCommandType type = command.getType();
                if ( type == RESUME || type == PAUSE )
                {
                    LOGGER.warn( "{}: completing command with type: {} exceptionally", id, type );
                    command.completeExceptionally( new IllegalStateException( id + " completed running!" ) );
                }
                else
                {
                    LOGGER.info( "{}: completing command with type: {}", id, UPDATE_PIPELINE_UPSTREAM_CONTEXT );
                    command.complete();
                }
                this.command = null;
            }
        }
    }


    enum PipelineInstanceRunnerCommandType
    {
        PAUSE,
        RESUME,
        UPDATE_PIPELINE_UPSTREAM_CONTEXT
    }


    private static class PipelineInstanceRunnerCommand
    {

        static PipelineInstanceRunnerCommand pause ()
        {
            return new PipelineInstanceRunnerCommand( PAUSE );
        }

        static PipelineInstanceRunnerCommand resume ()
        {
            return new PipelineInstanceRunnerCommand( RESUME );
        }

        static PipelineInstanceRunnerCommand updatePipelineUpstreamContext ()
        {
            return new PipelineInstanceRunnerCommand( UPDATE_PIPELINE_UPSTREAM_CONTEXT );
        }


        private final PipelineInstanceRunnerCommandType type;

        private final CompletableFuture<Void> future = new CompletableFuture<>();

        private PipelineInstanceRunnerCommand ( final PipelineInstanceRunnerCommandType type )
        {
            this.type = type;
        }

        PipelineInstanceRunnerCommandType getType ()
        {
            return type;
        }

        boolean hasType ( final PipelineInstanceRunnerCommandType type )
        {
            return this.type == type;
        }

        public CompletableFuture<Void> getFuture ()
        {
            return future;
        }

        void complete ()
        {
            future.complete( null );
        }

        void completeExceptionally ( final Throwable throwable )
        {
            checkNotNull( throwable );
            future.completeExceptionally( throwable );
        }

    }
}
