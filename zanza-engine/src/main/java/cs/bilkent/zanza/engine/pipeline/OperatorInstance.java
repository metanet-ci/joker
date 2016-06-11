package cs.bilkent.zanza.engine.pipeline;

import java.util.Arrays;
import java.util.function.Supplier;
import javax.annotation.concurrent.NotThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkState;
import cs.bilkent.zanza.engine.config.ZanzaConfig;
import cs.bilkent.zanza.engine.exception.InitializationException;
import cs.bilkent.zanza.engine.kvstore.KVStoreProvider;
import static cs.bilkent.zanza.engine.pipeline.OperatorInstanceStatus.COMPLETED;
import static cs.bilkent.zanza.engine.pipeline.OperatorInstanceStatus.COMPLETING;
import static cs.bilkent.zanza.engine.pipeline.OperatorInstanceStatus.INITIAL;
import static cs.bilkent.zanza.engine.pipeline.OperatorInstanceStatus.INITIALIZATION_FAILED;
import static cs.bilkent.zanza.engine.pipeline.OperatorInstanceStatus.RUNNING;
import static cs.bilkent.zanza.engine.pipeline.OperatorInstanceStatus.SHUT_DOWN;
import static cs.bilkent.zanza.engine.pipeline.UpstreamConnectionStatus.ACTIVE;
import static cs.bilkent.zanza.engine.pipeline.UpstreamConnectionStatus.CLOSED;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueueContext;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueueDrainer;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueueDrainerPool;
import cs.bilkent.zanza.flow.FlowDefinition;
import cs.bilkent.zanza.flow.OperatorDefinition;
import cs.bilkent.zanza.operator.InitializationContext;
import cs.bilkent.zanza.operator.InvocationContext.InvocationReason;
import static cs.bilkent.zanza.operator.InvocationContext.InvocationReason.INPUT_PORT_CLOSED;
import static cs.bilkent.zanza.operator.InvocationContext.InvocationReason.OPERATOR_REQUESTED_SHUTDOWN;
import static cs.bilkent.zanza.operator.InvocationContext.InvocationReason.SHUTDOWN;
import static cs.bilkent.zanza.operator.InvocationContext.InvocationReason.SUCCESS;
import cs.bilkent.zanza.operator.Operator;
import cs.bilkent.zanza.operator.impl.InitializationContextImpl;
import cs.bilkent.zanza.operator.impl.InvocationContextImpl;
import cs.bilkent.zanza.operator.impl.TuplesImpl;
import cs.bilkent.zanza.operator.kvstore.KVStore;
import cs.bilkent.zanza.operator.scheduling.ScheduleNever;
import cs.bilkent.zanza.operator.scheduling.ScheduleWhenAvailable;
import cs.bilkent.zanza.operator.scheduling.ScheduleWhenTuplesAvailable;
import static cs.bilkent.zanza.operator.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByPort.AVAILABLE_ON_ALL_PORTS;
import static cs.bilkent.zanza.operator.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByPort.AVAILABLE_ON_ANY_PORT;
import cs.bilkent.zanza.operator.scheduling.SchedulingStrategy;

/**
 * Manages runtime state of an {@link Operator} defined in a {@link FlowDefinition} and provides methods for operator invocation.
 * Holds the actual instance of user-defined {@link Operator} implementation and all necessary internal state required for operator
 * invocation, such as input tuple queues, key-value store etc.
 * <p>
 * Reflects the life-cycle defined in {@link Operator} interface and provides the corresponding methods.
 */
@NotThreadSafe
public class OperatorInstance
{

    private static Logger LOGGER = LoggerFactory.getLogger( OperatorInstance.class );


    private final String operatorName;

    private final OperatorDefinition operatorDefinition;

    private final TupleQueueContext queue;

    private final KVStoreProvider kvStoreProvider;

    private final TupleQueueDrainerPool drainerPool;

    private final InvocationContextImpl invocationContext;

    private final Supplier<TuplesImpl> outputSupplier;

    private OperatorInstanceStatus status = INITIAL;

    private UpstreamContext upstreamContext;

    private UpstreamContext selfUpstreamContext;

    private InvocationReason completionReason;

    private Operator operator;

    private SchedulingStrategy schedulingStrategy;

    private TupleQueueDrainer drainer;

    private OperatorInstanceLifecycleListener lifecycleListener;

    public OperatorInstance ( final PipelineInstanceId pipelineInstanceId,
                              final OperatorDefinition operatorDefinition,
                              final TupleQueueContext queue,
                              final KVStoreProvider kvStoreProvider,
                              final TupleQueueDrainerPool drainerPool,
                              final Supplier<TuplesImpl> outputSupplier )
    {
        this( pipelineInstanceId, operatorDefinition, queue, kvStoreProvider, drainerPool, outputSupplier, new InvocationContextImpl() );
    }

    public OperatorInstance ( final PipelineInstanceId pipelineInstanceId,
                              final OperatorDefinition operatorDefinition,
                              final TupleQueueContext queue,
                              final KVStoreProvider kvStoreProvider,
                              final TupleQueueDrainerPool drainerPool,
                              final Supplier<TuplesImpl> outputSupplier,
                              final InvocationContextImpl invocationContext )
    {
        this.operatorName = pipelineInstanceId.toString() + ".Operator<" + operatorDefinition.id() + ">";
        this.queue = queue;
        this.operatorDefinition = operatorDefinition;
        this.kvStoreProvider = kvStoreProvider;
        this.drainerPool = drainerPool;
        this.invocationContext = invocationContext;
        this.outputSupplier = outputSupplier;
    }

    /**
     * Initializes its internal state to get ready for operator invocation. After initialization is completed successfully, it moves
     * the status to {@link OperatorInstanceStatus#RUNNING}. If {@link Operator#init(InitializationContext)} throws an exception,
     * it moves the status to {@link OperatorInstanceStatus#INITIALIZATION_FAILED} and propagates the exception to the caller after
     * wrapping it with {@link InitializationException}.
     */
    public void init ( final ZanzaConfig config,
                       final UpstreamContext upstreamContext,
                       final OperatorInstanceLifecycleListener lifecycleListener )
    {
        checkState( status == INITIAL );
        try
        {
            this.lifecycleListener = lifecycleListener != null ? lifecycleListener : ( operatorName, status1 ) -> {
            };

            drainerPool.init( config );
            operator = operatorDefinition.createOperator();
            checkState( operator != null, "operator implementation can not be null" );
            setUpstreamContext( upstreamContext );
            initializeOperator( upstreamContext );
            setSelfUpstreamContext( ACTIVE );

            setStatus( RUNNING );
            LOGGER.info( "{} initialized. Initial scheduling strategy: {}", operatorName, schedulingStrategy );
        }
        catch ( Exception e )
        {
            setSelfUpstreamContext( CLOSED );
            setStatus( INITIALIZATION_FAILED );
            throw new InitializationException( "Operator " + operatorName + " initialization failed!", e );
        }
    }

    private void setStatus ( final OperatorInstanceStatus status )
    {
        try
        {
            this.status = status;
            lifecycleListener.onChange( operatorDefinition.id(), status );
        }
        catch ( Exception e )
        {
            LOGGER.error( operatorName + " lifecycle listener failed for status change to " + status, e );
        }
    }

    /**
     * Initializes the operator and acquires the drainer with the scheduling strategy provided by the operator.
     * Initial scheduling strategy is also verified.
     */
    private void initializeOperator ( final UpstreamContext upstreamContext )
    {
        final boolean[] upstreamConnectionStatuses = upstreamContext.getUpstreamConnectionStatuses();
        final InitializationContext initContext = new InitializationContextImpl( operatorDefinition.id(),
                                                                                 operatorDefinition.inputPortCount(),
                                                                                 operatorDefinition.outputPortCount(),
                                                                                 operatorDefinition.partitionFieldNames(),
                                                                                 operatorDefinition.schema(),
                                                                                 operatorDefinition.config(),
                                                                                 upstreamConnectionStatuses );
        schedulingStrategy = operator.init( initContext );
        if ( validateSchedulingStrategy( schedulingStrategy ) )
        {
            drainer = drainerPool.acquire( schedulingStrategy );
        }
        else
        {
            throw new IllegalStateException( "Operator " + operatorName + " returned invalid scheduling strategy " + schedulingStrategy );
        }
    }

    /**
     * Sets a new upstream context with an increment version.
     * It assigns the given {@link UpstreamConnectionStatus} value to all of its output ports.
     */
    private void setSelfUpstreamContext ( final UpstreamConnectionStatus status )
    {
        final int version = selfUpstreamContext != null ? selfUpstreamContext.getVersion() + 1 : 0;
        final UpstreamConnectionStatus[] selfStatuses = new UpstreamConnectionStatus[ operatorDefinition.outputPortCount() ];
        Arrays.fill( selfStatuses, 0, selfStatuses.length, status );
        selfUpstreamContext = new UpstreamContext( version, selfStatuses );
    }

    /**
     * Performs the operator invocation as described below.
     * <p>
     * When the operator is in {@link OperatorInstanceStatus#RUNNING} status:
     * invokes the operator successfully if
     * - the operator has a non-empty input for its {@link ScheduleWhenTuplesAvailable} scheduling strategy,
     * - scheduling strategy is {@link ScheduleWhenAvailable} and there is no change upstream context.
     * Otherwise, it checks if there is a change in the upstream context. If it is the case,
     * - it sets the scheduling strategy to {@link ScheduleWhenAvailable}, drains the queue and invokes the operator. If all input ports
     * are closed, it moves the status into {@link OperatorInstanceStatus#COMPLETING} and does not allow the operator to provide a new
     * scheduling strategy.
     * <p>
     * When the operator is in {@link OperatorInstanceStatus#COMPLETING} status:
     * It invokes the operator successfully if it can drain a non-empty input from the tuple queues. If there is no non-empty input:
     * - for {@link ScheduleWhenAvailable} scheduling strategy, it means that the operator consumed all of the input and completed the run.
     * Then, it is finalized.
     * - for {@link ScheduleWhenTuplesAvailable} scheduling strategy, it sets the scheduling strategy to {@link ScheduleWhenAvailable} and
     * tries to drain a non-empty input from the tuple queues to invoke the operator. If there is no non-empty input again, it means that
     * the operator consumed all of the input and completed the run, so the operator is finalized.
     * <p>
     * An operator may move to {@link OperatorInstanceStatus#COMPLETING} status when it returns {@link ScheduleNever} as new scheduling
     * strategy or all of its input ports are closed.
     *
     * @param upstreamInput
     *         input of the operator which is sent by the upstream operator
     * @param upstreamContext
     *         status of the upstream connections
     *
     * @return output of the operator invocation
     */
    public TuplesImpl invoke ( final TuplesImpl upstreamInput, final UpstreamContext upstreamContext )
    {
        if ( status == COMPLETED )
        {
            return null;
        }

        checkState( status == RUNNING || status == COMPLETING );

        offer( upstreamInput );
        TuplesImpl output = null;

        if ( status == RUNNING )
        {
            TuplesImpl input = drainQueueAndGetResult();
            if ( input != null && ( !( schedulingStrategy instanceof ScheduleWhenAvailable )
                                    || this.upstreamContext.getVersion() == upstreamContext.getVersion() ) )
            {
                output = invokeOperator( SUCCESS, input, drainer.getKey(), true );
            }
            else if ( handleNewUpstreamContext( upstreamContext ) )
            {
                InvocationReason reason;
                final boolean activeConnectionAbsent = operatorDefinition.inputPortCount() > 0
                                                       ? upstreamContext.isActiveConnectionAbsent()
                                                       : upstreamContext.getVersion() > 0;
                if ( activeConnectionAbsent )
                {
                    setStatus( COMPLETING );
                    completionReason = operatorDefinition.inputPortCount() > 0 ? INPUT_PORT_CLOSED : SHUTDOWN;
                    reason = completionReason;
                }
                else
                {
                    reason = INPUT_PORT_CLOSED;
                }

                if ( !( schedulingStrategy instanceof ScheduleWhenAvailable ) )
                {
                    setNewSchedulingStrategy( ScheduleWhenAvailable.INSTANCE );
                    input = drainQueueAndGetResult();
                }

                output = invokeOperator( reason, input, drainer.getKey(), !activeConnectionAbsent );
            }
            else
            {
                drainer.reset();
            }
        }
        else
        {
            // status = COMPLETING
            handleNewUpstreamContext( upstreamContext );

            TuplesImpl input = drainQueueAndGetResult();
            if ( input != null && input.isNonEmpty() )
            {
                output = invokeOperator( SUCCESS, input, drainer.getKey(), false );
            }
            else if ( schedulingStrategy instanceof ScheduleWhenAvailable )
            {
                finalizeRun();
            }
            else
            {
                setNewSchedulingStrategy( ScheduleWhenAvailable.INSTANCE );
                input = drainQueueAndGetResult();
                if ( input != null && input.isNonEmpty() )
                {
                    output = invokeOperator( completionReason, input, drainer.getKey(), false );
                }
                else
                {
                    finalizeRun();
                }
            }
        }

        return output;
    }

    private void offer ( final TuplesImpl input )
    {
        if ( input != null )
        {
            for ( int portIndex = 0; portIndex < input.getPortCount(); portIndex++ )
            {
                queue.offer( portIndex, input.getTuplesModifiable( portIndex ) );
            }
        }
    }

    private TuplesImpl drainQueueAndGetResult ()
    {
        queue.drain( drainer );
        return drainer.getResult();
    }

    /**
     * Invokes the operator, resets the drainer and handles the new scheduling strategy if allowed.
     */
    private TuplesImpl invokeOperator ( final InvocationReason reason,
                                        final TuplesImpl input,
                                        final Object key,
                                        final boolean handleNewSchedulingStrategy )
    {
        final KVStore kvStore = kvStoreProvider.getKVStore( key );
        final TuplesImpl output = outputSupplier.get();
        invocationContext.setInvocationParameters( reason, input, output, kvStore );
        operator.invoke( invocationContext );
        drainer.reset();

        final SchedulingStrategy newSchedulingStrategy = invocationContext.getNewSchedulingStrategy();
        if ( newSchedulingStrategy != null )
        {
            invocationContext.setNewSchedulingStrategy( null );
            if ( handleNewSchedulingStrategy )
            {
                handleNewSchedulingStrategy( newSchedulingStrategy );
            }
            else
            {
                LOGGER.warn( "{} not handling new {} with {}", operatorName, newSchedulingStrategy, upstreamContext );
            }
        }

        return output;
    }

    /**
     * Handles the new scheduling strategy returned by operator invocation. If it is a valid scheduling strategy, updates the scheduling
     * strategy of the operator. Otherwise, it moves the operator status into {@link OperatorInstanceStatus#COMPLETING} and sets the
     * completion reason as {@link InvocationReason#OPERATOR_REQUESTED_SHUTDOWN}.
     */
    private void handleNewSchedulingStrategy ( final SchedulingStrategy newSchedulingStrategy )
    {
        if ( validateSchedulingStrategy( newSchedulingStrategy ) )
        {
            setNewSchedulingStrategy( newSchedulingStrategy );
            ensureQueueCapacity();
        }
        else
        {
            LOGGER.info( "Moving to {} status since {} requested to shutdown", COMPLETING, operatorName );
            completionReason = OPERATOR_REQUESTED_SHUTDOWN;
            setStatus( COMPLETING );
        }
    }

    /**
     * Validates the scheduling strategy given by the operator. Returns true if it is valid and assignable for the operator
     * A scheduling strategy is valid if it is {@link ScheduleWhenAvailable} or {@link ScheduleWhenTuplesAvailable} with tuple requirements
     * that match to the operator's upstream connection statuses
     */
    private boolean validateSchedulingStrategy ( final SchedulingStrategy schedulingStrategy )
    {
        if ( schedulingStrategy == null )
        {
            LOGGER.error( "{} returns null scheduling strategy", operatorName );
            return false;
        }
        else if ( schedulingStrategy instanceof ScheduleNever )
        {
            LOGGER.info( "{} returns new scheduling strategy as {}", operatorName, ScheduleNever.class.getSimpleName() );
            return false;
        }
        else if ( schedulingStrategy instanceof ScheduleWhenTuplesAvailable )
        {
            final int inputPortCount = operatorDefinition.inputPortCount();
            if ( inputPortCount == 0 )
            {
                LOGGER.error( "{} has no input port but requested {}", operatorName, schedulingStrategy );
                return false;
            }

            // TODO THIS PART IS BROKEN. WE SHOULD HANDLE ScheduleWhenTuplesAvailable CORRECTLY
            final ScheduleWhenTuplesAvailable scheduleWhenTuplesAvailable = (ScheduleWhenTuplesAvailable) schedulingStrategy;

            if ( scheduleWhenTuplesAvailable.getPortCount() != inputPortCount )
            {
                LOGGER.error( " {} cannot satisfy {} since input port count is different than operator's input port count: {}",
                              operatorName,
                              scheduleWhenTuplesAvailable,
                              inputPortCount );
                return false;
            }

            if ( scheduleWhenTuplesAvailable.getTupleAvailabilityByPort() == AVAILABLE_ON_ANY_PORT )
            {
                if ( upstreamContext.isActiveConnectionAbsent() )
                {
                    LOGGER.error( "Cannot satisfy {} since there is no active upstream connection of {}",
                                  scheduleWhenTuplesAvailable,
                                  operatorName );
                    return false;
                }
            }
            else if ( scheduleWhenTuplesAvailable.getTupleAvailabilityByPort() == AVAILABLE_ON_ALL_PORTS )
            {
                if ( upstreamContext.getActiveConnectionCount() != inputPortCount )
                {
                    LOGGER.error( "Cannot satisfy {} since all upstream connections of {} are not active",
                                  scheduleWhenTuplesAvailable,
                                  operatorName );
                    return false;
                }
            }
            else
            {
                throw new IllegalArgumentException( schedulingStrategy.toString() );
            }
        }

        return true;
    }

    /**
     * Updates the scheduling strategy of the operator with new scheduling strategy given in the argument.
     * It also releases the drainer of the previous scheduling strategy and acquires a new one for the new scheduling strategy.
     */
    private void setNewSchedulingStrategy ( final SchedulingStrategy newSchedulingStrategy )
    {
        if ( drainer != null )
        {
            LOGGER.info( "{} setting new scheduling strategy: {} old scheduling strategy: {}",
                         operatorName,
                         newSchedulingStrategy,
                         schedulingStrategy );
            drainerPool.release( drainer );
        }
        else
        {
            LOGGER.info( "{} setting new scheduling strategy: {}", operatorName, newSchedulingStrategy );
        }
        schedulingStrategy = newSchedulingStrategy;
        drainer = drainerPool.acquire( schedulingStrategy );
    }

    /**
     * If scheduling strategy of the operator is set to {@link ScheduleWhenTuplesAvailable}, it makes sure that tuple queues have enough
     * internal capacity to satisfy the requested tuple counts.
     */
    private void ensureQueueCapacity ()
    {
        if ( schedulingStrategy instanceof ScheduleWhenTuplesAvailable )
        {
            final ScheduleWhenTuplesAvailable scheduleWhenTuplesAvailable = (ScheduleWhenTuplesAvailable) schedulingStrategy;
            for ( int portIndex = 0; portIndex < scheduleWhenTuplesAvailable.getPortCount(); portIndex++ )
            {
                queue.ensureCapacity( portIndex, scheduleWhenTuplesAvailable.getTupleCount( portIndex ) );
            }
        }
    }

    /**
     * Sets the new upstream context if it has a higher version than the current one.
     *
     * @return true if current upstream context is updated.
     */
    private boolean handleNewUpstreamContext ( final UpstreamContext upstreamContext )
    {
        final boolean isNew = this.upstreamContext.getVersion() < upstreamContext.getVersion();
        if ( isNew )
        {
            setUpstreamContext( upstreamContext );
        }

        return isNew;
    }

    /**
     * Sets the new upstream context and updates the upstream connection statuses of the invocation context
     */
    private void setUpstreamContext ( final UpstreamContext upstreamContext )
    {
        this.upstreamContext = upstreamContext;
        invocationContext.setUpstreamConnectionStatuses( upstreamContext.getUpstreamConnectionStatuses() );
    }

    /**
     * Finalizes the running schedule of the operator. It releases the drainer, sets the scheduling strategy to {@link ScheduleNever},
     * sets status to {@link OperatorInstanceStatus#COMPLETED} and updates the upstream context that will be passed to the next operator.
     */
    private void finalizeRun ()
    {
        if ( drainer != null )
        {
            drainerPool.release( drainer );
            drainer = null;
        }
        schedulingStrategy = ScheduleNever.INSTANCE;
        setStatus( COMPLETED );
        setSelfUpstreamContext( CLOSED );
    }

    /**
     * Shuts down the operator and sets status to {@link OperatorInstanceStatus#SHUT_DOWN}
     */
    public void shutdown ()
    {
        // TODO should we clear internal queues and kv store here?

        if ( status == INITIAL )
        {
            LOGGER.info( "{} ignoring shutdown request since not initialized", operatorName );
            return;
        }
        else if ( status == SHUT_DOWN )
        {
            LOGGER.info( "{} ignoring shutdown request since already shut down", operatorName );
            return;
        }

        checkState( status == RUNNING || status == INITIALIZATION_FAILED );
        try
        {
            if ( operator != null )
            {
                operator.shutdown();
            }
        }
        catch ( Exception e )
        {
            LOGGER.error( operatorName + " failed to shut down", e );
        }
        finally
        {
            setStatus( SHUT_DOWN );
            operator = null;
            drainer = null;
        }
    }

    public OperatorDefinition getOperatorDefinition ()
    {
        return operatorDefinition;
    }

    /**
     * Returns the last scheduling strategy returned by operator invocation
     *
     * @return the last scheduling strategy returned by operator invocation
     */
    public SchedulingStrategy getSchedulingStrategy ()
    {
        return schedulingStrategy;
    }

    public OperatorInstanceStatus getStatus ()
    {
        return status;
    }

    public String getOperatorName ()
    {
        return operatorName;
    }

    public Operator getOperator ()
    {
        return operator;
    }

    public boolean isInvokable ()
    {
        return status == RUNNING || status == COMPLETING;
    }

    public boolean isNonInvokable ()
    {
        return !isInvokable();
    }

    public InvocationReason getCompletionReason ()
    {
        return completionReason;
    }

    public UpstreamContext getUpstreamContext ()
    {
        return upstreamContext;
    }

    public UpstreamContext getSelfUpstreamContext ()
    {
        return selfUpstreamContext;
    }

}
