package cs.bilkent.zanza.engine.pipeline;

import java.util.Arrays;
import java.util.function.Supplier;
import javax.annotation.concurrent.NotThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import cs.bilkent.zanza.engine.exception.InitializationException;
import cs.bilkent.zanza.engine.kvstore.KVStoreContext;
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
import static cs.bilkent.zanza.operator.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByPort.ALL_PORTS;
import static cs.bilkent.zanza.operator.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByPort.ANY_PORT;
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

    private final KVStoreContext kvStoreContext;

    private final TupleQueueDrainerPool drainerPool;

    private final InvocationContextImpl invocationContext;

    private final Supplier<TuplesImpl> outputSupplier;

    private OperatorInstanceStatus status = INITIAL;

    private UpstreamContext upstreamContext;

    private UpstreamContext selfUpstreamContext;

    private InvocationReason completionReason;

    private Operator operator;

    private SchedulingStrategy initialSchedulingStrategy;

    private SchedulingStrategy schedulingStrategy;

    private TupleQueueDrainer drainer;

    private OperatorInstanceListener listener;

    private boolean invokedOnLastAttempt;

    public OperatorInstance ( final PipelineInstanceId pipelineInstanceId,
                              final OperatorDefinition operatorDefinition,
                              final TupleQueueContext queue,
                              final KVStoreContext kvStoreContext,
                              final TupleQueueDrainerPool drainerPool,
                              final Supplier<TuplesImpl> outputSupplier )
    {
        this( pipelineInstanceId, operatorDefinition, queue, kvStoreContext, drainerPool, outputSupplier, new InvocationContextImpl() );
    }

    public OperatorInstance ( final PipelineInstanceId pipelineInstanceId,
                              final OperatorDefinition operatorDefinition,
                              final TupleQueueContext queue,
                              final KVStoreContext kvStoreContext,
                              final TupleQueueDrainerPool drainerPool,
                              final Supplier<TuplesImpl> outputSupplier,
                              final InvocationContextImpl invocationContext )
    {
        this.operatorName = pipelineInstanceId.toString() + ".Operator<" + operatorDefinition.id() + ">";
        this.queue = queue;
        this.operatorDefinition = operatorDefinition;
        this.kvStoreContext = kvStoreContext;
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
    public SchedulingStrategy init ( final UpstreamContext upstreamContext, final OperatorInstanceListener listener )
    {
        checkState( status == INITIAL );
        try
        {
            this.listener = listener != null ? listener : ( operatorId, status1 ) -> {
            };

            operator = operatorDefinition.createOperator();
            checkState( operator != null, "operator implementation can not be null" );
            setUpstreamContext( upstreamContext );
            initializeOperator( upstreamContext );
            setSelfUpstreamContext( ACTIVE );

            setStatus( RUNNING );
            LOGGER.info( "{} initialized. Initial scheduling strategy: {}", operatorName, schedulingStrategy );
            return initialSchedulingStrategy;
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
            listener.onStatusChange( operatorDefinition.id(), status );
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
        final SchedulingStrategy schedulingStrategy = operator.init( initContext );
        verifySchedulingStrategy( schedulingStrategy, upstreamContext );
        this.schedulingStrategy = schedulingStrategy;
        initialSchedulingStrategy = this.schedulingStrategy;
        verifySchedulingStrategy( this.schedulingStrategy, upstreamContext );
        drainer = drainerPool.acquire( this.schedulingStrategy );
        if ( schedulingStrategy instanceof ScheduleWhenTuplesAvailable )
        {
            final ScheduleWhenTuplesAvailable ss = (ScheduleWhenTuplesAvailable) schedulingStrategy;
            queue.setTupleCounts( ss.getTupleCounts(), ss.getTupleAvailabilityByPort() );
        }
    }

    public boolean isInvokable ( final UpstreamContext upstreamContext )
    {
        try
        {
            verifySchedulingStrategy( initialSchedulingStrategy, upstreamContext );
            return true;
        }
        catch ( IllegalStateException e )
        {
            LOGGER.info( "{} not invokable anymore. scheduling strategy: {} upstream context: {} error: {}",
                         operatorName,
                         schedulingStrategy,
                         upstreamContext,
                         e.getMessage() );
            return false;
        }
    }

    private void verifySchedulingStrategy ( final SchedulingStrategy schedulingStrategy, final UpstreamContext upstreamContext )
    {
        checkArgument( operatorDefinition.inputPortCount() == upstreamContext.getPortCount() );

        if ( schedulingStrategy instanceof ScheduleWhenAvailable )
        {
            checkState( operatorDefinition.inputPortCount() == 0,
                        "%s cannot be used by operator: %s with input port count: %s",
                        ScheduleWhenAvailable.class.getSimpleName(),
                        operatorName,
                        operatorDefinition.inputPortCount() );
            checkState( upstreamContext.getVersion() == 0, "upstream context is closed for 0 input port operator: %s", operatorName );
        }
        else if ( schedulingStrategy instanceof ScheduleWhenTuplesAvailable )
        {
            checkState( operatorDefinition.inputPortCount() > 0,
                        "0 input port operator: %s cannot use %s",
                        operatorName,
                        ScheduleWhenTuplesAvailable.class.getSimpleName() );
            final ScheduleWhenTuplesAvailable s = (ScheduleWhenTuplesAvailable) schedulingStrategy;
            checkState( operatorDefinition.inputPortCount() == s.getPortCount(), "" );
            if ( s.getTupleAvailabilityByPort() == ANY_PORT )
            {
                boolean valid = false;
                for ( int i = 0; i < operatorDefinition.inputPortCount(); i++ )
                {
                    if ( s.getTupleCount( i ) > 0 && upstreamContext.getUpstreamConnectionStatus( i ) == ACTIVE )
                    {
                        valid = true;
                        break;
                    }
                }

                checkState( valid );
            }
            else if ( s.getTupleAvailabilityByPort() == ALL_PORTS )
            {
                for ( int i = 0; i < operatorDefinition.inputPortCount(); i++ )
                {
                    checkState( upstreamContext.getUpstreamConnectionStatus( i ) == ACTIVE );
                }
            }
            else
            {
                throw new IllegalStateException( s.toString() );
            }
        }
        else
        {
            throw new IllegalStateException( operatorName + " returns invalid initial scheduling strategy: " + schedulingStrategy );
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
     * - it makes the final invocation and moves the operator into {@link OperatorInstanceStatus#COMPLETED},
     * if the scheduling strategy is {@link ScheduleWhenAvailable}.
     * - if the scheduling strategy is {@link ScheduleWhenTuplesAvailable} and operator is still invokable with the new upstream context,
     * it skips the invocation.
     * - if the scheduling strategy is {@link ScheduleWhenTuplesAvailable} and operator is not invokable with the new upstream context
     * anymore, it invokes the operator with {@link InvocationReason#INPUT_PORT_CLOSED},
     * <p>
     * When the operator is in {@link OperatorInstanceStatus#COMPLETING} status:
     * It invokes the operator successfully if it can drain a non-empty input from the tuple queues. If there is no non-empty input:
     * - it performs the final invocation and moves the operator to {@link OperatorInstanceStatus#COMPLETED} status
     * if all input ports are closed.
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
        invokedOnLastAttempt = false;
        if ( status == COMPLETED )
        {
            return null;
        }

        checkState( status == RUNNING || status == COMPLETING );

        offer( upstreamInput );

        TuplesImpl input = drainQueueAndGetResult(), output = null;

        if ( status == RUNNING )
        {
            if ( schedulingStrategy instanceof ScheduleWhenAvailable )
            {
                if ( handleNewUpstreamContext( upstreamContext ) )
                {
                    output = invokeOperator( SHUTDOWN, input, drainer.getKey() );
                    completeRun();
                    completionReason = SHUTDOWN;
                }
                else
                {
                    output = invokeOperator( SUCCESS, input, drainer.getKey() );
                }
            }
            else if ( input != null )
            {
                output = invokeOperator( SUCCESS, input, drainer.getKey() );
            }
            else if ( handleNewUpstreamContext( upstreamContext ) && !isInvokable( upstreamContext ) )
            {
                setStatus( COMPLETING );
                completionReason = INPUT_PORT_CLOSED;
                setNewSchedulingStrategy( ScheduleWhenAvailable.INSTANCE );
                queue.prepareGreedyDraining();
                input = drainQueueAndGetResult();
                if ( input != null && input.isNonEmpty() )
                {
                    output = invokeOperator( INPUT_PORT_CLOSED, input, drainer.getKey() );
                }
            }
            else
            {
                drainer.reset();
            }
        }
        else if ( input != null && input.isNonEmpty() )
        {
            // status = COMPLETING
            output = invokeOperator( INPUT_PORT_CLOSED, input, drainer.getKey() );
        }
        else
        {
            // status = COMPLETING
            if ( handleNewUpstreamContext( upstreamContext ) )
            {
                output = invokeOperator( INPUT_PORT_CLOSED, new TuplesImpl( operatorDefinition.inputPortCount() ), null );
            }
            else
            {
                drainer.reset();
            }

            if ( upstreamContext.isActiveConnectionAbsent() )
            {
                completeRun();
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
    private TuplesImpl invokeOperator ( final InvocationReason reason, final TuplesImpl input, final Object key )
    {
        final KVStore kvStore = kvStoreContext.getKVStore( key );
        final TuplesImpl output = outputSupplier.get();
        invocationContext.setInvocationParameters( reason, input, output, kvStore );
        operator.invoke( invocationContext );
        drainer.reset();
        invokedOnLastAttempt = true;

        return output;
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
        checkArgument( upstreamContext != null );
        this.upstreamContext = upstreamContext;
        invocationContext.setUpstreamConnectionStatuses( upstreamContext.getUpstreamConnectionStatuses() );
    }

    /**
     * Finalizes the running schedule of the operator. It releases the drainer, sets the scheduling strategy to {@link ScheduleNever},
     * sets status to {@link OperatorInstanceStatus#COMPLETED} and updates the upstream context that will be passed to the next operator.
     */
    private void completeRun ()
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

    public SchedulingStrategy getInitialSchedulingStrategy ()
    {
        return initialSchedulingStrategy;
    }

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

    public KVStoreContext getKvStoreContext ()
    {
        return kvStoreContext;
    }

    public TupleQueueContext getQueue ()
    {
        return queue;
    }

    public TupleQueueDrainerPool getDrainerPool ()
    {
        return drainerPool;
    }

    public TupleQueueDrainer getDrainer ()
    {
        return drainer;
    }

    public Supplier<TuplesImpl> getOutputSupplier ()
    {
        return outputSupplier;
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

    public boolean isInvokedOnLastAttempt ()
    {
        return invokedOnLastAttempt;
    }

}
