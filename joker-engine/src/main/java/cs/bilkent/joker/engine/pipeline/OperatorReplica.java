package cs.bilkent.joker.engine.pipeline;

import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;
import javax.annotation.concurrent.NotThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import cs.bilkent.joker.engine.exception.InitializationException;
import cs.bilkent.joker.engine.kvstore.OperatorKVStore;
import cs.bilkent.joker.engine.metric.PipelineReplicaMeter;
import cs.bilkent.joker.engine.partition.PartitionKey;
import static cs.bilkent.joker.engine.pipeline.OperatorReplicaStatus.COMPLETED;
import static cs.bilkent.joker.engine.pipeline.OperatorReplicaStatus.COMPLETING;
import static cs.bilkent.joker.engine.pipeline.OperatorReplicaStatus.INITIAL;
import static cs.bilkent.joker.engine.pipeline.OperatorReplicaStatus.INITIALIZATION_FAILED;
import static cs.bilkent.joker.engine.pipeline.OperatorReplicaStatus.RUNNING;
import static cs.bilkent.joker.engine.pipeline.OperatorReplicaStatus.SHUT_DOWN;
import static cs.bilkent.joker.engine.pipeline.UpstreamConnectionStatus.ACTIVE;
import static cs.bilkent.joker.engine.pipeline.UpstreamConnectionStatus.CLOSED;
import cs.bilkent.joker.engine.tuplequeue.OperatorTupleQueue;
import cs.bilkent.joker.engine.tuplequeue.TupleQueueDrainer;
import cs.bilkent.joker.engine.tuplequeue.TupleQueueDrainerPool;
import static cs.bilkent.joker.engine.util.ExceptionUtils.checkInterruption;
import cs.bilkent.joker.flow.FlowDef;
import cs.bilkent.joker.operator.InitializationContext;
import cs.bilkent.joker.operator.InvocationContext.InvocationReason;
import static cs.bilkent.joker.operator.InvocationContext.InvocationReason.INPUT_PORT_CLOSED;
import static cs.bilkent.joker.operator.InvocationContext.InvocationReason.SHUTDOWN;
import static cs.bilkent.joker.operator.InvocationContext.InvocationReason.SUCCESS;
import cs.bilkent.joker.operator.Operator;
import cs.bilkent.joker.operator.OperatorDef;
import cs.bilkent.joker.operator.Tuple;
import cs.bilkent.joker.operator.impl.InitializationContextImpl;
import cs.bilkent.joker.operator.impl.InvocationContextImpl;
import cs.bilkent.joker.operator.impl.TuplesImpl;
import cs.bilkent.joker.operator.kvstore.KVStore;
import cs.bilkent.joker.operator.scheduling.ScheduleNever;
import cs.bilkent.joker.operator.scheduling.ScheduleWhenAvailable;
import cs.bilkent.joker.operator.scheduling.ScheduleWhenTuplesAvailable;
import static cs.bilkent.joker.operator.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByPort.ANY_PORT;
import cs.bilkent.joker.operator.scheduling.SchedulingStrategy;
import static java.lang.Math.min;

/**
 * Manages runtime state of an {@link Operator} defined in a {@link FlowDef} and provides methods for operator invocation.
 * Holds the actual instance of user-defined {@link Operator} implementation and all necessary internal state required for operator
 * invocation, such as input tuple queues, key-value store etc.
 * <p>
 * Reflects the life-cycle defined in {@link Operator} interface and provides the corresponding methods.
 */
@NotThreadSafe
public class OperatorReplica
{

    private static final Logger LOGGER = LoggerFactory.getLogger( OperatorReplica.class );


    private final PipelineReplicaId pipelineReplicaId;

    private final String operatorName;

    private final OperatorDef operatorDef;

    private final OperatorTupleQueue queue;

    private final OperatorKVStore operatorKvStore;

    private final TupleQueueDrainerPool drainerPool;

    private final InvocationContextImpl invocationContext;

    private final Supplier<TuplesImpl> outputSupplier;

    private final PipelineReplicaMeter meter;

    private OperatorReplicaStatus status = INITIAL;

    private UpstreamContext upstreamContext;

    private UpstreamContext selfUpstreamContext;

    private InvocationReason completionReason;

    private Operator operator;

    private SchedulingStrategy initialSchedulingStrategy;

    private SchedulingStrategy schedulingStrategy;

    private TupleQueueDrainer drainer;

    private OperatorReplicaListener listener = ( operatorId, status1 ) ->
    {
    };

    private boolean operatorInvokedOnLastAttempt;

    public OperatorReplica ( final PipelineReplicaId pipelineReplicaId,
                             final OperatorDef operatorDef,
                             final OperatorTupleQueue queue,
                             final OperatorKVStore operatorKvStore,
                             final TupleQueueDrainerPool drainerPool,
                             final Supplier<TuplesImpl> outputSupplier,
                             final PipelineReplicaMeter meter )
    {
        this( pipelineReplicaId, operatorDef, queue, operatorKvStore, drainerPool, outputSupplier, meter, new InvocationContextImpl() );
    }

    public OperatorReplica ( final PipelineReplicaId pipelineReplicaId,
                             final OperatorDef operatorDef,
                             final OperatorTupleQueue queue,
                             final OperatorKVStore operatorKvStore,
                             final TupleQueueDrainerPool drainerPool,
                             final Supplier<TuplesImpl> outputSupplier,
                             final PipelineReplicaMeter meter,
                             final InvocationContextImpl invocationContext )
    {
        this.pipelineReplicaId = pipelineReplicaId;
        this.operatorName = pipelineReplicaId.toString() + ".Operator<" + operatorDef.getId() + ">";
        this.queue = queue;
        this.operatorDef = operatorDef;
        this.operatorKvStore = operatorKvStore;
        this.drainerPool = drainerPool;
        this.outputSupplier = outputSupplier;
        this.meter = meter;
        this.invocationContext = invocationContext;
    }

    /**
     * Initializes its internal state to get ready for operator invocation. After initialization is completed successfully, it moves
     * the status to {@link OperatorReplicaStatus#RUNNING}. If {@link Operator#init(InitializationContext)} throws an exception,
     * it moves the status to {@link OperatorReplicaStatus#INITIALIZATION_FAILED} and propagates the exception to the caller after
     * wrapping it with {@link InitializationException}.
     */
    public SchedulingStrategy init ( final UpstreamContext upstreamContext )
    {
        checkState( status == INITIAL, "Cannot initialize Operator %s as it is in %s state", operatorName, status );
        try
        {
            operator = operatorDef.createOperator();
            checkState( operator != null, "Operator %s implementation can not be null", operatorName );
            setUpstreamContext( upstreamContext );
            initializeOperator( upstreamContext );
            setSelfUpstreamContext( ACTIVE );

            setStatus( RUNNING );
            LOGGER.info( "{} initialized. Initial scheduling strategy: {} Drainer: {}",
                         operatorName,
                         schedulingStrategy,
                         drainer.getClass().getSimpleName() );
            return initialSchedulingStrategy;
        }
        catch ( Exception e )
        {
            setSelfUpstreamContext( CLOSED );
            setStatus( INITIALIZATION_FAILED );
            throw new InitializationException( "Operator " + operatorName + " initialization failed!", e );
        }
    }

    private void setStatus ( final OperatorReplicaStatus status )
    {
        this.status = status;
        listener.onStatusChange( operatorDef.getId(), status );
    }

    /**
     * Initializes the operator and acquires the drainer with the scheduling strategy provided by the operator.
     * Initial scheduling strategy is also verified.
     */
    private void initializeOperator ( final UpstreamContext upstreamContext )
    {
        final boolean[] upstreamConnectionStatuses = upstreamContext.getUpstreamConnectionStatuses( operatorDef.getInputPortCount() );
        final InitializationContext initContext = new InitializationContextImpl( operatorDef, upstreamConnectionStatuses );
        final SchedulingStrategy schedulingStrategy = operator.init( initContext );
        upstreamContext.verifyOrFail( operatorDef, schedulingStrategy );
        this.initialSchedulingStrategy = schedulingStrategy;
        setNewSchedulingStrategy( schedulingStrategy );
        setQueueTupleCounts( schedulingStrategy );
    }

    private void setQueueTupleCounts ( final SchedulingStrategy schedulingStrategy )
    {
        if ( schedulingStrategy instanceof ScheduleWhenTuplesAvailable )
        {
            final ScheduleWhenTuplesAvailable ss = (ScheduleWhenTuplesAvailable) schedulingStrategy;
            queue.setTupleCounts( ss.getTupleCounts(), ss.getTupleAvailabilityByPort() );
        }
    }

    /**
     * Sets a new upstream context with an increment version.
     * It assigns the given {@link UpstreamConnectionStatus} value to all of its output ports.
     */
    private void setSelfUpstreamContext ( final UpstreamConnectionStatus status )
    {
        final int version = selfUpstreamContext != null ? selfUpstreamContext.getVersion() + 1 : 0;
        final UpstreamConnectionStatus[] selfStatuses = new UpstreamConnectionStatus[ operatorDef.getOutputPortCount() ];
        Arrays.fill( selfStatuses, 0, selfStatuses.length, status );
        selfUpstreamContext = new UpstreamContext( version, selfStatuses );
    }

    public TuplesImpl invoke ( final TuplesImpl upstreamInput, final UpstreamContext upstreamContext )
    {
        return invoke( false, upstreamInput, upstreamContext );
    }

    /**
     * Performs the operator invocation as described below.
     * <p>
     * When the operator is in {@link OperatorReplicaStatus#RUNNING} status:
     * invokes the operator successfully if
     * - the operator has a non-empty input for its {@link ScheduleWhenTuplesAvailable} scheduling strategy,
     * - scheduling strategy is {@link ScheduleWhenAvailable} and there is no change upstream context.
     * Otherwise, it checks if there is a change in the upstream context. If it is the case,
     * - it makes the final invocation and moves the operator into {@link OperatorReplicaStatus#COMPLETED},
     * if the scheduling strategy is {@link ScheduleWhenAvailable}.
     * - if the scheduling strategy is {@link ScheduleWhenTuplesAvailable} and operator is still invokable with the new upstream context,
     * it skips the invocation.
     * - if the scheduling strategy is {@link ScheduleWhenTuplesAvailable} and operator is not invokable with the new upstream context
     * anymore, it invokes the operator with {@link InvocationReason#INPUT_PORT_CLOSED},
     * <p>
     * When the operator is in {@link OperatorReplicaStatus#COMPLETING} status:
     * It invokes the operator successfully if it can drain a non-empty input from the tuple queues. If there is no non-empty input:
     * - it performs the final invocation and moves the operator to {@link OperatorReplicaStatus#COMPLETED} status
     * if all input ports are closed.
     *
     * @param drainerMaySkipBlocking
     *         a boolean flag which is passed to drainer to specify if the drainer may not block for the current invocation if it is a
     *         blocking drainer
     * @param upstreamInput
     *         input of the operator which is sent by the upstream operator
     * @param upstreamContext
     *         status of the upstream connections
     *
     * @return output of the operator invocation
     */
    public TuplesImpl invoke ( final boolean drainerMaySkipBlocking, final TuplesImpl upstreamInput, final UpstreamContext upstreamContext )
    {
        operatorInvokedOnLastAttempt = false;

        if ( status == COMPLETED )
        {
            return null;
        }

        checkState( status == RUNNING || status == COMPLETING, operatorName );

        offer( upstreamInput );

        TuplesImpl input, output = null;

        final boolean singleInvocation = !( meter.isTicked() && queue.isOverloaded() );

        while ( true )
        {
            input = drainQueue( drainerMaySkipBlocking );
            boolean invoked = true;
            if ( status == RUNNING )
            {
                if ( schedulingStrategy instanceof ScheduleWhenAvailable )
                {
                    if ( handleNewUpstreamContext( upstreamContext ) )
                    {
                        output = invokeOperator( SHUTDOWN, input, output, drainer.getKey() );
                        completeRun();
                        completionReason = SHUTDOWN;
                    }
                    else
                    {
                        output = invokeOperator( SUCCESS, input, output, drainer.getKey() );
                    }
                }
                else if ( input != null )
                {
                    output = invokeOperator( SUCCESS, input, output, drainer.getKey() );
                }
                else if ( handleNewUpstreamContext( upstreamContext ) && !upstreamContext.isInvokable( operatorDef,
                                                                                                       initialSchedulingStrategy ) )
                {
                    setStatus( COMPLETING );
                    completionReason = INPUT_PORT_CLOSED;
                    setNewSchedulingStrategy( ScheduleWhenAvailable.INSTANCE );
                    setQueueTupleCountsForGreedyDraining();
                    input = drainQueue( drainerMaySkipBlocking );
                    if ( input != null && input.isNonEmpty() )
                    {
                        output = invokeOperator( INPUT_PORT_CLOSED, input, output, drainer.getKey() );
                    }
                    else
                    {
                        invoked = false;
                    }
                }
                else
                {
                    invoked = false;
                }
            }
            else if ( input != null && input.isNonEmpty() )
            {
                // status = COMPLETING
                output = invokeOperator( INPUT_PORT_CLOSED, input, output, drainer.getKey() );
            }
            else
            {
                // status = COMPLETING
                if ( handleNewUpstreamContext( upstreamContext ) )
                {
                    output = invokeOperator( INPUT_PORT_CLOSED, new TuplesImpl( operatorDef.getInputPortCount() ), output, null );
                }
                else
                {
                    invoked = false;
                }

                if ( upstreamContext.isActiveConnectionAbsent() )
                {
                    completeRun();
                }
            }

            operatorInvokedOnLastAttempt |= invoked;

            if ( singleInvocation || !invoked || status == COMPLETED )
            {
                break;
            }
        }

        return output;
    }

    private void setQueueTupleCountsForGreedyDraining ()
    {
        final int[] tupleCounts = new int[ operatorDef.getInputPortCount() ];
        Arrays.fill( tupleCounts, 1 );
        queue.setTupleCounts( tupleCounts, ANY_PORT );
    }

    public void offer ( final TuplesImpl input )
    {
        if ( input != null )
        {
            final int portCount = min( operatorDef.getInputPortCount(), input.getPortCount() );
            for ( int portIndex = 0; portIndex < portCount; portIndex++ )
            {
                final List<Tuple> tuples = input.getTuplesModifiable( portIndex );
                queue.offer( portIndex, tuples );
            }
        }
    }

    private TuplesImpl drainQueue ( final boolean drainerMaySkipBlocking )
    {
        drainer.reset();
        queue.drain( drainerMaySkipBlocking, drainer );
        return drainer.getResult();
    }

    /**
     * Invokes the operator, resets the drainer and handles the new scheduling strategy if allowed.
     */
    private TuplesImpl invokeOperator ( final InvocationReason reason,
                                        final TuplesImpl input,
                                        final TuplesImpl output,
                                        final PartitionKey key )
    {
        final KVStore kvStore = operatorKvStore.getKVStore( key );
        final TuplesImpl invocationOutput = output != null ? output : outputSupplier.get();
        invocationContext.setInvocationParameters( reason, input, invocationOutput, key, kvStore );
        meter.addConsumedTuples( operatorDef.getId(), input );
        meter.startOperatorInvocation( operatorDef.getId() );
        operator.invoke( invocationContext );
        meter.completeOperatorInvocation( operatorDef.getId() );
        invocationContext.resetInvocationParameters();

        return invocationOutput;
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
        LOGGER.info( "{} acquired {}", operatorName, drainer.getClass().getSimpleName() );
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
        checkArgument( upstreamContext != null, "upstream context is null! operator ", operatorName );
        this.upstreamContext = upstreamContext;
        invocationContext.setUpstreamConnectionStatuses( upstreamContext.getUpstreamConnectionStatuses( operatorDef.getInputPortCount() ) );
    }

    /**
     * Finalizes the running schedule of the operator. It releases the drainer, sets the scheduling strategy to {@link ScheduleNever},
     * sets status to {@link OperatorReplicaStatus#COMPLETED} and updates the upstream context that will be passed to the next operator.
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
     * Shuts down the operator and sets status to {@link OperatorReplicaStatus#SHUT_DOWN}
     */
    public void shutdown ()
    {
        checkState( status != INITIAL, "cannot shutdown %s since it is in %s status", operatorName, INITIAL );

        if ( status == SHUT_DOWN )
        {
            LOGGER.info( "{} ignoring shutdown request since already shut down", operatorName );
            return;
        }

        checkState( status == INITIALIZATION_FAILED || status == RUNNING || status == COMPLETING || status == COMPLETED,
                    "Operator %s cannot be shut down because it is in %s state",
                    operatorName,
                    status );
        try
        {
            if ( operator != null )
            {
                operator.shutdown();
                LOGGER.info( "Operator {} is shut down.", operatorName );
            }
        }
        catch ( Exception e )
        {
            checkInterruption( e );
            LOGGER.error( operatorName + " failed to shut down", e );
        }
        finally
        {
            setStatus( SHUT_DOWN );
            operator = null;
            drainer = null;
        }
    }

    public OperatorReplica duplicate ( final PipelineReplicaId pipelineReplicaId,
                                       final OperatorTupleQueue queue,
                                       final TupleQueueDrainerPool drainerPool,
                                       final Supplier<TuplesImpl> outputSupplier,
                                       final PipelineReplicaMeter meter )
    {
        checkState( this.status == RUNNING,
                    "cannot duplicate %s to %s because status is %s",
                    this.operatorName,
                    pipelineReplicaId,
                    this.status );

        final OperatorReplica duplicate = new OperatorReplica( pipelineReplicaId, this.operatorDef, queue, this.operatorKvStore,
                                                               drainerPool,
                                                               outputSupplier,
                                                               meter );

        duplicate.status = this.status;
        duplicate.upstreamContext = this.upstreamContext;
        duplicate.selfUpstreamContext = this.selfUpstreamContext;
        duplicate.operator = this.operator;
        duplicate.initialSchedulingStrategy = this.initialSchedulingStrategy;
        duplicate.schedulingStrategy = this.schedulingStrategy;
        drainerPool.reset();
        duplicate.drainer = drainerPool.acquire( duplicate.schedulingStrategy );

        LOGGER.info( "{} is duplicated to {}", this.operatorName, duplicate.operatorName );

        return duplicate;
    }

    void setOperatorReplicaListener ( final OperatorReplicaListener listener )
    {
        checkArgument( listener != null, "cannot set null operator replica listener to %s", operatorName );
        this.listener = listener;
    }

    public OperatorDef getOperatorDef ()
    {
        return operatorDef;
    }

    public PipelineReplicaMeter getMeter ()
    {
        return meter;
    }

    public SchedulingStrategy getInitialSchedulingStrategy ()
    {
        return initialSchedulingStrategy;
    }

    public SchedulingStrategy getSchedulingStrategy ()
    {
        return schedulingStrategy;
    }

    public OperatorReplicaStatus getStatus ()
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

    public OperatorKVStore getOperatorKvStore ()
    {
        return operatorKvStore;
    }

    public OperatorTupleQueue getQueue ()
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

    public UpstreamContext getUpstreamContext ()
    {
        return upstreamContext;
    }

    public UpstreamContext getSelfUpstreamContext ()
    {
        return selfUpstreamContext;
    }

    boolean isInvokable ()
    {
        return status == RUNNING || status == COMPLETING;
    }

    boolean isOperatorInvokedOnLastAttempt ()
    {
        return operatorInvokedOnLastAttempt;
    }

    InvocationReason getCompletionReason ()
    {
        return completionReason;
    }

    @Override
    public String toString ()
    {
        return "OperatorReplica{" + "operatorName='" + operatorName + '\'' + ", operatorType=" + operatorDef.getOperatorType() + ", queue="
               + queue.getClass().getSimpleName() + ", drainer=" + ( drainer != null ? drainer.getClass().getSimpleName() : null )
               + ", status=" + status + ", upstreamContext=" + upstreamContext + ", selfUpstreamContext=" + selfUpstreamContext
               + ", completionReason=" + completionReason + ", initialSchedulingStrategy=" + initialSchedulingStrategy
               + ", schedulingStrategy=" + schedulingStrategy + '}';
    }

}
