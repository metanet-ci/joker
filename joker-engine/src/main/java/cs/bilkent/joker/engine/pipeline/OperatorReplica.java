package cs.bilkent.joker.engine.pipeline;

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
import static cs.bilkent.joker.engine.pipeline.OperatorReplicaStatus.COMPLETED;
import static cs.bilkent.joker.engine.pipeline.OperatorReplicaStatus.COMPLETING;
import static cs.bilkent.joker.engine.pipeline.OperatorReplicaStatus.INITIAL;
import static cs.bilkent.joker.engine.pipeline.OperatorReplicaStatus.INITIALIZATION_FAILED;
import static cs.bilkent.joker.engine.pipeline.OperatorReplicaStatus.RUNNING;
import static cs.bilkent.joker.engine.pipeline.OperatorReplicaStatus.SHUT_DOWN;
import cs.bilkent.joker.engine.tuplequeue.OperatorTupleQueue;
import cs.bilkent.joker.engine.tuplequeue.TupleQueueDrainer;
import cs.bilkent.joker.engine.tuplequeue.TupleQueueDrainerPool;
import cs.bilkent.joker.engine.tuplequeue.impl.drainer.GreedyDrainer;
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
import cs.bilkent.joker.operator.scheduling.ScheduleNever;
import cs.bilkent.joker.operator.scheduling.ScheduleWhenAvailable;
import cs.bilkent.joker.operator.scheduling.ScheduleWhenTuplesAvailable;
import static cs.bilkent.joker.operator.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByPort.ANY_PORT;
import cs.bilkent.joker.operator.scheduling.SchedulingStrategy;
import static java.lang.Math.min;
import static java.util.Arrays.fill;

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

    private SchedulingStrategy schedulingStrategy;

    private TupleQueueDrainer drainer;

    private OperatorReplicaListener listener = ( operatorId, status1 ) -> {
    };

    public OperatorReplica ( final PipelineReplicaId pipelineReplicaId,
                             final OperatorDef operatorDef,
                             final OperatorTupleQueue queue,
                             final OperatorKVStore operatorKvStore,
                             final TupleQueueDrainerPool drainerPool,
                             final Supplier<TuplesImpl> outputSupplier,
                             final PipelineReplicaMeter meter )
    {
        this.operatorName = pipelineReplicaId.toString() + ".Operator<" + operatorDef.getId() + ">";
        this.queue = queue;
        this.operatorDef = operatorDef;
        this.operatorKvStore = operatorKvStore;
        this.drainerPool = drainerPool;
        this.outputSupplier = outputSupplier;
        this.meter = meter;
        this.invocationContext = new InvocationContextImpl( operatorDef.getInputPortCount(),
                                                            operatorKvStore::getKVStore,
                                                            outputSupplier.get() );
    }

    // constructor for operator replica duplication
    private OperatorReplica ( final PipelineReplicaId pipelineReplicaId,
                              final OperatorDef operatorDef,
                              final OperatorTupleQueue queue,
                              final OperatorKVStore operatorKvStore,
                              final TupleQueueDrainerPool drainerPool,
                              final Supplier<TuplesImpl> outputSupplier,
                              final PipelineReplicaMeter meter,
                              final Operator operator,
                              final OperatorReplicaStatus status,
                              final UpstreamContext upstreamContext,
                              final UpstreamContext selfUpstreamContext,
                              final SchedulingStrategy schedulingStrategy,
                              final TupleQueueDrainer drainer )
    {
        this( pipelineReplicaId, operatorDef, queue, operatorKvStore, drainerPool, outputSupplier, meter );
        this.operator = operator;
        this.status = status;
        this.upstreamContext = upstreamContext;
        this.selfUpstreamContext = selfUpstreamContext;
        this.schedulingStrategy = schedulingStrategy;
        this.drainer = drainer;
    }

    /**
     * Initializes its internal state to get ready for operator invocation. After initialization is completed successfully, it moves
     * the status to {@link OperatorReplicaStatus#RUNNING}. If {@link Operator#init(InitializationContext)} throws an exception,
     * it moves the status to {@link OperatorReplicaStatus#INITIALIZATION_FAILED} and propagates the exception to the caller after
     * wrapping it with {@link InitializationException}.
     */
    public void init ( final UpstreamContext upstreamContext, final UpstreamContext selfUpstreamContext )
    {
        checkState( status == INITIAL, "Cannot initialize Operator %s as it is in %s state", operatorName, status );
        try
        {
            this.selfUpstreamContext = selfUpstreamContext;
            operator = operatorDef.createOperator();
            checkState( operator != null, "Operator %s implementation can not be null", operatorName );
            setUpstreamContext( upstreamContext );
            initializeOperator( upstreamContext );

            setStatus( RUNNING );
            LOGGER.info( "{} initialized. Initial scheduling strategy: {} Drainer: {}",
                         operatorName,
                         schedulingStrategy,
                         drainer.getClass().getSimpleName() );
        }
        catch ( Exception e )
        {
            closeSelfUpstreamContext();
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
        final boolean[] upstreamConnectionStatuses = upstreamContext.getUpstreamConnectionStatuses();
        final InitializationContext initContext = new InitializationContextImpl( operatorDef, upstreamConnectionStatuses );
        final SchedulingStrategy schedulingStrategy = operator.init( initContext );
        upstreamContext.verifyInitializable( operatorDef, schedulingStrategy );
        setNewSchedulingStrategy( schedulingStrategy );
        setDrainer( drainerPool.acquire( schedulingStrategy ) );
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

    private void setDrainer ( final TupleQueueDrainer drainer )
    {
        this.drainer = drainer;
        LOGGER.info( "{} acquired {}", operatorName, drainer.getClass().getSimpleName() );
    }

    private void closeSelfUpstreamContext ()
    {
        if ( selfUpstreamContext != null )
        {
            selfUpstreamContext = selfUpstreamContext.withAllUpstreamConnectionsClosed();
        }
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
        if ( status == COMPLETED )
        {
            return null;
        }

        checkState( status == RUNNING || status == COMPLETING, operatorName );

        offer( upstreamInput );

        invocationContext.reset();
        drainQueue( drainerMaySkipBlocking );

        if ( status == RUNNING )
        {
            if ( schedulingStrategy instanceof ScheduleWhenAvailable )
            {
                if ( handleNewUpstreamContext( upstreamContext ) )
                {
                    invokeOperator( SHUTDOWN );
                    completeRun();
                    completionReason = SHUTDOWN;
                }
                else
                {
                    invokeOperator( SUCCESS );
                }
            }
            else if ( invocationContext.getInputCount() > 0 )
            {
                invokeOperator( SUCCESS );
            }
            else if ( handleNewUpstreamContext( upstreamContext ) && !upstreamContext.isInvokable( operatorDef, schedulingStrategy ) )
            {
                setStatus( COMPLETING );
                completionReason = INPUT_PORT_CLOSED;
                setDrainer( new GreedyDrainer( operatorDef.getInputPortCount() ) );
                setQueueTupleCountsForGreedyDraining();
                drainQueue( drainerMaySkipBlocking );
                if ( invocationContext.getInputCount() > 0 )
                {
                    invokeOperator( INPUT_PORT_CLOSED );
                }
                else
                {
                    return null;
                }
            }
            else
            {
                return null;
            }
        }
        else if ( invocationContext.getInputCount() > 0 )
        {
            // status = COMPLETING
            invokeOperator( INPUT_PORT_CLOSED );
        }
        else if ( handleNewUpstreamContext( upstreamContext ) )
        {
            // status = COMPLETING
            invocationContext.createInputTuples( null );
            invokeOperator( INPUT_PORT_CLOSED );

            if ( upstreamContext.isOpenUpstreamConnectionAbsent() )
            {
                completeRun();
            }
        }
        else
        {
            // status = COMPLETING
            if ( upstreamContext.isOpenUpstreamConnectionAbsent() )
            {
                completeRun();
            }

            return null;
        }

        final TuplesImpl output = invocationContext.getOutput();
        return output.isEmpty() ? null : output;
    }

    private void drainQueue ( final boolean drainerMaySkipBlocking )
    {
        queue.drain( drainerMaySkipBlocking, drainer, invocationContext::createInputTuples );
    }

    private void setQueueTupleCountsForGreedyDraining ()
    {
        final int[] tupleCounts = new int[ operatorDef.getInputPortCount() ];
        fill( tupleCounts, 1 );
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

    /**
     * Invokes the operator, resets the drainer and handles the new scheduling strategy if allowed.
     */
    private void invokeOperator ( final InvocationReason reason )
    {
        invocationContext.setInvocationReason( reason );
        meter.onInvocationStart( operatorDef.getId() );

        do
        {
            operator.invoke( invocationContext );
        } while ( invocationContext.nextInput() );

        meter.addTuples( operatorDef.getId(), invocationContext.getInputs(), invocationContext.getInputCount() );

        meter.onInvocationComplete( operatorDef.getId() );
    }

    /**
     * Updates the scheduling strategy of the operator with new scheduling strategy given in the argument.
     * It also releases the drainer of the previous scheduling strategy and acquires a new one for the new scheduling strategy.
     */
    private void setNewSchedulingStrategy ( final SchedulingStrategy newSchedulingStrategy )
    {
        LOGGER.info( "{} setting new scheduling strategy: {}", operatorName, newSchedulingStrategy );
        schedulingStrategy = newSchedulingStrategy;
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
        invocationContext.setUpstreamConnectionStatuses( upstreamContext.getUpstreamConnectionStatuses() );
    }

    /**
     * Finalizes the running schedule of the operator. It releases the drainer, sets the scheduling strategy to {@link ScheduleNever},
     * sets status to {@link OperatorReplicaStatus#COMPLETED} and updates the upstream context that will be passed to the next operator.
     */
    private void completeRun ()
    {
        closeSelfUpstreamContext();
        setStatus( COMPLETED );
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
                                       final PipelineReplicaMeter meter,
                                       final UpstreamContext selfUpstreamContext )
    {
        checkState( this.status == RUNNING,
                    "cannot duplicate %s to %s because status is %s",
                    this.operatorName,
                    pipelineReplicaId,
                    this.status );

        final OperatorReplica duplicate = new OperatorReplica( pipelineReplicaId,
                                                               this.operatorDef,
                                                               queue,
                                                               this.operatorKvStore,
                                                               drainerPool,
                                                               outputSupplier,
                                                               meter,
                                                               this.operator,
                                                               this.status,
                                                               this.upstreamContext,
                                                               selfUpstreamContext,
                                                               this.schedulingStrategy,
                                                               drainerPool.acquire( this.schedulingStrategy ) );

        LOGGER.debug( "{} is duplicated to {}", this.operatorName, duplicate.operatorName );

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

    InvocationReason getCompletionReason ()
    {
        return completionReason;
    }

    InvocationContextImpl getInvocationContext ()
    {
        return invocationContext;
    }

    @Override
    public String toString ()
    {
        return "OperatorReplica{" + "operatorName='" + operatorName + '\'' + ", operatorType=" + operatorDef.getOperatorType() + ", queue="
               + queue.getClass().getSimpleName() + ", drainer=" + ( drainer != null ? drainer.getClass().getSimpleName() : null )
               + ", status=" + status + ", upstreamContext=" + upstreamContext + ", selfUpstreamContext=" + selfUpstreamContext
               + ", completionReason=" + completionReason + ", schedulingStrategy=" + schedulingStrategy + '}';
    }

}
