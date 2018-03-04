package cs.bilkent.joker.engine.pipeline;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import javax.annotation.concurrent.NotThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import cs.bilkent.joker.engine.exception.InitializationException;
import cs.bilkent.joker.engine.metric.PipelineReplicaMeter;
import static cs.bilkent.joker.engine.pipeline.OperatorReplicaStatus.COMPLETED;
import static cs.bilkent.joker.engine.pipeline.OperatorReplicaStatus.COMPLETING;
import static cs.bilkent.joker.engine.pipeline.OperatorReplicaStatus.INITIAL;
import static cs.bilkent.joker.engine.pipeline.OperatorReplicaStatus.INITIALIZATION_FAILED;
import static cs.bilkent.joker.engine.pipeline.OperatorReplicaStatus.RUNNING;
import static cs.bilkent.joker.engine.pipeline.OperatorReplicaStatus.SHUT_DOWN;
import cs.bilkent.joker.engine.tuplequeue.OperatorQueue;
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
import cs.bilkent.joker.operator.impl.InternalInvocationContext;
import cs.bilkent.joker.operator.impl.TuplesImpl;
import cs.bilkent.joker.operator.scheduling.ScheduleNever;
import cs.bilkent.joker.operator.scheduling.ScheduleWhenAvailable;
import cs.bilkent.joker.operator.scheduling.ScheduleWhenTuplesAvailable;
import static cs.bilkent.joker.operator.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByPort.ANY_PORT;
import cs.bilkent.joker.operator.scheduling.SchedulingStrategy;
import cs.bilkent.joker.partition.impl.PartitionKey;
import static java.lang.Math.min;
import static java.lang.System.arraycopy;
import static java.util.Arrays.fill;
import static java.util.stream.Collectors.joining;

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

    private final OperatorDef[] fusedOperatorDefs;

    private final OperatorQueue queue;

    private final TupleQueueDrainerPool drainerPool;

    private final PipelineReplicaMeter meter;

    private final InternalInvocationContext invocationContext;

    private final InternalInvocationContext[] fusedInvocationContexts;

    private final InternalInvocationContext lastInvocationContext;

    private final Function<PartitionKey, TuplesImpl> drainerTuplesSupplier;

    private TupleQueueDrainer drainer;

    private OperatorReplicaStatus status = INITIAL;

    private UpstreamContext upstreamContext;

    private UpstreamContext[] fusedUpstreamContexts;

    private UpstreamContext downstreamContext;

    private InvocationReason completionReason;

    private Operator operator;

    private Operator[] fusedOperators;

    private SchedulingStrategy schedulingStrategy;


    private OperatorReplicaListener listener = ( operatorId, status1 ) -> {
    };

    public OperatorReplica ( final PipelineReplicaId pipelineReplicaId,
                             final OperatorQueue queue,
                             final TupleQueueDrainerPool drainerPool,
                             final PipelineReplicaMeter meter,
                             final Function<PartitionKey, TuplesImpl> drainerTuplesSupplier,
                             final OperatorDef[] operatorDefs,
                             final InternalInvocationContext[] invocationContexts )
    {
        checkArgument( pipelineReplicaId != null );
        checkArgument( queue != null );
        checkArgument( drainerPool != null );
        checkArgument( meter != null );
        checkArgument( drainerTuplesSupplier != null );
        checkArgument( operatorDefs != null && operatorDefs.length > 0 );
        checkArgument( invocationContexts != null && invocationContexts.length == operatorDefs.length );
        this.operatorName = generateOperatorName( pipelineReplicaId, operatorDefs );
        this.queue = queue;
        this.drainerPool = drainerPool;
        this.meter = meter;
        this.drainerTuplesSupplier = drainerTuplesSupplier;
        this.operatorDef = operatorDefs[ 0 ];
        final int fusedOperatorCount = operatorDefs.length - 1;
        this.fusedOperatorDefs = new OperatorDef[ fusedOperatorCount ];
        if ( fusedOperatorCount > 0 )
        {
            arraycopy( operatorDefs, 1, this.fusedOperatorDefs, 0, fusedOperatorCount );
        }
        this.fusedOperators = new Operator[ fusedOperatorCount ];
        this.invocationContext = invocationContexts[ 0 ];
        this.fusedInvocationContexts = new InternalInvocationContext[ fusedOperatorCount ];
        if ( fusedOperatorCount > 0 )
        {
            arraycopy( invocationContexts, 1, fusedInvocationContexts, 0, fusedOperatorCount );
        }
        this.lastInvocationContext = invocationContexts[ fusedOperatorCount ];
        this.fusedUpstreamContexts = new UpstreamContext[ fusedOperatorCount ];
    }

    private OperatorReplica ( final PipelineReplicaId pipelineReplicaId,
                              final OperatorQueue queue,
                              final TupleQueueDrainerPool drainerPool,
                              final PipelineReplicaMeter meter,
                              final Function<PartitionKey, TuplesImpl> drainerTuplesSupplier,
                              final OperatorDef[] operatorDefs,
                              final InternalInvocationContext[] invocationContexts,
                              final SchedulingStrategy schedulingStrategy,
                              final Operator operator,
                              final Operator[] fusedOperators,
                              final UpstreamContext upstreamContext,
                              final UpstreamContext[] fusedUpstreamContexts,
                              final UpstreamContext downstreamContext )
    {
        this( pipelineReplicaId, queue, drainerPool, meter, drainerTuplesSupplier, operatorDefs, invocationContexts );

        this.downstreamContext = downstreamContext;

        setUpstreamContext( 0, upstreamContext );
        for ( int i = 0; i < fusedUpstreamContexts.length; i++ )
        {
            setUpstreamContext( i + 1, fusedUpstreamContexts[ i ] );
        }

        this.operator = operator;
        arraycopy( fusedOperators, 0, this.fusedOperators, 0, fusedOperators.length );

        setSchedulingStrategy( schedulingStrategy );
        setDrainer( drainerPool.acquire( schedulingStrategy ) );
        setQueueTupleCounts( schedulingStrategy );

        setStatus( RUNNING );
    }

    private String generateOperatorName ( final PipelineReplicaId pipelineReplicaId, final OperatorDef[] operatorDefs )
    {
        if ( operatorDefs.length == 1 )
        {
            return pipelineReplicaId.toString() + ".Operator<" + operatorDefs[ 0 ].getId() + ">";
        }

        return Arrays.stream( operatorDefs )
                     .map( OperatorDef::getId )
                     .collect( joining( ",", pipelineReplicaId.toString() + ".Operators<", ">" ) );
    }

    /**
     * Initializes its internal state to get ready for operator invocation. After initialization is completed successfully, it moves
     * the status to {@link OperatorReplicaStatus#RUNNING}. If {@link Operator#init(InitializationContext)} throws an exception,
     * it moves the status to {@link OperatorReplicaStatus#INITIALIZATION_FAILED} and propagates the exception to the caller after
     * wrapping it with {@link InitializationException}.
     */
    public SchedulingStrategy[] init ( final UpstreamContext[] upstreamContexts, final UpstreamContext downstreamContext )
    {
        checkState( status == INITIAL, "Cannot initialize %s as it is in %s state", operatorName, status );

        try
        {
            final SchedulingStrategy[] schedulingStrategies = new SchedulingStrategy[ getOperatorCount() ];

            this.downstreamContext = downstreamContext;
            for ( int i = 0; i < upstreamContexts.length; i++ )
            {
                setUpstreamContext( i, upstreamContexts[ i ] );
            }

            operator = operatorDef.createOperator();
            checkState( operator != null, "%s implementation can not be null", operatorName );
            final SchedulingStrategy schedulingStrategy = initializeOperator( operatorDef, operator, upstreamContext );
            schedulingStrategies[ 0 ] = schedulingStrategy;

            setSchedulingStrategy( schedulingStrategy );
            setDrainer( drainerPool.acquire( schedulingStrategy ) );
            setQueueTupleCounts( schedulingStrategy );

            for ( int i = 0; i < fusedOperatorDefs.length; i++ )
            {
                fusedOperators[ i ] = fusedOperatorDefs[ i ].createOperator();
                checkState( fusedOperators[ i ] != null, "%s implementation can not be null", operatorName );
                schedulingStrategies[ i + 1 ] = initializeOperator( fusedOperatorDefs[ i ],
                                                                    fusedOperators[ i ],
                                                                    fusedUpstreamContexts[ i ] );
            }

            setStatus( RUNNING );
            LOGGER.info( "{} initialized. Initial scheduling strategies: {} Drainer: {}",
                         operatorName, schedulingStrategies,
                         drainer.getClass().getSimpleName() );

            return schedulingStrategies;
        }
        catch ( Exception e )
        {
            closeDownstreamContext();
            setStatus( INITIALIZATION_FAILED );
            throw new InitializationException( operatorName + " initialization failed!", e );
        }
    }

    private void setStatus ( final OperatorReplicaStatus status )
    {
        this.status = status;
        listener.onStatusChange( operatorDef.getId(), status );
    }

    private SchedulingStrategy initializeOperator ( final OperatorDef operatorDef,
                                                    final Operator operator,
                                                    final UpstreamContext upstreamContext )
    {
        final InitializationContext initContext = new InitializationContextImpl( operatorDef, upstreamContext.getConnectionStatuses() );
        final SchedulingStrategy schedulingStrategy = operator.init( initContext );
        upstreamContext.verifyInitializable( operatorDef, schedulingStrategy );

        return schedulingStrategy;
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

    private void closeDownstreamContext ()
    {
        if ( downstreamContext != null )
        {
            downstreamContext = downstreamContext.withAllConnectionsClosed();
        }
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
        resetInvocationContexts();
        drainQueue( drainerMaySkipBlocking );

        if ( status == RUNNING )
        {
            if ( schedulingStrategy instanceof ScheduleWhenAvailable )
            {
                if ( handleNewUpstreamContext( upstreamContext ) )
                {
                    closeFusedUpstreamContexts();
                    invokeOperators( SHUTDOWN );
                    completeRun();
                    completionReason = SHUTDOWN;
                }
                else
                {
                    invokeOperators( SUCCESS );
                }
            }
            else if ( invocationContext.getInputCount() > 0 )
            {
                invokeOperators( SUCCESS );
            }
            else if ( handleNewUpstreamContext( upstreamContext ) && !upstreamContext.isInvokable( operatorDef, schedulingStrategy ) )
            {
                closeFusedUpstreamContexts();
                setStatus( COMPLETING );
                completionReason = INPUT_PORT_CLOSED;
                setGreedyDrainerForCompletion();
                drainQueue( drainerMaySkipBlocking );
                if ( invocationContext.getInputCount() > 0 )
                {
                    invokeOperators( INPUT_PORT_CLOSED );
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
            invokeOperators( INPUT_PORT_CLOSED );
        }
        else if ( handleNewUpstreamContext( upstreamContext ) )
        {
            // status = COMPLETING
            drainerTuplesSupplier.apply( null );
            invokeOperators( INPUT_PORT_CLOSED );

            if ( upstreamContext.isOpenConnectionAbsent() )
            {
                completeRun();
            }
        }
        else
        {
            // status = COMPLETING
            if ( upstreamContext.isOpenConnectionAbsent() )
            {
                completeRun();
            }

            return null;
        }

        return lastInvocationContext.getOutput();
    }

    private void drainQueue ( final boolean drainerMaySkipBlocking )
    {
        queue.drain( drainerMaySkipBlocking, drainer, drainerTuplesSupplier );
    }

    private void setGreedyDrainerForCompletion ()
    {
        // TODO we need to choose between GreedyDrainer and BlockingGreedyDrainer
        setDrainer( new GreedyDrainer( operatorDef.getInputPortCount() ) );
        final int[] tupleCounts = new int[ operatorDef.getInputPortCount() ];
        fill( tupleCounts, 1 );
        queue.setTupleCounts( tupleCounts, ANY_PORT );
    }

    private void offer ( final TuplesImpl input )
    {
        if ( input != null )
        {
            // TODO FIXIT
            final int portCount = min( operatorDef.getInputPortCount(), input.getPortCount() );
            for ( int portIndex = 0; portIndex < portCount; portIndex++ )
            {
                final List<Tuple> tuples = input.getTuplesModifiable( portIndex );
                queue.offer( portIndex, tuples );
            }
        }
    }

    private void resetInvocationContexts ()
    {
        invocationContext.reset();
        for ( int i = 0; i < fusedInvocationContexts.length; i++ )
        {
            fusedInvocationContexts[ i ].reset();
        }
    }

    private void invokeOperators ( final InvocationReason reason )
    {
        invokeOperator( operatorDef, reason, invocationContext, operator );

        for ( int i = 0; i < fusedOperators.length; i++ )
        {
            final InternalInvocationContext invocationContext = fusedInvocationContexts[ i ];
            if ( invocationContext.getInputCount() == 0 )
            {
                break;
            }

            invokeOperator( fusedOperatorDefs[ i ], reason, invocationContext, fusedOperators[ i ] );
        }
    }

    private void invokeOperator ( final OperatorDef operatorDef,
                                  final InvocationReason reason,
                                  final InternalInvocationContext invocationContext,
                                  final Operator operator )
    {
        invocationContext.setInvocationReason( reason );
        meter.onInvocationStart( operatorDef.getId() );
        do
        {
            operator.invoke( invocationContext );
        } while ( invocationContext.nextInput() );
        meter.onInvocationComplete( operatorDef.getId() );

        meter.addTuples( operatorDef.getId(), invocationContext.getInputs(), invocationContext.getInputCount() );
    }

    /**
     * Updates the scheduling strategy of the operator with new scheduling strategy given in the argument.
     */
    private void setSchedulingStrategy ( final SchedulingStrategy schedulingStrategy )
    {
        checkArgument( schedulingStrategy != null );
        LOGGER.info( "{} setting new scheduling strategy: {}", operatorName, schedulingStrategy );
        this.schedulingStrategy = schedulingStrategy;
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
            setUpstreamContext( 0, upstreamContext );
        }

        return isNew;
    }

    private void closeFusedUpstreamContexts ()
    {
        for ( int i = 0; i < fusedUpstreamContexts.length; i++ )
        {
            setUpstreamContext( i + 1, fusedUpstreamContexts[ i ].withAllConnectionsClosed() );
        }
    }

    /**
     * Sets the new upstream context and updates the upstream connection statuses of the invocation context
     */
    private void setUpstreamContext ( final int index, final UpstreamContext upstreamContext )
    {
        checkArgument( index >= 0 );
        checkArgument( upstreamContext != null, "%s upstream context is null!", operatorName );
        if ( index == 0 )
        {
            this.upstreamContext = upstreamContext;
            invocationContext.setUpstreamConnectionStatuses( upstreamContext.getConnectionStatuses() );
        }
        else
        {
            fusedUpstreamContexts[ index - 1 ] = upstreamContext;
            fusedInvocationContexts[ index - 1 ].setUpstreamConnectionStatuses( upstreamContext.getConnectionStatuses() );
        }
    }

    /**
     * Finalizes the running schedule of the operator. It releases the drainer, sets the scheduling strategy to {@link ScheduleNever},
     * sets status to {@link OperatorReplicaStatus#COMPLETED} and updates the upstream context that will be passed to the next operator.
     */
    private void completeRun ()
    {
        closeDownstreamContext();
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

        shutdownOperator( operator, operatorDef );

        for ( int i = 0; i < getFusedOperatorCount(); i++ )
        {
            shutdownOperator( fusedOperators[ i ], fusedOperatorDefs[ i ] );
        }

        LOGGER.info( "{} is shut down.", operatorName );

        setStatus( SHUT_DOWN );
        operator = null;
        drainer = null;
    }

    private void shutdownOperator ( final Operator operator, final OperatorDef operatorDef )
    {
        try
        {
            if ( operator != null )
            {
                operator.shutdown();
            }
        }
        catch ( Exception e )
        {
            checkInterruption( e );
            LOGGER.error( operatorName + "->" + operatorDef.getId() + " failed to shut down", e );
        }
    }

    public OperatorReplica duplicate ( final PipelineReplicaId pipelineReplicaId,
                                       final PipelineReplicaMeter meter,
                                       final OperatorQueue queue,
                                       final TupleQueueDrainerPool drainerPool,
                                       final UpstreamContext downstreamContext )
    {
        checkState( this.status == RUNNING,
                    "cannot duplicate %s to %s because status is %s",
                    this.operatorName,
                    pipelineReplicaId,
                    this.status );

        return new OperatorReplica( pipelineReplicaId,
                                    queue,
                                    drainerPool,
                                    meter,
                                    this.drainerTuplesSupplier,
                                    getOperatorDefs(),
                                    getInvocationContexts(),
                                    this.schedulingStrategy,
                                    this.operator,
                                    this.fusedOperators,
                                    this.upstreamContext,
                                    this.fusedUpstreamContexts,
                                    downstreamContext );
    }

    public static OperatorReplica running ( final PipelineReplicaId pipelineReplicaId,
                                            final PipelineReplicaMeter meter,
                                            final OperatorQueue queue,
                                            final TupleQueueDrainerPool drainerPool,
                                            final OperatorDef[] operatorDefs,
                                            final Function<PartitionKey, TuplesImpl> drainerTuplesSupplier,
                                            final InternalInvocationContext[] invocationContexts,
                                            final Operator[] operators,
                                            final UpstreamContext[] upstreamContexts,
                                            final SchedulingStrategy schedulingStrategy,
                                            final UpstreamContext downstreamContext )
    {
        final Operator[] fusedOperators = new Operator[ operators.length - 1 ];
        arraycopy( operators, 1, fusedOperators, 0, fusedOperators.length );

        final UpstreamContext[] fusedUpstreamContexts = new UpstreamContext[ upstreamContexts.length - 1 ];
        arraycopy( upstreamContexts, 1, fusedUpstreamContexts, 0, fusedUpstreamContexts.length );

        return new OperatorReplica( pipelineReplicaId,
                                    queue,
                                    drainerPool,
                                    meter,
                                    drainerTuplesSupplier,
                                    operatorDefs,
                                    invocationContexts,
                                    schedulingStrategy,
                                    operators[ 0 ],
                                    fusedOperators,
                                    upstreamContexts[ 0 ],
                                    fusedUpstreamContexts,
                                    downstreamContext );
    }

    void setOperatorReplicaListener ( final OperatorReplicaListener listener )
    {
        checkArgument( listener != null, "cannot set null operator replica listener to %s", operatorName );
        this.listener = listener;
    }

    public int getOperatorCount ()
    {
        return getFusedOperatorCount() + 1;
    }

    public int getFusedOperatorCount ()
    {
        return fusedOperatorDefs.length;
    }

    // used during parallelisation changes

    public OperatorDef getOperatorDef ( int index )
    {
        return index == 0 ? operatorDef : fusedOperatorDefs[ index - 1 ];
    }

    public OperatorDef[] getOperatorDefs ()
    {
        final OperatorDef[] operatorDefs = new OperatorDef[ fusedOperatorDefs.length + 1 ];
        operatorDefs[ 0 ] = operatorDef;
        arraycopy( fusedOperatorDefs, 0, operatorDefs, 1, fusedOperatorDefs.length );

        return operatorDefs;
    }

    public int getIndex ( final OperatorDef operatorDef )
    {
        if ( this.operatorDef.equals( operatorDef ) )
        {
            return 0;
        }

        for ( int i = 0; i < fusedOperatorDefs.length; i++ )
        {
            if ( fusedOperatorDefs[ i ].equals( operatorDef ) )
            {
                return i + 1;
            }
        }

        return -1;
    }

    public InternalInvocationContext getInvocationContext ( int index )
    {
        return index == 0 ? invocationContext : fusedInvocationContexts[ index - 1 ];
    }

    public InternalInvocationContext[] getInvocationContexts ()
    {
        final InternalInvocationContext[] invocationContexts = new InternalInvocationContext[ fusedInvocationContexts.length + 1 ];
        invocationContexts[ 0 ] = invocationContext;
        arraycopy( fusedInvocationContexts, 0, invocationContexts, 1, fusedInvocationContexts.length );

        return invocationContexts;
    }

    public PipelineReplicaMeter getMeter ()
    {
        return meter;
    }

    public OperatorReplicaStatus getStatus ()
    {
        return status;
    }
    // used during parallelisation changes. should not be exposed...

    public String getOperatorName ()
    {
        return operatorName;
    }
    // used during testing

    public Operator getOperator ( final int index )
    {
        return index == 0 ? operator : fusedOperators[ index - 1 ];
    }

    public Operator[] getOperators ()
    {
        final Operator[] operators = new Operator[ fusedOperators.length + 1 ];
        operators[ 0 ] = operator;
        arraycopy( fusedOperators, 0, operators, 1, fusedOperators.length );

        return operators;
    }

    public OperatorQueue getQueue ()
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
    // used during testing

    public UpstreamContext getUpstreamContext ( int index )
    {
        return index == 0 ? upstreamContext : fusedUpstreamContexts[ index - 1 ];
    }

    public UpstreamContext[] getUpstreamContexts ()
    {
        final UpstreamContext[] upstreamContexts = new UpstreamContext[ fusedUpstreamContexts.length + 1 ];
        upstreamContexts[ 0 ] = upstreamContext;
        arraycopy( fusedUpstreamContexts, 0, upstreamContexts, 1, fusedUpstreamContexts.length );

        return upstreamContexts;
    }

    public UpstreamContext getDownstreamContext ()
    {
        return downstreamContext;
    }

    boolean isInvokable ()
    {
        return status == RUNNING || status == COMPLETING;
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
               + ", status=" + status + ", upstreamContext=" + upstreamContext + ", downstreamContext=" + downstreamContext
               + ", completionReason=" + completionReason + ", schedulingStrategy=" + schedulingStrategy + '}';
    }

}
