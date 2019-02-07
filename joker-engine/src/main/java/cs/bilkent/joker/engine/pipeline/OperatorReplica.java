package cs.bilkent.joker.engine.pipeline;

import java.util.Arrays;
import java.util.function.Function;
import java.util.stream.IntStream;
import javax.annotation.concurrent.NotThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import cs.bilkent.joker.engine.config.JokerConfig;
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
import static cs.bilkent.joker.engine.util.ExceptionUtils.checkInterruption;
import cs.bilkent.joker.flow.FlowDef;
import cs.bilkent.joker.operator.InitCtx;
import cs.bilkent.joker.operator.InvocationCtx.InvocationReason;
import static cs.bilkent.joker.operator.InvocationCtx.InvocationReason.INPUT_PORT_CLOSED;
import static cs.bilkent.joker.operator.InvocationCtx.InvocationReason.SHUTDOWN;
import static cs.bilkent.joker.operator.InvocationCtx.InvocationReason.SUCCESS;
import cs.bilkent.joker.operator.Operator;
import cs.bilkent.joker.operator.OperatorDef;
import cs.bilkent.joker.operator.impl.DefaultInvocationCtx;
import cs.bilkent.joker.operator.impl.InitCtxImpl;
import cs.bilkent.joker.operator.impl.InternalInvocationCtx;
import cs.bilkent.joker.operator.impl.TuplesImpl;
import cs.bilkent.joker.operator.scheduling.ScheduleNever;
import cs.bilkent.joker.operator.scheduling.ScheduleWhenAvailable;
import cs.bilkent.joker.operator.scheduling.ScheduleWhenTuplesAvailable;
import static cs.bilkent.joker.operator.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByCount.AT_LEAST;
import static cs.bilkent.joker.operator.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByPort.ANY_PORT;
import static cs.bilkent.joker.operator.scheduling.ScheduleWhenTuplesAvailable.scheduleWhenTuplesAvailableOnAny;
import cs.bilkent.joker.operator.scheduling.SchedulingStrategy;
import cs.bilkent.joker.partition.impl.PartitionKey;
import static java.lang.Math.min;
import static java.lang.System.arraycopy;
import static java.util.Collections.singletonList;
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


    private final JokerConfig config;

    private final String operatorName;

    private final OperatorDef operatorDef;

    private final OperatorDef[] fusedOperatorDefs;

    private final OperatorQueue queue;

    private final TupleQueueDrainerPool drainerPool;

    private final PipelineReplicaMeter meter;

    private final long latencyStageTickMask;

    private final DefaultInvocationCtx invocationCtx;

    private final InternalInvocationCtx[] fusedInvocationCtxes;

    private final InternalInvocationCtx lastInvocationCtx;

    private final Function<PartitionKey, TuplesImpl> drainerTuplesSupplier;

    private TupleQueueDrainer drainer;

    private OperatorReplicaStatus status = INITIAL;

    private UpstreamCtx upstreamCtx;

    private UpstreamCtx[] fusedUpstreamCtxes;

    private UpstreamCtx downstreamCtx;

    private InvocationReason completionReason;

    private Operator operator;

    private Operator[] fusedOperators;

    private SchedulingStrategy schedulingStrategy;

    private int[] minTupleCounts;


    private OperatorReplicaListener listener = ( operatorId, status1 ) -> {
    };

    public OperatorReplica ( final JokerConfig config,
                             final PipelineReplicaId pipelineReplicaId,
                             final OperatorQueue queue,
                             final TupleQueueDrainerPool drainerPool,
                             final PipelineReplicaMeter meter,
                             final Function<PartitionKey, TuplesImpl> drainerTuplesSupplier,
                             final OperatorDef[] operatorDefs,
                             final InternalInvocationCtx[] invocationCtxes )
    {
        checkArgument( config != null );
        checkArgument( pipelineReplicaId != null );
        checkArgument( queue != null );
        checkArgument( drainerPool != null );
        checkArgument( meter != null );
        checkArgument( drainerTuplesSupplier != null );
        checkArgument( operatorDefs != null && operatorDefs.length > 0 );
        checkArgument( invocationCtxes != null && invocationCtxes.length == operatorDefs.length );
        this.config = config;
        this.operatorName = generateOperatorName( pipelineReplicaId, operatorDefs );
        this.queue = queue;
        this.drainerPool = drainerPool;
        this.meter = meter;
        this.latencyStageTickMask = config.getPipelineManagerConfig().getLatencyStageTickMask();
        this.drainerTuplesSupplier = drainerTuplesSupplier;
        this.operatorDef = operatorDefs[ 0 ];
        final int fusedOperatorCount = operatorDefs.length - 1;
        this.fusedOperatorDefs = new OperatorDef[ fusedOperatorCount ];
        if ( fusedOperatorCount > 0 )
        {
            arraycopy( operatorDefs, 1, this.fusedOperatorDefs, 0, fusedOperatorCount );
        }
        this.fusedOperators = new Operator[ fusedOperatorCount ];
        this.invocationCtx = (DefaultInvocationCtx) invocationCtxes[ 0 ];
        this.fusedInvocationCtxes = new InternalInvocationCtx[ fusedOperatorCount ];
        if ( fusedOperatorCount > 0 )
        {
            arraycopy( invocationCtxes, 1, fusedInvocationCtxes, 0, fusedOperatorCount );
        }
        this.lastInvocationCtx = invocationCtxes[ fusedOperatorCount ];
        this.fusedUpstreamCtxes = new UpstreamCtx[ fusedOperatorCount ];
    }

    private OperatorReplica ( final JokerConfig config,
                              final PipelineReplicaId pipelineReplicaId,
                              final OperatorQueue queue,
                              final TupleQueueDrainerPool drainerPool,
                              final PipelineReplicaMeter meter,
                              final Function<PartitionKey, TuplesImpl> drainerTuplesSupplier,
                              final OperatorDef[] operatorDefs,
                              final InternalInvocationCtx[] invocationCtxes,
                              final SchedulingStrategy schedulingStrategy,
                              final Operator operator,
                              final Operator[] fusedOperators,
                              final UpstreamCtx upstreamCtx,
                              final UpstreamCtx[] fusedUpstreamCtxes,
                              final UpstreamCtx downstreamCtx )
    {
        this( config, pipelineReplicaId, queue, drainerPool, meter, drainerTuplesSupplier, operatorDefs, invocationCtxes );

        this.downstreamCtx = downstreamCtx;

        setUpstreamCtx( 0, upstreamCtx );
        for ( int i = 0; i < fusedUpstreamCtxes.length; i++ )
        {
            setUpstreamCtx( i + 1, fusedUpstreamCtxes[ i ] );
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
     * the status to {@link OperatorReplicaStatus#RUNNING}. If {@link Operator#init(InitCtx)} throws an exception,
     * it moves the status to {@link OperatorReplicaStatus#INITIALIZATION_FAILED} and propagates the exception to the caller after
     * wrapping it with {@link InitializationException}.
     */
    public SchedulingStrategy[] init ( final UpstreamCtx[] upstreamCtxes, final UpstreamCtx downstreamCtx )
    {
        checkState( status == INITIAL, "Cannot initialize %s as it is in %s state", operatorName, status );

        try
        {
            final SchedulingStrategy[] schedulingStrategies = new SchedulingStrategy[ getOperatorCount() ];

            this.downstreamCtx = downstreamCtx;
            for ( int i = 0; i < upstreamCtxes.length; i++ )
            {
                setUpstreamCtx( i, upstreamCtxes[ i ] );
            }

            operator = operatorDef.createOperator();
            checkState( operator != null, "%s implementation can not be null", operatorName );
            final SchedulingStrategy schedulingStrategy = initializeOperator( operatorDef, operator, upstreamCtx );
            schedulingStrategies[ 0 ] = schedulingStrategy;

            setSchedulingStrategy( schedulingStrategy );
            setDrainer( drainerPool.acquire( schedulingStrategy ) );
            setQueueTupleCounts( schedulingStrategy );

            for ( int i = 0; i < fusedOperatorDefs.length; i++ )
            {
                fusedOperators[ i ] = fusedOperatorDefs[ i ].createOperator();
                checkState( fusedOperators[ i ] != null, "%s implementation can not be null", operatorName );
                schedulingStrategies[ i + 1 ] = initializeOperator( fusedOperatorDefs[ i ], fusedOperators[ i ], fusedUpstreamCtxes[ i ] );
            }

            setStatus( RUNNING );
            LOGGER.info( "{} initialized. Initial scheduling strategies: {} Drainer: {}",
                         operatorName,
                         schedulingStrategies,
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

    private SchedulingStrategy initializeOperator ( final OperatorDef operatorDef, final Operator operator, final UpstreamCtx upstreamCtx )
    {
        final InitCtx initCtx = new InitCtxImpl( operatorDef, upstreamCtx.getConnectionStatuses() );
        final SchedulingStrategy schedulingStrategy = operator.init( initCtx );
        upstreamCtx.verifyInitializable( operatorDef, schedulingStrategy );

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
        if ( downstreamCtx != null )
        {
            downstreamCtx = downstreamCtx.withAllConnectionsClosed();
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
     * @param upstreamInput
     *         input of the operator which is sent by the upstream operator
     * @param upstreamCtx
     *         status of the upstream connections
     *
     * @return output of the operator invocation
     */
    public TuplesImpl invoke ( final TuplesImpl upstreamInput, final UpstreamCtx upstreamCtx )
    {
        if ( status == COMPLETED )
        {
            return null;
        }

        checkState( status == RUNNING || status == COMPLETING, operatorName );

        offer( upstreamInput );
        drainQueue();

        if ( status == RUNNING )
        {
            if ( schedulingStrategy instanceof ScheduleWhenAvailable )
            {
                if ( handleNewUpstreamCtx( upstreamCtx ) )
                {
                    closeFusedUpstreamCtxes();
                    invokeOperators( SHUTDOWN );
                    completeRun();
                    completionReason = SHUTDOWN;
                }
                else
                {
                    invokeOperators( SUCCESS );
                }
            }
            else if ( invocationCtx.getInputCount() > 0 )
            {
                invokeOperators( SUCCESS );
            }
            else if ( handleNewUpstreamCtx( upstreamCtx ) && !upstreamCtx.isInvokable( operatorDef, schedulingStrategy ) )
            {
                closeFusedUpstreamCtxes();
                setStatus( COMPLETING );
                completionReason = INPUT_PORT_CLOSED;
                setCompletionDrainer();
                drainQueue();
                if ( invocationCtx.getInputCount() > 0 )
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
        else if ( invocationCtx.getInputCount() > 0 )
        {
            // status = COMPLETING
            invokeOperators( INPUT_PORT_CLOSED );
        }
        else if ( handleNewUpstreamCtx( upstreamCtx ) )
        {
            // status = COMPLETING
            drainerTuplesSupplier.apply( null );
            invokeOperators( INPUT_PORT_CLOSED );

            if ( upstreamCtx.isOpenConnectionAbsent() )
            {
                completeRun();
            }
        }
        else
        {
            // status = COMPLETING
            if ( upstreamCtx.isOpenConnectionAbsent() )
            {
                completeRun();
            }

            return null;
        }

        return lastInvocationCtx.getOutput();
    }

    private void drainQueue ()
    {
        resetInvocationCtxes();
        queue.drain( drainer, drainerTuplesSupplier );
    }

    private void setCompletionDrainer ()
    {
        final int inputPortCount = operatorDef.getInputPortCount();
        final ScheduleWhenTuplesAvailable completionStrategy = scheduleWhenTuplesAvailableOnAny( AT_LEAST,
                                                                                                 inputPortCount,
                                                                                                 1,
                                                                                                 IntStream.range( 0, inputPortCount )
                                                                                                          .toArray() );
        final TupleQueueDrainer completionDrainer = drainerPool.acquire( completionStrategy );

        LOGGER.info( "{} acquired {} for completion", operatorName, completionDrainer.getClass().getSimpleName() );

        setDrainer( completionDrainer );
        queue.setTupleCounts( completionStrategy.getTupleCounts(), ANY_PORT );
    }

    private void offer ( final TuplesImpl input )
    {
        if ( input != null )
        {
            // TODO FIXIT
            final int portCount = min( operatorDef.getInputPortCount(), input.getPortCount() );
            for ( int portIndex = 0; portIndex < portCount; portIndex++ )
            {
                queue.offer( portIndex, input.getTuples( portIndex ) );
            }
        }
    }

    private void resetInvocationCtxes ()
    {
        invocationCtx.reset();
        for ( int i = 0; i < fusedInvocationCtxes.length; i++ )
        {
            fusedInvocationCtxes[ i ].reset();
        }
    }

    private void invokeOperators ( final InvocationReason reason )
    {
        invokeOperator( operatorDef, reason, invocationCtx, operator );

        for ( int i = 0; i < fusedOperators.length; i++ )
        {
            final InternalInvocationCtx invocationCtx = fusedInvocationCtxes[ i ];
            if ( invocationCtx.getInputCount() == 0 )
            {
                break;
            }

            invokeOperator( fusedOperatorDefs[ i ], reason, invocationCtx, fusedOperators[ i ] );
        }
    }

    private void invokeOperator ( final OperatorDef operatorDef,
                                  final InvocationReason reason,
                                  final InternalInvocationCtx invocationCtx,
                                  final Operator operator )
    {
        invocationCtx.setInvocationReason( reason );
        meter.onInvocationStart( operatorDef.getId() );

        // TODO FIX_LATENCY
        //        long invocationStartNs = 0;
        //        if ( meter.isTicked( latencyStageTickMask ) )
        //        {
        //            invocationCtx.trackOutputTuple();
        //            invocationStartNs = System.nanoTime();
        //        }

        do
        {
            operator.invoke( invocationCtx );
        } while ( invocationCtx.nextInput() );

        //        final Tuple trackedTuple = invocationCtx.getTrackedOutputTuple();
        //        if ( trackedTuple != null )
        //        {
        //            final int inputTupleCount = meter.onInvocationComplete( operatorDef.getId(),
        //                                                                    invocationCtx.getInputs(),
        //                                                                    invocationCtx.getInputCount(),
        //                                                                    true );
        //            final long duration = ( System.nanoTime() - invocationStartNs ) / max( 1, inputTupleCount );
        //            trackedTuple.recordServiceTime( operatorDef.getId(), duration );
        //        }
        //        else
        //        {
        //            meter.onInvocationComplete( operatorDef.getId(), invocationCtx.getInputs(), invocationCtx.getInputCount(), false );
        //        }

        // TODO FIX HACK
        if ( meter.getPipelineReplicaId().pipelineId.getRegionId() > 0 )
        {
            meter.onInvocationComplete( operatorDef.getId(), invocationCtx.getInputs(), invocationCtx.getInputCount(), false );
        }
        else
        {
            meter.onInvocationComplete( operatorDef.getId(), singletonList( invocationCtx.getOutput() ), 1, false );
        }
    }

    /**
     * Updates the scheduling strategy of the operator with new scheduling strategy given in the argument.
     */
    private void setSchedulingStrategy ( final SchedulingStrategy schedulingStrategy )
    {
        checkArgument( schedulingStrategy != null );
        LOGGER.info( "{} setting new scheduling strategy: {}", operatorName, schedulingStrategy );
        this.schedulingStrategy = schedulingStrategy;
        if ( schedulingStrategy instanceof ScheduleWhenTuplesAvailable )
        {
            minTupleCounts = new int[ operatorDef.getInputPortCount() ];
            final ScheduleWhenTuplesAvailable s = (ScheduleWhenTuplesAvailable) schedulingStrategy;
            System.arraycopy( s.getTupleCounts(), 0, minTupleCounts, 0, operatorDef.getInputPortCount() );
        }
        else
        {
            minTupleCounts = new int[ operatorDef.getInputPortCount() ];
        }
    }

    /**
     * Sets the new upstream context if it has a higher version than the current one.
     *
     * @return true if current upstream context is updated.
     */
    private boolean handleNewUpstreamCtx ( final UpstreamCtx upstreamCtx )
    {
        final boolean isNew = this.upstreamCtx.getVersion() < upstreamCtx.getVersion();
        if ( isNew )
        {
            setUpstreamCtx( 0, upstreamCtx );
        }

        return isNew;
    }

    private void closeFusedUpstreamCtxes ()
    {
        for ( int i = 0; i < fusedUpstreamCtxes.length; i++ )
        {
            setUpstreamCtx( i + 1, fusedUpstreamCtxes[ i ].withAllConnectionsClosed() );
        }
    }

    /**
     * Sets the new upstream context and updates the upstream connection statuses of the invocation context
     */
    private void setUpstreamCtx ( final int index, final UpstreamCtx upstreamCtx )
    {
        checkArgument( index >= 0 );
        checkArgument( upstreamCtx != null, "%s upstream context is null!", operatorName );
        if ( index == 0 )
        {
            this.upstreamCtx = upstreamCtx;
            invocationCtx.setUpstreamConnectionStatuses( upstreamCtx.getConnectionStatuses() );
        }
        else
        {
            fusedUpstreamCtxes[ index - 1 ] = upstreamCtx;
            fusedInvocationCtxes[ index - 1 ].setUpstreamConnectionStatuses( upstreamCtx.getConnectionStatuses() );
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
                                       final UpstreamCtx downstreamCtx )
    {
        checkState( this.status == RUNNING,
                    "cannot duplicate %s to %s because status is %s",
                    this.operatorName,
                    pipelineReplicaId,
                    this.status );

        return new OperatorReplica( config, pipelineReplicaId, queue, drainerPool, meter,
                                    this.drainerTuplesSupplier,
                                    getOperatorDefs(),
                                    getInvocationCtxes(),
                                    this.schedulingStrategy,
                                    this.operator,
                                    this.fusedOperators,
                                    this.upstreamCtx,
                                    this.fusedUpstreamCtxes,
                                    downstreamCtx );
    }

    public static OperatorReplica newRunningInstance ( final JokerConfig config, final PipelineReplicaId pipelineReplicaId,
                                                       final PipelineReplicaMeter meter,
                                                       final OperatorQueue queue,
                                                       final TupleQueueDrainerPool drainerPool,
                                                       final OperatorDef[] operatorDefs,
                                                       final Function<PartitionKey, TuplesImpl> drainerTuplesSupplier,
                                                       final InternalInvocationCtx[] invocationCtxes,
                                                       final Operator[] operators,
                                                       final UpstreamCtx[] upstreamCtxes,
                                                       final SchedulingStrategy schedulingStrategy,
                                                       final UpstreamCtx downstreamCtx )
    {
        final Operator[] fusedOperators = new Operator[ operators.length - 1 ];
        arraycopy( operators, 1, fusedOperators, 0, fusedOperators.length );

        final UpstreamCtx[] fusedUpstreamCtxes = new UpstreamCtx[ upstreamCtxes.length - 1 ];
        arraycopy( upstreamCtxes, 1, fusedUpstreamCtxes, 0, fusedUpstreamCtxes.length );

        return new OperatorReplica( config, pipelineReplicaId, queue, drainerPool, meter,
                                    drainerTuplesSupplier,
                                    operatorDefs,
                                    invocationCtxes,
                                    schedulingStrategy,
                                    operators[ 0 ],
                                    fusedOperators,
                                    upstreamCtxes[ 0 ],
                                    fusedUpstreamCtxes,
                                    downstreamCtx );
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

    public InternalInvocationCtx getInvocationCtx ( int index )
    {
        return index == 0 ? invocationCtx : fusedInvocationCtxes[ index - 1 ];
    }

    public InternalInvocationCtx[] getInvocationCtxes ()
    {
        final InternalInvocationCtx[] invocationCtxes = new InternalInvocationCtx[ fusedInvocationCtxes.length + 1 ];
        invocationCtxes[ 0 ] = invocationCtx;
        arraycopy( fusedInvocationCtxes, 0, invocationCtxes, 1, fusedInvocationCtxes.length );

        return invocationCtxes;
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

    public UpstreamCtx getUpstreamCtx ( int index )
    {
        return index == 0 ? upstreamCtx : fusedUpstreamCtxes[ index - 1 ];
    }

    public UpstreamCtx[] getUpstreamCtxes ()
    {
        final UpstreamCtx[] upstreamCtxes = new UpstreamCtx[ fusedUpstreamCtxes.length + 1 ];
        upstreamCtxes[ 0 ] = upstreamCtx;
        arraycopy( fusedUpstreamCtxes, 0, upstreamCtxes, 1, fusedUpstreamCtxes.length );

        return upstreamCtxes;
    }

    public UpstreamCtx getDownstreamCtx ()
    {
        return downstreamCtx;
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
               + ", status=" + status + ", upstreamCtx=" + upstreamCtx + ", downstreamCtx=" + downstreamCtx + ", completionReason="
               + completionReason + ", schedulingStrategy=" + schedulingStrategy + '}';
    }

}
