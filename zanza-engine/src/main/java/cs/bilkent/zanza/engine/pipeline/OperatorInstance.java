package cs.bilkent.zanza.engine.pipeline;

import java.util.List;
import java.util.function.Supplier;
import javax.annotation.concurrent.NotThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkState;
import cs.bilkent.zanza.engine.config.ZanzaConfig;
import cs.bilkent.zanza.engine.exception.InitializationException;
import cs.bilkent.zanza.engine.kvstore.KVStoreProvider;
import static cs.bilkent.zanza.engine.pipeline.OperatorInstanceStatus.INITIAL;
import static cs.bilkent.zanza.engine.pipeline.OperatorInstanceStatus.INITIALIZATION_FAILED;
import static cs.bilkent.zanza.engine.pipeline.OperatorInstanceStatus.RUNNING;
import static cs.bilkent.zanza.engine.pipeline.OperatorInstanceStatus.SHUT_DOWN;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueueContext;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueueDrainer;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueueDrainerPool;
import cs.bilkent.zanza.flow.FlowDefinition;
import cs.bilkent.zanza.flow.OperatorDefinition;
import cs.bilkent.zanza.operator.InitializationContext;
import cs.bilkent.zanza.operator.InvocationContext.InvocationReason;
import static cs.bilkent.zanza.operator.InvocationContext.InvocationReason.SUCCESS;
import cs.bilkent.zanza.operator.Operator;
import cs.bilkent.zanza.operator.Tuple;
import cs.bilkent.zanza.operator.impl.InvocationContextImpl;
import cs.bilkent.zanza.operator.impl.TuplesImpl;
import cs.bilkent.zanza.operator.kvstore.KVStore;
import cs.bilkent.zanza.operator.scheduling.ScheduleNever;
import cs.bilkent.zanza.operator.scheduling.ScheduleWhenAvailable;
import cs.bilkent.zanza.operator.scheduling.ScheduleWhenTuplesAvailable;
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

    private Operator operator;

    private SchedulingStrategy schedulingStrategy;

    private TupleQueueDrainer drainer;

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
    public void init ( ZanzaConfig config )
    {
        checkState( status == INITIAL );
        try
        {
            drainerPool.init( config );
            operator = operatorDefinition.createOperator();
            final InitializationContext context = new OperatorDefinitionInitializationContextAdaptor( operatorDefinition );
            schedulingStrategy = operator.init( context );
            drainer = drainerPool.acquire( schedulingStrategy );
            status = RUNNING;
            LOGGER.info( "{} initialized. Initial scheduling strategy: {}", operatorName, schedulingStrategy );
        }
        catch ( Exception e )
        {
            status = INITIALIZATION_FAILED;
            throw new InitializationException( "Operator " + operatorName + " initialization failed!", e );
        }
    }

    public TuplesImpl invoke ( final TuplesImpl upstreamInput )
    {
        checkState( status == RUNNING );

        if ( isNonInvokable() )
        {
            LOGGER.debug( "{} ignoring invoke", operatorName );
            return null;
        }

        try
        {
            if ( upstreamInput != null )
            {
                for ( int portIndex = 0; portIndex < upstreamInput.getPortCount(); portIndex++ )
                {
                    queue.offer( portIndex, upstreamInput.getTuplesModifiable( portIndex ) );
                }
            }

            queue.drain( drainer );
            final TuplesImpl input = drainer.getResult();
            if ( input != null )
            {
                return invokeInternal( SUCCESS, input, true );
            }

            drainer.reset();
            return null;
        }
        catch ( Exception e )
        {
            LOGGER.error( operatorName + " failed during invoke!", e );
            setNoMoreInvocations();
            return null;
        }
    }

    public TuplesImpl forceInvoke ( final InvocationReason reason, final TuplesImpl upstreamInput, final boolean upstreamCompleted )
    {
        checkState( status == RUNNING );

        if ( schedulingStrategy instanceof ScheduleNever )
        {
            LOGGER.debug( "{} ignoring force invoke", operatorName );
            return null;
        }

        try
        {
            if ( upstreamInput != null )
            {
                for ( int portIndex = 0; portIndex < upstreamInput.getPortCount(); portIndex++ )
                {
                    final List<Tuple> tuples = upstreamInput.getTuplesModifiable( portIndex );
                    if ( !tuples.isEmpty() )
                    {
                        queue.offer( portIndex, tuples );
                    }
                }
            }

            if ( schedulingStrategy instanceof ScheduleWhenAvailable && upstreamCompleted )
            {
                return forceInvokeInternal( reason );
            }

            queue.drain( drainer );
            final TuplesImpl input = drainer.getResult();
            if ( input != null )
            {
                return invokeInternal( SUCCESS, input, true );
            }
            else if ( upstreamCompleted )
            {
                return forceInvokeInternal( reason );
            }

            drainer.reset();
            return null;
        }
        catch ( Exception e )
        {
            LOGGER.error( operatorName + " failed during force invoke!", e );
            setNoMoreInvocations();
            return null;
        }
    }

    private TuplesImpl invokeInternal ( final InvocationReason reason, final TuplesImpl input, final boolean handleNewSchedulingStrategy )
    {
        final KVStore kvStore = kvStoreProvider.getKVStore( drainer.getKey() );
        final TuplesImpl output = outputSupplier.get();
        invocationContext.setInvocationParameters( reason, input, output, kvStore );
        operator.invoke( invocationContext );
        drainer.reset();

        final SchedulingStrategy newSchedulingStrategy = invocationContext.getSchedulingStrategy();
        invocationContext.setNextSchedulingStrategy( null );
        if ( newSchedulingStrategy != null )
        {
            if ( handleNewSchedulingStrategy )
            {
                LOGGER.info( "{} setting new scheduling strategy: {}", operatorName, newSchedulingStrategy );
                schedulingStrategy = newSchedulingStrategy;
                drainerPool.release( drainer );
                if ( !( newSchedulingStrategy instanceof ScheduleNever ) )
                {
                    drainer = drainerPool.acquire( schedulingStrategy );
                    if ( schedulingStrategy instanceof ScheduleWhenTuplesAvailable )
                    {
                        final ScheduleWhenTuplesAvailable scheduleWhenTuplesAvailable = (ScheduleWhenTuplesAvailable) schedulingStrategy;
                        for ( int portIndex = 0; portIndex < scheduleWhenTuplesAvailable.getPortCount(); portIndex++ )
                        {
                            queue.ensureCapacity( portIndex, scheduleWhenTuplesAvailable.getTupleCount( portIndex ) );
                        }
                    }
                }
                else
                {
                    drainer = null;
                }
            }
        }

        return output;
    }

    private TuplesImpl forceInvokeInternal ( final InvocationReason reason )
    {

        drainerPool.release( drainer );
        drainer = drainerPool.acquire( ScheduleWhenAvailable.INSTANCE );
        queue.drain( drainer );
        final TuplesImpl input = drainer.getResult();
        final TuplesImpl output = input != null ? invokeInternal( reason, input, false ) : null;
        setNoMoreInvocations();
        return output;
    }

    private void setNoMoreInvocations ()
    {
        schedulingStrategy = ScheduleNever.INSTANCE;
        if ( drainer != null )
        {
            drainerPool.release( drainer );
            drainer = null;
        }
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
            status = SHUT_DOWN;
            operator = null;
            drainer = null;
        }
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
        return !isNonInvokable();
    }

    public boolean isNonInvokable ()
    {
        return schedulingStrategy instanceof ScheduleNever || schedulingStrategy == null;
    }

}
