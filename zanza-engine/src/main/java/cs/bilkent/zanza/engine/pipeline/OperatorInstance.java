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


    private final PipelineInstanceId pipelineInstanceId;

    private final String operatorName;

    private final TupleQueueContext queue;

    private final OperatorDefinition operatorDefinition;

    private final KVStoreProvider kvStoreProvider;

    private final TupleQueueDrainerPool drainerPool;

    private final InvocationContextImpl invocationContext;

    private final Supplier<TuplesImpl> outputSupplier;

    private OperatorInstanceStatus status = INITIAL;

    private Operator operator;

    private SchedulingStrategy schedulingStrategy;

    private TupleQueueDrainer drainer;

    public OperatorInstance ( final PipelineInstanceId pipelineInstanceId,
                              final String operatorName,
                              final TupleQueueContext queue,
                              final OperatorDefinition operatorDefinition,
                              final KVStoreProvider kvStoreProvider,
                              final TupleQueueDrainerPool drainerPool,
                              final Supplier<TuplesImpl> outputSupplier )
    {
        this( pipelineInstanceId,
              operatorName,
              queue,
              operatorDefinition,
              kvStoreProvider,
              drainerPool,
              outputSupplier,
              new InvocationContextImpl() );
    }

    public OperatorInstance ( final PipelineInstanceId pipelineInstanceId,
                              final String operatorName,
                              final TupleQueueContext queue,
                              final OperatorDefinition operatorDefinition,
                              final KVStoreProvider kvStoreProvider,
                              final TupleQueueDrainerPool drainerPool,
                              final Supplier<TuplesImpl> outputSupplier,
                              final InvocationContextImpl invocationContext )
    {
        this.pipelineInstanceId = pipelineInstanceId;
        this.operatorName = operatorName;
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
            drainerPool.init( config, operatorDefinition );
            operator = operatorDefinition.createOperator();
            final InitializationContext context = new OperatorDefinitionInitializationContextAdaptor( operatorDefinition );
            schedulingStrategy = operator.init( context );
            drainer = drainerPool.acquire( schedulingStrategy );
            status = RUNNING;
            LOGGER.info( "{}:{} initialized. Initial scheduling strategy: {}", pipelineInstanceId, operatorName, schedulingStrategy );
        }
        catch ( Exception e )
        {
            status = INITIALIZATION_FAILED;
            throw new InitializationException( "Operator " + operatorName + " initialization failed!", e );
        }
    }

    /**
     * Delivers the tuples sent by an upstream operator and tries to invoke the operator. If the operator's recent
     * {@link SchedulingStrategy} is satisfied by the input tuples, the operator is invoked.
     * <p>
     * If the operator invocation throws an exception, {@link SchedulingStrategy} is set to {@link ScheduleNever} and no more invocations
     * are allowed.
     *
     * @param upstreamInput
     *         tuples sent by upstream operators
     *
     * @return result of the invocation if the operator is invoked, NULL otherwise
     */
    public TuplesImpl invoke ( final TuplesImpl upstreamInput )
    {
        checkState( status == RUNNING );

        if ( schedulingStrategy instanceof ScheduleNever )
        {
            LOGGER.warn( "{}:{} completed its execution but invoked again. Input: {}", pipelineInstanceId, operatorName, upstreamInput );
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
                final KVStore kvStore = kvStoreProvider.getKVStore( drainer.getKey() );
                final TuplesImpl output = outputSupplier.get();
                invocationContext.setInvocationParameters( SUCCESS, input, output, kvStore );

                operator.invoke( invocationContext );

                final SchedulingStrategy nextSchedulingStrategy = invocationContext.getSchedulingStrategy();
                if ( nextSchedulingStrategy != null )
                {
                    schedulingStrategy = nextSchedulingStrategy;
                    invocationContext.setNextSchedulingStrategy( null );
                    drainerPool.release( drainer );
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

                return output;
            }

            return null;
        }
        catch ( Exception e )
        {
            LOGGER.error( pipelineInstanceId + ":" + operatorName + " failed during invoke!", e );
            schedulingStrategy = ScheduleNever.INSTANCE;
            return null;
        }
        finally
        {
            drainer.reset();
        }
    }

    /**
     * Performs the final invocation of the operator with the provided {@link InvocationReason}. It performs the final invocation
     * of the operator with all pending input tuples, even if recent {@link SchedulingStrategy} of the operator is not satisfied.
     * After operator invocation is completed, {@link SchedulingStrategy} field is set to {@link ScheduleNever}.
     *
     * @param upstreamInput
     *         tuples sent by upstream operators
     * @param reason
     *         reason of the final invocation
     *
     * @return tuples produced by final invocation of the operator
     */
    public TuplesImpl forceInvoke ( final TuplesImpl upstreamInput, final InvocationReason reason )
    {
        checkState( status == RUNNING );

        if ( schedulingStrategy instanceof ScheduleNever )
        {
            LOGGER.warn( "{}:{} ignoring force invoke", pipelineInstanceId, operatorName );
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

            drainerPool.release( drainer );
            drainer = drainerPool.acquire( ScheduleNever.INSTANCE );

            queue.drain( drainer );
            final TuplesImpl input = drainer.getResult();
            final KVStore kvStore = kvStoreProvider.getKVStore( drainer.getKey() );
            final TuplesImpl output = outputSupplier.get();
            invocationContext.setInvocationParameters( reason, input, output, kvStore );

            operator.invoke( invocationContext );

            if ( invocationContext.getSchedulingStrategy() instanceof ScheduleNever )
            {
                LOGGER.info( "{}:{} force invoked and completed its execution.", pipelineInstanceId, operatorName );
            }
            else
            {
                LOGGER.warn( "{}:{} force invoked and requested new scheduling.", pipelineInstanceId, operatorName );
            }

            return output;
        }
        catch ( Exception e )
        {
            LOGGER.error( pipelineInstanceId + ":" + operatorName + " failed during force invoke!", e );

            return null;
        }
        finally
        {
            schedulingStrategy = ScheduleNever.INSTANCE;
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

        if ( status == SHUT_DOWN )
        {
            LOGGER.info( "{}:{} ignoring shutdown request since already shut down", pipelineInstanceId, operatorName );
            return;
        }

        checkState( status == RUNNING || status == INITIALIZATION_FAILED );
        try
        {
            drainerPool.release( drainer );
            operator.shutdown();
        }
        catch ( Exception e )
        {
            LOGGER.error( pipelineInstanceId + ":" + operatorName + " failed to shut down", e );
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
    public SchedulingStrategy schedulingStrategy ()
    {
        return schedulingStrategy;
    }

    public OperatorInstanceStatus status ()
    {
        return status;
    }

}
