package cs.bilkent.zanza.engine.pipeline;

import javax.annotation.concurrent.NotThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkState;
import cs.bilkent.zanza.engine.exception.InitializationException;
import cs.bilkent.zanza.engine.kvstore.KVStoreProvider;
import static cs.bilkent.zanza.engine.pipeline.OperatorInstanceStatus.INITIAL;
import static cs.bilkent.zanza.engine.pipeline.OperatorInstanceStatus.INITIALIZATION_FAILED;
import static cs.bilkent.zanza.engine.pipeline.OperatorInstanceStatus.RUNNING;
import static cs.bilkent.zanza.engine.pipeline.OperatorInstanceStatus.SHUT_DOWN;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueueContext;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueueDrainer;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueueDrainerFactory;
import cs.bilkent.zanza.engine.tuplequeue.impl.drainer.GreedyDrainer;
import cs.bilkent.zanza.flow.FlowDefinition;
import cs.bilkent.zanza.flow.OperatorDefinition;
import cs.bilkent.zanza.operator.InitializationContext;
import cs.bilkent.zanza.operator.InvocationContext.InvocationReason;
import static cs.bilkent.zanza.operator.InvocationContext.InvocationReason.SUCCESS;
import cs.bilkent.zanza.operator.InvocationResult;
import cs.bilkent.zanza.operator.Operator;
import cs.bilkent.zanza.operator.PortsToTuples;
import cs.bilkent.zanza.operator.PortsToTuples.PortToTuples;
import cs.bilkent.zanza.operator.impl.InvocationContextImpl;
import cs.bilkent.zanza.operator.kvstore.KVStore;
import cs.bilkent.zanza.operator.scheduling.ScheduleNever;
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

    private final TupleQueueDrainerFactory drainerFactory;

    private OperatorInstanceStatus status = INITIAL;

    private Operator operator;

    private SchedulingStrategy schedulingStrategy;

    public OperatorInstance ( final PipelineInstanceId pipelineInstanceId,
                              final String operatorName,
                              final TupleQueueContext queue,
                              final OperatorDefinition operatorDefinition,
                              final KVStoreProvider kvStoreProvider,
                              final TupleQueueDrainerFactory drainerFactory )
    {
        this.pipelineInstanceId = pipelineInstanceId;
        this.operatorName = operatorName;
        this.queue = queue;
        this.operatorDefinition = operatorDefinition;
        this.kvStoreProvider = kvStoreProvider;
        this.drainerFactory = drainerFactory;
    }

    /**
     * Initializes its internal state to get ready for operator invocation. After initialization is completed successfully, it moves
     * the status to {@link OperatorInstanceStatus#RUNNING}. If {@link Operator#init(InitializationContext)} throws an exception,
     * it moves the status to {@link OperatorInstanceStatus#INITIALIZATION_FAILED} and propagates the exception to the caller after
     * wrapping it with {@link InitializationException}.
     */
    public void init ()
    {
        checkState( status == INITIAL );
        try
        {
            operator = operatorDefinition.createOperator();
            final InitializationContext context = new OperatorDefinitionInitializationContextAdaptor( operatorDefinition );
            schedulingStrategy = operator.init( context );
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
    public InvocationResult invoke ( final PortsToTuples upstreamInput )
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
                for ( PortToTuples port : upstreamInput.getPortToTuplesList() )
                {
                    queue.offer( port.getPortIndex(), port.getTuples() );
                }
            }

            // TODO create a drainer factory per operator instance and re-use drainer objects here to reduce garbage
            final TupleQueueDrainer drainer = drainerFactory.create( schedulingStrategy );
            queue.drain( drainer );
            final PortsToTuples input = drainer.getResult();
            if ( input != null )
            {
                final KVStore kvStore = kvStoreProvider.getKVStore( drainer.getKey() );
                // TODO reuse invocation context
                final InvocationResult result = operator.invoke( new InvocationContextImpl( SUCCESS, input, kvStore ) );
                schedulingStrategy = result.getSchedulingStrategy();

                // TODO TUPLE QUEUE ENSURE CAPACITY

                return result;
            }

            return null;
        }
        catch ( Exception e )
        {
            LOGGER.error( pipelineInstanceId + ":" + operatorName + " failed during invoke!", e );
            schedulingStrategy = ScheduleNever.INSTANCE;
            return new InvocationResult( ScheduleNever.INSTANCE, null );
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
    public PortsToTuples forceInvoke ( final PortsToTuples upstreamInput, final InvocationReason reason )
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
                for ( PortToTuples port : upstreamInput.getPortToTuplesList() )
                {
                    queue.offer( port.getPortIndex(), port.getTuples() );
                }
            }

            final TupleQueueDrainer drainer = new GreedyDrainer();
            queue.drain( drainer );
            final PortsToTuples input = drainer.getResult();
            final KVStore kvStore = kvStoreProvider.getKVStore( drainer.getKey() );
            final InvocationResult result = operator.invoke( new InvocationContextImpl( reason, input, kvStore ) );
            if ( result.getSchedulingStrategy() instanceof ScheduleNever )
            {
                LOGGER.info( "{}:{} force invoked and completed its execution.", pipelineInstanceId, operatorName );
            }
            else
            {
                LOGGER.warn( "{}:{} force invoked and requested new scheduling.", pipelineInstanceId, operatorName );
            }

            return result.getOutputTuples();
        }
        catch ( Exception e )
        {
            LOGGER.error( pipelineInstanceId + ":" + operatorName + " failed during force invoke!", e );

            return null;
        }
        finally
        {
            schedulingStrategy = ScheduleNever.INSTANCE;
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
            operator.shutdown();
            operator = null;
        }
        catch ( Exception e )
        {
            LOGGER.error( pipelineInstanceId + ":" + operatorName + " failed to shut down", e );
        }
        finally
        {
            status = SHUT_DOWN;
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
