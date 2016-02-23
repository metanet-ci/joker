package cs.bilkent.zanza.engine.pipeline;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkState;
import cs.bilkent.zanza.engine.exception.InitializationException;
import cs.bilkent.zanza.engine.kvstore.KVStoreProvider;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueueContext;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueueDrainer;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueueDrainerFactory;
import cs.bilkent.zanza.engine.tuplequeue.impl.drainer.GreedyDrainer;
import cs.bilkent.zanza.flow.OperatorDefinition;
import cs.bilkent.zanza.kvstore.KVStore;
import cs.bilkent.zanza.operator.InitializationContext;
import cs.bilkent.zanza.operator.InvocationContext.InvocationReason;
import static cs.bilkent.zanza.operator.InvocationContext.InvocationReason.SUCCESS;
import cs.bilkent.zanza.operator.InvocationResult;
import cs.bilkent.zanza.operator.Operator;
import cs.bilkent.zanza.operator.PortsToTuples;
import cs.bilkent.zanza.scheduling.ScheduleNever;
import cs.bilkent.zanza.scheduling.SchedulingStrategy;
import cs.bilkent.zanza.utils.SimpleInvocationContext;

public class OperatorInstance
{

    private static Logger LOGGER = LoggerFactory.getLogger( OperatorInstance.class );


    private final PipelineInstanceId pipelineInstanceId;

    private final String operatorName;

    private final TupleQueueContext queue;

    private final OperatorDefinition operatorDefinition;

    private final KVStoreProvider kvStoreProvider;

    private final TupleQueueDrainerFactory drainerFactory;

    private OperatorInstanceStatus status = OperatorInstanceStatus.NON_INITIALIZED;

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

    public void init ()
    {
        checkState( status == OperatorInstanceStatus.NON_INITIALIZED );
        try
        {
            operator = operatorDefinition.createOperator();
            final InitializationContext context = new OperatorDefinitionInitializationContextAdaptor( operatorDefinition );
            schedulingStrategy = operator.init( context );
            status = OperatorInstanceStatus.RUNNING;
            LOGGER.info( "{}:{} initialized. Initial scheduling strategy: {}", pipelineInstanceId, operatorName, schedulingStrategy );
        }
        catch ( Exception e )
        {
            status = OperatorInstanceStatus.INITIALIZATION_FAILED;
            throw new InitializationException( "Operator " + operatorName + " initialization failed!", e );
        }
    }

    public InvocationResult invoke ( final PortsToTuples upstreamInput )
    {
        checkState( status == OperatorInstanceStatus.RUNNING );

        if ( schedulingStrategy instanceof ScheduleNever )
        {
            LOGGER.warn( "{}:{} completed its execution but invoked again. Input: {}", pipelineInstanceId, operatorName, upstreamInput );
            return null;
        }

        queue.add( upstreamInput );
        final TupleQueueDrainer drainer = drainerFactory.create( schedulingStrategy );
        queue.drain( drainer );
        final PortsToTuples input = drainer.getResult();
        if ( input != null )
        {
            final KVStore kvStore = kvStoreProvider.getKVStore( drainer.getKey() );
            final InvocationResult result = operator.process( new SimpleInvocationContext( SUCCESS, input, kvStore ) );
            schedulingStrategy = result.getSchedulingStrategy();
            return result;
        }

        return null;
    }

    public PortsToTuples forceInvoke ( PortsToTuples upstreamInput, InvocationReason reason )
    {
        checkState( status == OperatorInstanceStatus.RUNNING );

        if ( schedulingStrategy instanceof ScheduleNever )
        {
            LOGGER.warn( "{}:{} ignoring force invoke", pipelineInstanceId, operatorName );
            return null;
        }

        queue.add( upstreamInput );
        final TupleQueueDrainer drainer = new GreedyDrainer();
        queue.drain( drainer );
        final PortsToTuples input = drainer.getResult();
        final KVStore kvStore = kvStoreProvider.getKVStore( drainer.getKey() );
        final InvocationResult result = operator.process( new SimpleInvocationContext( reason, input, kvStore ) );
        schedulingStrategy = result.getSchedulingStrategy();
        if ( schedulingStrategy instanceof ScheduleNever )
        {
            LOGGER.info( "{}:{} force invoked and completed its execution.", pipelineInstanceId, operatorName );
        }
        else
        {
            LOGGER.warn( "{}:{} force invoked and requested new scheduling.", pipelineInstanceId, operatorName );
            schedulingStrategy = ScheduleNever.INSTANCE;
        }

        return result.getOutputTuples();
    }

    public void shutdown ()
    {
        if ( status == OperatorInstanceStatus.SHUT_DOWN )
        {
            LOGGER.info( "{}:{} ignoring shutdown request since already shut down", pipelineInstanceId, operatorName );
            return;
        }

        checkState( status == OperatorInstanceStatus.RUNNING || status == OperatorInstanceStatus.INITIALIZATION_FAILED );
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
            status = OperatorInstanceStatus.SHUT_DOWN;
        }
    }

    public OperatorInstanceStatus getStatus ()
    {
        return status;
    }

    public SchedulingStrategy getSchedulingStrategy ()
    {
        return schedulingStrategy;
    }

}
