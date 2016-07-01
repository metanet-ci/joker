package cs.bilkent.zanza.engine.pipeline;

import java.lang.reflect.Field;
import java.util.function.Supplier;

import org.junit.Before;
import org.mockito.Mock;

import cs.bilkent.zanza.engine.config.ZanzaConfig;
import cs.bilkent.zanza.engine.kvstore.KVStoreContext;
import static cs.bilkent.zanza.engine.pipeline.OperatorInstanceInitializationTest.newUpstreamContextInstance;
import static cs.bilkent.zanza.engine.pipeline.UpstreamConnectionStatus.ACTIVE;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueueContext;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueueDrainer;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueueDrainerPool;
import cs.bilkent.zanza.flow.OperatorDefinition;
import cs.bilkent.zanza.operator.InitializationContext;
import static cs.bilkent.zanza.operator.InvocationContext.InvocationReason.OPERATOR_REQUESTED_SHUTDOWN;
import cs.bilkent.zanza.operator.Operator;
import cs.bilkent.zanza.operator.impl.InvocationContextImpl;
import cs.bilkent.zanza.operator.impl.TuplesImpl;
import cs.bilkent.zanza.operator.kvstore.KVStore;
import cs.bilkent.zanza.operator.scheduling.SchedulingStrategy;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AbstractOperatorInstanceInvocationTest
{
    @Mock
    protected TupleQueueContext queue;

    @Mock
    protected Operator operator;

    @Mock
    protected OperatorDefinition operatorDefinition;

    @Mock
    protected KVStoreContext kvStoreContext;

    @Mock
    protected KVStore kvStore;

    @Mock
    protected TupleQueueDrainer drainer;

    @Mock
    protected TupleQueueDrainerPool drainerPool;

    @Mock
    protected Supplier<TuplesImpl> outputSupplier;

    protected final Object key = new Object();

    protected final InvocationContextImpl invocationContext = new InvocationContextImpl();

    protected OperatorInstance operatorInstance;

    protected UpstreamContext initializationUpstreamContext;

    @Before
    public void before ()
    {
        operatorInstance = new OperatorInstance( new PipelineInstanceId( new PipelineId( 0, 0 ), 0 ),
                                                 operatorDefinition,
                                                 queue, kvStoreContext,
                                                 drainerPool,
                                                 outputSupplier,
                                                 invocationContext );

        applyDefaultMocks();
    }

    protected void applyDefaultMocks ()
    {
        when( operatorDefinition.id() ).thenReturn( "op1" );
        when( drainerPool.acquire( any( SchedulingStrategy.class ) ) ).thenReturn( drainer );
        when( drainer.getKey() ).thenReturn( key );
        when( kvStoreContext.getKVStore( key ) ).thenReturn( kvStore );
    }

    protected void initializeOperatorInstance ( final int inputPortCount,
                                                final int outputPortCount,
                                                final SchedulingStrategy schedulingStrategy )
    {
        mockOperatorDefinition( inputPortCount, outputPortCount );
        mockOperatorInitializationSchedulingStrategy( schedulingStrategy );

        initializationUpstreamContext = newUpstreamContextInstance( 0, inputPortCount, ACTIVE );
        operatorInstance.init( new ZanzaConfig(), initializationUpstreamContext, null );
    }

    protected void mockOperatorDefinition ( final int inputPortCount, final int outputPortCount )
    {
        when( operatorDefinition.inputPortCount() ).thenReturn( inputPortCount );
        when( operatorDefinition.outputPortCount() ).thenReturn( outputPortCount );
        try
        {
            when( operatorDefinition.createOperator() ).thenReturn( operator );
        }
        catch ( Throwable e )
        {
            throw new RuntimeException( e );
        }
    }

    protected void mockOperatorInitializationSchedulingStrategy ( final SchedulingStrategy schedulingStrategy )
    {
        when( operator.init( any( InitializationContext.class ) ) ).thenReturn( schedulingStrategy );
    }

    protected void assertOperatorInvocation ()
    {
        verify( kvStoreContext ).getKVStore( key );
        verify( operator ).invoke( invocationContext );
        verify( drainer ).reset();
    }

    protected void assertNoOperatorInvocation ()
    {
        verify( kvStoreContext, never() ).getKVStore( key );
        verify( operator, never() ).invoke( invocationContext );
    }

    protected void moveOperatorInstanceToStatus ( OperatorInstanceStatus status )
    {
        try
        {
            final Field statusField = OperatorInstance.class.getDeclaredField( "status" );
            statusField.setAccessible( true );
            statusField.set( operatorInstance, status );

            final Field completionReasonField = OperatorInstance.class.getDeclaredField( "completionReason" );
            completionReasonField.setAccessible( true );
            completionReasonField.set( operatorInstance, OPERATOR_REQUESTED_SHUTDOWN );
        }
        catch ( Exception e )
        {
            throw new RuntimeException( e );
        }
    }

}
