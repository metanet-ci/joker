package cs.bilkent.joker.engine.pipeline;

import java.lang.reflect.Field;
import java.util.function.Supplier;

import org.junit.Before;
import org.mockito.Mock;

import cs.bilkent.joker.engine.kvstore.KVStoreContext;
import static cs.bilkent.joker.engine.pipeline.OperatorReplicaInitializationTest.newUpstreamContextInstance;
import static cs.bilkent.joker.engine.pipeline.UpstreamConnectionStatus.ACTIVE;
import cs.bilkent.joker.engine.tuplequeue.TupleQueueContext;
import cs.bilkent.joker.engine.tuplequeue.TupleQueueDrainer;
import cs.bilkent.joker.engine.tuplequeue.TupleQueueDrainerPool;
import cs.bilkent.joker.operator.InitializationContext;
import static cs.bilkent.joker.operator.InvocationContext.InvocationReason.OPERATOR_REQUESTED_SHUTDOWN;
import cs.bilkent.joker.operator.Operator;
import cs.bilkent.joker.operator.OperatorDef;
import cs.bilkent.joker.operator.impl.InvocationContextImpl;
import cs.bilkent.joker.operator.impl.TuplesImpl;
import cs.bilkent.joker.operator.kvstore.KVStore;
import cs.bilkent.joker.operator.scheduling.SchedulingStrategy;
import cs.bilkent.joker.testutils.AbstractJokerTest;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class AbstractOperatorReplicaInvocationTest extends AbstractJokerTest
{
    @Mock
    protected TupleQueueContext queue;

    @Mock
    protected Operator operator;

    @Mock
    protected OperatorDef operatorDef;

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

    protected OperatorReplica operatorReplica;

    protected UpstreamContext initializationUpstreamContext;

    @Before
    public void before ()
    {
        operatorReplica = new OperatorReplica( new PipelineReplicaId( new PipelineId( 0, 0 ), 0 ),
                                               operatorDef,
                                               queue,
                                               kvStoreContext,
                                               drainerPool,
                                               outputSupplier,
                                               invocationContext );

        applyDefaultMocks();
    }

    protected void applyDefaultMocks ()
    {
        when( operatorDef.id() ).thenReturn( "op1" );
        when( drainerPool.acquire( any( SchedulingStrategy.class ) ) ).thenReturn( drainer );
        when( drainer.getKey() ).thenReturn( key );
        when( kvStoreContext.getKVStore( key ) ).thenReturn( kvStore );
    }

    protected void initializeOperatorReplica ( final int inputPortCount,
                                               final int outputPortCount,
                                               final SchedulingStrategy schedulingStrategy )
    {
        mockOperatorDef( inputPortCount, outputPortCount );
        mockOperatorInitializationSchedulingStrategy( schedulingStrategy );

        initializationUpstreamContext = newUpstreamContextInstance( 0, inputPortCount, ACTIVE );
        operatorReplica.init( initializationUpstreamContext );
    }

    protected void mockOperatorDef ( final int inputPortCount, final int outputPortCount )
    {
        when( operatorDef.inputPortCount() ).thenReturn( inputPortCount );
        when( operatorDef.outputPortCount() ).thenReturn( outputPortCount );
        try
        {
            when( operatorDef.createOperator() ).thenReturn( operator );
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

    protected void setOperatorReplicaStatus ( OperatorReplicaStatus status )
    {
        try
        {
            final Field statusField = OperatorReplica.class.getDeclaredField( "status" );
            statusField.setAccessible( true );
            statusField.set( operatorReplica, status );

            final Field completionReasonField = OperatorReplica.class.getDeclaredField( "completionReason" );
            completionReasonField.setAccessible( true );
            completionReasonField.set( operatorReplica, OPERATOR_REQUESTED_SHUTDOWN );
        }
        catch ( Exception e )
        {
            throw new RuntimeException( e );
        }
    }

}
