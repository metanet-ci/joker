package cs.bilkent.joker.engine.pipeline;

import java.lang.reflect.Field;
import java.util.function.Supplier;

import org.junit.Before;
import org.mockito.Mock;

import cs.bilkent.joker.engine.config.JokerConfig;
import cs.bilkent.joker.engine.kvstore.OperatorKVStore;
import cs.bilkent.joker.engine.metric.PipelineReplicaMeter;
import cs.bilkent.joker.engine.partition.PartitionKey;
import cs.bilkent.joker.engine.partition.impl.PartitionKey1;
import static cs.bilkent.joker.engine.pipeline.OperatorReplicaInitializationTest.newUpstreamContextInstance;
import static cs.bilkent.joker.engine.pipeline.UpstreamConnectionStatus.ACTIVE;
import cs.bilkent.joker.engine.tuplequeue.OperatorTupleQueue;
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
import cs.bilkent.joker.test.AbstractJokerTest;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class AbstractOperatorReplicaInvocationTest extends AbstractJokerTest
{
    @Mock
    protected OperatorTupleQueue queue;

    @Mock
    protected Operator operator;

    @Mock
    protected OperatorDef operatorDef;

    @Mock
    protected OperatorKVStore operatorKvStore;

    @Mock
    protected KVStore kvStore;

    @Mock
    protected TupleQueueDrainer drainer;

    @Mock
    protected TupleQueueDrainerPool drainerPool;

    @Mock
    protected Supplier<TuplesImpl> outputSupplier;

    protected final JokerConfig config = new JokerConfig();

    protected final PartitionKey key = new PartitionKey1( new Object() );

    protected final InvocationContextImpl invocationContext = new InvocationContextImpl();

    protected OperatorReplica operatorReplica;

    protected UpstreamContext initializationUpstreamContext;

    @Before
    public void before ()
    {
        final PipelineReplicaId pipelineReplicaId = new PipelineReplicaId( new PipelineId( 0, 0 ), 0 );
        operatorReplica = new OperatorReplica( pipelineReplicaId, operatorDef, queue, operatorKvStore,
                                               drainerPool,
                                               outputSupplier, mock( PipelineReplicaMeter.class ),
                                               invocationContext );

        applyDefaultMocks();
    }

    void applyDefaultMocks ()
    {
        when( operatorDef.id() ).thenReturn( "op1" );
        when( drainerPool.acquire( any( SchedulingStrategy.class ) ) ).thenReturn( drainer );
        when( drainer.getKey() ).thenReturn( key );
        when( operatorKvStore.getKVStore( key ) ).thenReturn( kvStore );
    }

    void initializeOperatorReplica ( final int inputPortCount, final int outputPortCount, final SchedulingStrategy schedulingStrategy )
    {
        mockOperatorDef( inputPortCount, outputPortCount );
        mockOperatorInitializationSchedulingStrategy( schedulingStrategy );

        initializationUpstreamContext = newUpstreamContextInstance( 0, inputPortCount, ACTIVE );
        operatorReplica.init( initializationUpstreamContext );
    }

    private void mockOperatorDef ( final int inputPortCount, final int outputPortCount )
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

    private void mockOperatorInitializationSchedulingStrategy ( final SchedulingStrategy schedulingStrategy )
    {
        when( operator.init( any( InitializationContext.class ) ) ).thenReturn( schedulingStrategy );
    }

    void assertOperatorInvocation ()
    {
        assertTrue( operatorReplica.isOperatorInvokedOnLastAttempt() );
        verify( operatorKvStore ).getKVStore( key );
        verify( operator ).invoke( invocationContext );
        verify( drainer ).reset();
    }

    void assertNoOperatorInvocation ()
    {
        assertFalse( operatorReplica.isOperatorInvokedOnLastAttempt() );
        verify( operatorKvStore, never() ).getKVStore( key );
        verify( operator, never() ).invoke( invocationContext );
    }

    void setOperatorReplicaStatus ( OperatorReplicaStatus status )
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
