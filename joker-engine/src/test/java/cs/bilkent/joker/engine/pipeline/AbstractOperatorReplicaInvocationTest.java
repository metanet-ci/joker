package cs.bilkent.joker.engine.pipeline;

import java.lang.reflect.Field;
import java.util.function.Supplier;

import org.junit.Before;
import org.mockito.Mock;

import cs.bilkent.joker.engine.config.JokerConfig;
import cs.bilkent.joker.engine.flow.PipelineId;
import cs.bilkent.joker.engine.kvstore.OperatorKVStore;
import cs.bilkent.joker.engine.metric.PipelineReplicaMeter;
import static cs.bilkent.joker.engine.pipeline.UpstreamContext.newInitialUpstreamContextWithAllPortsConnected;
import cs.bilkent.joker.engine.tuplequeue.OperatorTupleQueue;
import cs.bilkent.joker.engine.tuplequeue.TupleQueueDrainer;
import cs.bilkent.joker.engine.tuplequeue.TupleQueueDrainerPool;
import cs.bilkent.joker.operator.InitializationContext;
import static cs.bilkent.joker.operator.InvocationContext.InvocationReason.SHUTDOWN;
import cs.bilkent.joker.operator.Operator;
import cs.bilkent.joker.operator.OperatorDef;
import cs.bilkent.joker.operator.impl.InvocationContextImpl;
import cs.bilkent.joker.operator.impl.TuplesImpl;
import cs.bilkent.joker.operator.kvstore.KVStore;
import cs.bilkent.joker.operator.scheduling.SchedulingStrategy;
import cs.bilkent.joker.partition.impl.PartitionKey;
import cs.bilkent.joker.partition.impl.PartitionKey1;
import cs.bilkent.joker.test.AbstractJokerTest;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AbstractOperatorReplicaInvocationTest extends AbstractJokerTest
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

    protected InvocationContextImpl invocationContext = null;

    protected OperatorReplica operatorReplica;

    protected UpstreamContext initializationUpstreamContext;

    protected UpstreamContext downstreamContext;

    protected final int outputPortCount = 1;

    @Before
    public void before ()
    {
        applyDefaultMocks();
    }

    void applyDefaultMocks ()
    {
        when( outputSupplier.get() ).thenReturn( new TuplesImpl( outputPortCount ) );
        when( operatorDef.getId() ).thenReturn( "op1" );
        when( drainerPool.acquire( any( SchedulingStrategy.class ) ) ).thenReturn( drainer );
        when( operatorKvStore.getKVStore( key ) ).thenReturn( kvStore );
    }

    void initializeOperatorReplica ( final int inputPortCount, final SchedulingStrategy schedulingStrategy )
    {
        mockOperatorDef( inputPortCount, outputPortCount );

        final PipelineReplicaId pipelineReplicaId = new PipelineReplicaId( new PipelineId( 0, 0 ), 0 );
        operatorReplica = new OperatorReplica( pipelineReplicaId,
                                               operatorDef,
                                               queue,
                                               operatorKvStore,
                                               drainerPool,
                                               outputSupplier,
                                               mock( PipelineReplicaMeter.class ) );
        invocationContext = operatorReplica.getInvocationContext();

        mockOperatorInitializationSchedulingStrategy( schedulingStrategy );

        initializationUpstreamContext = newInitialUpstreamContextWithAllPortsConnected( inputPortCount );
        downstreamContext = newInitialUpstreamContextWithAllPortsConnected( outputPortCount );
        operatorReplica.init( initializationUpstreamContext, downstreamContext );
    }

    private void mockOperatorDef ( final int inputPortCount, final int outputPortCount )
    {
        when( operatorDef.getInputPortCount() ).thenReturn( inputPortCount );
        when( operatorDef.getOutputPortCount() ).thenReturn( outputPortCount );
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
        verify( operator ).invoke( invocationContext );
    }

    void assertNoOperatorInvocation ()
    {
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
            completionReasonField.set( operatorReplica, SHUTDOWN );
        }
        catch ( Exception e )
        {
            throw new RuntimeException( e );
        }
    }

}
