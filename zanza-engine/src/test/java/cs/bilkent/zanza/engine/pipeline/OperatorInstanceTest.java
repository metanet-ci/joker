package cs.bilkent.zanza.engine.pipeline;

import java.util.function.Supplier;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import cs.bilkent.zanza.engine.config.ZanzaConfig;
import cs.bilkent.zanza.engine.exception.InitializationException;
import cs.bilkent.zanza.engine.kvstore.KVStoreProvider;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueueContext;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueueDrainer;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueueDrainerPool;
import cs.bilkent.zanza.flow.OperatorDefinition;
import cs.bilkent.zanza.operator.InvocationContext.InvocationReason;
import static cs.bilkent.zanza.operator.InvocationContext.InvocationReason.INPUT_PORT_CLOSED;
import cs.bilkent.zanza.operator.Operator;
import cs.bilkent.zanza.operator.Tuple;
import cs.bilkent.zanza.operator.impl.InvocationContextImpl;
import cs.bilkent.zanza.operator.impl.TuplesImpl;
import cs.bilkent.zanza.operator.kvstore.KVStore;
import cs.bilkent.zanza.operator.scheduling.ScheduleNever;
import cs.bilkent.zanza.operator.scheduling.ScheduleWhenAvailable;
import cs.bilkent.zanza.operator.scheduling.ScheduleWhenTuplesAvailable;
import static cs.bilkent.zanza.operator.scheduling.ScheduleWhenTuplesAvailable.scheduleWhenTuplesAvailableOnDefaultPort;
import cs.bilkent.zanza.operator.scheduling.SchedulingStrategy;
import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyList;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith( MockitoJUnitRunner.class )
public class OperatorInstanceTest
{

    @Mock
    private TupleQueueContext queue;

    @Mock
    private Operator operator;

    @Mock
    private OperatorDefinition operatorDefinition;

    @Mock
    private KVStoreProvider kvStoreProvider;

    @Mock
    private KVStore kvStore;

    @Mock
    private TupleQueueDrainer drainer;

    @Mock
    private TupleQueueDrainerPool drainerPool;

    @Mock
    private Supplier<TuplesImpl> outputSupplier;

    @Mock
    private ZanzaConfig config;

    private final Object key = new Object();

    private final InvocationContextImpl invocationContext = new InvocationContextImpl();

    private OperatorInstance operatorInstance;

    @Before
    public void before ()
    {
        operatorInstance = new OperatorInstance( new PipelineInstanceId( 0, 0, 0 ),
                                                 operatorDefinition, queue,
                                                 kvStoreProvider, drainerPool, outputSupplier, invocationContext );
        when( operatorDefinition.id() ).thenReturn( "op1" );
        when( drainerPool.acquire( any( SchedulingStrategy.class ) ) ).thenReturn( drainer );
        when( drainer.getKey() ).thenReturn( key );
        when( kvStoreProvider.getKVStore( key ) ).thenReturn( kvStore );
    }

    @Test
    public void shouldSetStatusWhenInitializationSucceeds ()
    {
        initOperatorInstance( ScheduleNever.INSTANCE );
        assertThat( operatorInstance.status(), equalTo( OperatorInstanceStatus.RUNNING ) );
    }

    @Test
    public void shouldSetStatusWhenInitializationFails ()
    {
        createOperatorInstance();
        final RuntimeException exception = new RuntimeException();
        when( operator.init( anyObject() ) ).thenThrow( exception );

        try
        {
            operatorInstance.init( config );
            fail();
        }
        catch ( InitializationException expected )
        {
            assertThat( operatorInstance.status(), equalTo( OperatorInstanceStatus.INITIALIZATION_FAILED ) );
        }
    }

    @Test( expected = IllegalStateException.class )
    public void shouldFailToInvokeIfNotInitialized ()
    {
        operatorInstance.invoke( null );
    }

    @Test
    public void shouldNotInvokeOperatorAfterScheduleNever () throws Exception
    {
        initOperatorInstance( ScheduleNever.INSTANCE );

        operatorInstance.invoke( null );

        verify( queue, never() ).offer( anyInt(), anyObject() );
    }

    @Test
    public void shouldInvokeOperatorWhenSchedulingStrategySatisfied ()
    {
        final ScheduleWhenTuplesAvailable strategy = scheduleWhenTuplesAvailableOnDefaultPort( 1 );
        final ScheduleWhenTuplesAvailable expectedOutputStrategy = scheduleWhenTuplesAvailableOnDefaultPort( 2 );
        initOperatorInstance( strategy );

        when( drainerPool.acquire( expectedOutputStrategy ) ).thenReturn( drainer );
        final TuplesImpl operatorInput = TuplesImpl.newInstance( 1, new Tuple( "f1", "val2" ) );
        when( drainer.getResult() ).thenReturn( operatorInput );

        final TuplesImpl expectedOutput = TuplesImpl.newInstance( 1, new Tuple( "f1", "val3" ) );
        when( outputSupplier.get() ).thenReturn( expectedOutput );

        invocationContext.setNextSchedulingStrategy( expectedOutputStrategy );

        final TuplesImpl upstreamInput = TuplesImpl.newInstance( 1, new Tuple( "f1", "val1" ) );
        final TuplesImpl output = operatorInstance.invoke( upstreamInput );

        verify( queue ).offer( 0, singletonList( new Tuple( "f1", "val1" ) ) );
        verify( drainerPool ).acquire( strategy );
        verify( queue ).drain( drainer );
        verify( kvStoreProvider ).getKVStore( key );
        verify( operator ).invoke( invocationContext );
        verify( drainerPool ).release( drainer );
        verify( drainerPool ).acquire( expectedOutputStrategy );
        verify( drainer ).reset();

        assertThat( invocationContext.getReason(), equalTo( InvocationReason.SUCCESS ) );
        assertThat( invocationContext.getKVStore(), equalTo( kvStore ) );
        assertThat( invocationContext.getInput(), equalTo( operatorInput ) );

        assertThat( output, equalTo( expectedOutput ) );
        assertThat( operatorInstance.schedulingStrategy(), equalTo( expectedOutputStrategy ) );
    }

    @Test
    public void shouldNotInvokeOperatorWhenSchedulingStrategyNotSatisfied ()
    {
        final ScheduleWhenTuplesAvailable strategy = scheduleWhenTuplesAvailableOnDefaultPort( 1 );
        initOperatorInstance( strategy );

        final TuplesImpl upstreamInput = TuplesImpl.newInstance( 1, new Tuple( "f1", "val1" ) );
        final TuplesImpl output = operatorInstance.invoke( upstreamInput );

        verify( queue ).offer( 0, singletonList( new Tuple( "f1", "val1" ) ) );
        verify( drainerPool ).acquire( strategy );
        verify( queue ).drain( drainer );
        verify( kvStoreProvider, never() ).getKVStore( key );
        verify( operator, never() ).invoke( invocationContext );
        assertNull( output );
        assertThat( operatorInstance.schedulingStrategy(), equalTo( strategy ) );
    }

    @Test
    public void shouldNotForceInvokeOperatorAfterScheduleNever ()
    {
        initOperatorInstance( ScheduleNever.INSTANCE );

        operatorInstance.forceInvoke( null, INPUT_PORT_CLOSED );

        verify( queue, never() ).offer( anyInt(), anyObject() );
    }

    @Test
    public void shouldReturnScheduleNeverIfOperatorInvocationFailsWithException ()
    {
        initOperatorInstance( scheduleWhenTuplesAvailableOnDefaultPort( 1 ) );

        when( drainer.getResult() ).thenReturn( TuplesImpl.newInstance( 1, new Tuple( "f1", "val2" ) ) );
        doThrow( new RuntimeException() ).when( operator ).invoke( invocationContext );

        final TuplesImpl output = operatorInstance.invoke( null );

        assertNull( output );
        assertTrue( operatorInstance.schedulingStrategy() instanceof ScheduleNever );
    }

    @Test
    public void shouldSuppressExceptionThrownDuringForceInvoke ()
    {
        initOperatorInstance( scheduleWhenTuplesAvailableOnDefaultPort( 1 ) );

        doThrow( new RuntimeException() ).when( operator ).invoke( invocationContext );

        final TuplesImpl result = operatorInstance.forceInvoke( null, INPUT_PORT_CLOSED );

        assertNull( result );
        assertTrue( operatorInstance.schedulingStrategy() instanceof ScheduleNever );
    }

    @Test
    public void shouldForceInvokeRegardlessOfCurrentSchedulingStrategy ()
    {
        testForceInvoke( ScheduleNever.INSTANCE );
    }

    @Test
    public void shouldForcefullySetToScheduleNeverIfForceInvokeDoesNotReturnIt ()
    {
        testForceInvoke( scheduleWhenTuplesAvailableOnDefaultPort( 2 ) );
    }

    private void testForceInvoke ( final SchedulingStrategy outputStrategy )
    {
        initOperatorInstance( scheduleWhenTuplesAvailableOnDefaultPort( 1 ) );

        when( kvStoreProvider.getKVStore( null ) ).thenReturn( kvStore );
        final TuplesImpl upstreamInput = new TuplesImpl( 1 );
        final TuplesImpl operatorInput = TuplesImpl.newInstance( 1, new Tuple( "f1", "val" ) );
        when( drainer.getResult() ).thenReturn( operatorInput );

        final TuplesImpl output = TuplesImpl.newInstance( 1, new Tuple( "f1", "val1" ) );
        when( outputSupplier.get() ).thenReturn( output );

        invocationContext.setNextSchedulingStrategy( outputStrategy );

        final TuplesImpl result = operatorInstance.forceInvoke( upstreamInput, INPUT_PORT_CLOSED );

        verify( drainerPool ).acquire( ScheduleWhenAvailable.INSTANCE );
        verify( drainerPool, times( 2 ) ).release( drainer );
        verify( queue, never() ).offer( anyInt(), anyList() );
        verify( queue ).drain( drainer );
        verify( kvStoreProvider ).getKVStore( key );

        verify( operator ).invoke( invocationContext );

        assertThat( invocationContext.getReason(), equalTo( INPUT_PORT_CLOSED ) );
        assertThat( invocationContext.getKVStore(), equalTo( kvStore ) );
        assertThat( invocationContext.getInput(), equalTo( operatorInput ) );

        assertThat( result, equalTo( output ) );
        assertThat( operatorInstance.schedulingStrategy(), equalTo( ScheduleNever.INSTANCE ) );
    }

    private void initOperatorInstance ( final SchedulingStrategy strategy )
    {
        createOperatorInstance();
        when( operator.init( anyObject() ) ).thenReturn( strategy );
        operatorInstance.init( mock( ZanzaConfig.class ) );
        when( drainerPool.acquire( strategy ) ).thenReturn( drainer );
    }

    private void createOperatorInstance ()
    {
        try
        {
            when( operatorDefinition.createOperator() ).thenReturn( operator );
        }
        catch ( Exception e )
        {
            throw new RuntimeException( e );
        }
    }

}
