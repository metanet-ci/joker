package cs.bilkent.zanza.engine.pipeline;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import cs.bilkent.zanza.engine.config.ZanzaConfig;
import cs.bilkent.zanza.engine.exception.InitializationException;
import cs.bilkent.zanza.operator.InvocationContext.InvocationReason;
import static cs.bilkent.zanza.operator.InvocationContext.InvocationReason.INPUT_PORT_CLOSED;
import cs.bilkent.zanza.operator.Tuple;
import cs.bilkent.zanza.operator.impl.TuplesImpl;
import cs.bilkent.zanza.operator.scheduling.ScheduleNever;
import cs.bilkent.zanza.operator.scheduling.ScheduleWhenAvailable;
import cs.bilkent.zanza.operator.scheduling.SchedulingStrategy;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith( MockitoJUnitRunner.class )
public class PipelineInstanceTest
{

    @Mock
    private OperatorInstance operator0;

    @Mock
    private OperatorInstance operator1;

    @Mock
    private OperatorInstance operator2;

    @Mock
    private ZanzaConfig config;

    private PipelineInstance pipeline;

    private final SchedulingStrategy continuingSchedulingStrategy = ScheduleWhenAvailable.INSTANCE;

    private final SchedulingStrategy terminatingSchedulingStrategy = ScheduleNever.INSTANCE;

    private final TuplesImpl upstreamInput1 = new TuplesImpl( 1 );

    private final TuplesImpl upstreamInput2 = new TuplesImpl( 1 );

    private final TuplesImpl output = new TuplesImpl( 1 );

    @Before
    public void before ()
    {
        pipeline = new PipelineInstance( new PipelineInstanceId( 0, 0, 0 ), new OperatorInstance[] { operator0, operator1, operator2 } );

        upstreamInput1.add( new Tuple( "k1", "v1" ) );
        upstreamInput2.add( new Tuple( "k2", "v2" ) );
        output.add( new Tuple( "k3", "v3" ) );
    }

    @Test
    public void shouldInitOperatorsSuccessfully ()
    {
        pipeline.init( config );

        verify( operator0 ).init( config );
        verify( operator1 ).init( config );
        verify( operator2 ).init( config );
    }

    @Test
    public void shouldShutdownInitializedOperatorsWhenAnOperatorFailsToInit ()
    {
        doThrow( new InitializationException( "" ) ).when( operator1 ).init( config );

        try
        {
            pipeline.init( config );
            fail();
        }
        catch ( InitializationException expected )
        {

        }

        verify( operator0 ).init( config );
        verify( operator0 ).shutdown();
        verify( operator1 ).init( config );
        verify( operator1 ).shutdown();
        verify( operator2, never() ).init( config );
    }

    @Test
    public void shouldShutdownOperators ()
    {
        pipeline.init( config );
        pipeline.shutdown();

        verify( operator0 ).shutdown();
        verify( operator1 ).shutdown();
        verify( operator2 ).shutdown();
    }

    @Test
    public void shouldShutdownOperatorsOnlyOnce ()
    {
        pipeline.init( config );
        pipeline.shutdown();
        pipeline.shutdown();

        verify( operator0, times( 1 ) ).shutdown();
        verify( operator1, times( 1 ) ).shutdown();
        verify( operator2, times( 1 ) ).shutdown();
    }

    @Test
    public void shouldInvokeAllOperatorsWithContinuingSchedulingStrategies ()
    {
        pipeline.init( config );

        mockOperatorInvoke( operator0, null, continuingSchedulingStrategy, upstreamInput1 );
        mockOperatorInvoke( operator1, upstreamInput1, continuingSchedulingStrategy, upstreamInput2 );
        mockOperatorInvoke( operator2, upstreamInput2, continuingSchedulingStrategy, output );

        final TuplesImpl result = pipeline.invoke();

        verify( operator0 ).invoke( null );
        verify( operator1 ).invoke( upstreamInput1 );
        verify( operator2 ).invoke( upstreamInput2 );

        assertTrue( result == output );
        assertTrue( pipeline.isProducingDownstreamTuples() );
        assertTrue( pipeline.isInvokableOperatorAvailable() );
    }

    @Test
    public void shouldNotProduceDownstreamTuplesWhenLastOperatorBecomesNonInvokable ()
    {
        pipeline.init( config );

        mockOperatorInvoke( operator0, null, continuingSchedulingStrategy, upstreamInput1 );
        mockOperatorInvoke( operator1, upstreamInput1, continuingSchedulingStrategy, upstreamInput2 );
        mockOperatorInvoke( operator2, upstreamInput2, terminatingSchedulingStrategy, output );

        final TuplesImpl result = pipeline.invoke();

        verify( operator0 ).invoke( null );
        verify( operator1 ).invoke( upstreamInput1 );
        verify( operator2 ).invoke( upstreamInput2 );

        assertTrue( result == output );
        assertFalse( pipeline.isProducingDownstreamTuples() );
        assertTrue( pipeline.isInvokableOperatorAvailable() );
    }

    @Test
    public void shouldForceInvokeAllOperatorsAfterFirstScheduleNeverOperator ()
    {
        pipeline.init( config );

        mockOperatorInvoke( operator0, null, terminatingSchedulingStrategy, null );
        mockOperatorForceInvoke( operator1, INPUT_PORT_CLOSED, null, true, continuingSchedulingStrategy, upstreamInput2 );
        mockOperatorForceInvoke( operator2, INPUT_PORT_CLOSED, upstreamInput2, false, continuingSchedulingStrategy, output );

        final TuplesImpl result = pipeline.invoke();

        verify( operator0 ).invoke( null );
        verify( operator1 ).forceInvoke( INPUT_PORT_CLOSED, null, true );
        verify( operator2 ).forceInvoke( INPUT_PORT_CLOSED, upstreamInput2, false );

        assertTrue( result == output );
        assertTrue( pipeline.isProducingDownstreamTuples() );
        assertTrue( pipeline.isInvokableOperatorAvailable() );
    }

    @Test
    public void shouldForceInvokeOperators ()
    {
        pipeline.init( config );

        mockOperatorForceInvoke( operator0, INPUT_PORT_CLOSED, null, true, terminatingSchedulingStrategy, null );
        mockOperatorForceInvoke( operator1, INPUT_PORT_CLOSED, null, true, terminatingSchedulingStrategy, upstreamInput2 );
        mockOperatorForceInvoke( operator2, INPUT_PORT_CLOSED, upstreamInput2, true, continuingSchedulingStrategy, output );

        final TuplesImpl result = pipeline.forceInvoke( INPUT_PORT_CLOSED );

        verify( operator0 ).forceInvoke( INPUT_PORT_CLOSED, null, true );
        verify( operator1 ).forceInvoke( INPUT_PORT_CLOSED, null, true );
        verify( operator2 ).forceInvoke( INPUT_PORT_CLOSED, upstreamInput2, true );

        assertTrue( result == output );
        assertTrue( pipeline.isProducingDownstreamTuples() );
        assertTrue( pipeline.isInvokableOperatorAvailable() );
    }

    @Test
    public void shouldBeNoInvokableOperatorsAfterAllOperatorsSetScheduleNever ()
    {
        pipeline.init( config );

        mockOperatorForceInvoke( operator0, INPUT_PORT_CLOSED, null, true, terminatingSchedulingStrategy, null );
        mockOperatorForceInvoke( operator1, INPUT_PORT_CLOSED, null, true, terminatingSchedulingStrategy, upstreamInput2 );

        when( operator2.isInvokable() ).thenReturn( true );
        when( operator2.forceInvoke( INPUT_PORT_CLOSED, upstreamInput2, true ) ).thenReturn( output );
        when( operator2.getSchedulingStrategy() ).thenReturn( terminatingSchedulingStrategy );
        when( operator2.isNonInvokable() ).thenReturn( false, true );
        when( operator2.isInvokable() ).thenReturn( true, false );

        final TuplesImpl result = pipeline.forceInvoke( INPUT_PORT_CLOSED );

        verify( operator0 ).forceInvoke( INPUT_PORT_CLOSED, null, true );
        verify( operator1 ).forceInvoke( INPUT_PORT_CLOSED, null, true );
        verify( operator2 ).forceInvoke( INPUT_PORT_CLOSED, upstreamInput2, true );

        assertTrue( result == output );
        assertFalse( pipeline.isProducingDownstreamTuples() );
        assertFalse( pipeline.isInvokableOperatorAvailable() );
    }

    private void mockOperatorInvoke ( final OperatorInstance operator,
                                      final TuplesImpl input,
                                      final SchedulingStrategy schedulingStrategy,
                                      final TuplesImpl output )
    {
        when( operator.invoke( input ) ).thenReturn( output );
        when( operator.getSchedulingStrategy() ).thenReturn( schedulingStrategy );
        final boolean nonInvokable = schedulingStrategy instanceof ScheduleNever;
        when( operator.isNonInvokable() ).thenReturn( nonInvokable );
        when( operator.isInvokable() ).thenReturn( !nonInvokable );
    }

    private void mockOperatorForceInvoke ( final OperatorInstance operator,
                                           final InvocationReason reason,
                                           final TuplesImpl input,
                                           final boolean upstreamClosed,
                                           final SchedulingStrategy schedulingStrategy,
                                           final TuplesImpl output )
    {
        when( operator.forceInvoke( reason, input, upstreamClosed ) ).thenReturn( output );
        when( operator.getSchedulingStrategy() ).thenReturn( schedulingStrategy );
        final boolean nonInvokable = schedulingStrategy instanceof ScheduleNever;
        when( operator.isNonInvokable() ).thenReturn( nonInvokable );
        when( operator.isInvokable() ).thenReturn( !nonInvokable );
    }

}
