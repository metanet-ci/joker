package cs.bilkent.zanza.engine.pipeline;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import cs.bilkent.zanza.engine.config.ZanzaConfig;
import cs.bilkent.zanza.engine.exception.InitializationException;
import cs.bilkent.zanza.operator.InvocationContext.InvocationReason;
import cs.bilkent.zanza.operator.Tuple;
import cs.bilkent.zanza.operator.impl.TuplesImpl;
import cs.bilkent.zanza.operator.scheduling.ScheduleNever;
import cs.bilkent.zanza.operator.scheduling.ScheduleWhenAvailable;
import cs.bilkent.zanza.operator.scheduling.SchedulingStrategy;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
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

        mockOperator( operator0, null, continuingSchedulingStrategy, upstreamInput1 );
        mockOperator( operator1, upstreamInput1, continuingSchedulingStrategy, upstreamInput2 );
        mockOperator( operator2, upstreamInput2, continuingSchedulingStrategy, output );

        final TuplesImpl result = pipeline.invoke();

        verify( operator0 ).invoke( null );
        verify( operator1 ).invoke( upstreamInput1 );
        verify( operator2 ).invoke( upstreamInput2 );

        assertTrue( result == output );
        assertThat( pipeline.currentHighestInvokableIndex(), equalTo( 2 ) );
        assertTrue( pipeline.isInvokable() );
    }

    @Test
    public void shouldInvokeAllOperatorsWhenLastOperatorReturnsTerminatingSchedulingStrategy ()
    {
        pipeline.init( config );

        mockOperator( operator0, null, continuingSchedulingStrategy, upstreamInput1 );
        mockOperator( operator1, upstreamInput1, continuingSchedulingStrategy, upstreamInput2 );
        mockOperator( operator2, upstreamInput2, terminatingSchedulingStrategy, output );

        final TuplesImpl result = pipeline.invoke();

        verify( operator0 ).invoke( null );
        verify( operator1 ).invoke( upstreamInput1 );
        verify( operator2 ).invoke( upstreamInput2 );

        assertTrue( result == output );
        assertThat( pipeline.currentHighestInvokableIndex(), equalTo( 1 ) );
        assertTrue( pipeline.isInvokable() );
    }

    @Test
    public void shouldForceInvokeOperatorsSubsequentOfTerminatingSchedulingStrategy ()
    {
        pipeline.init( config );

        mockOperator( operator0, null, continuingSchedulingStrategy, upstreamInput1 );
        mockOperator( operator1, upstreamInput1, terminatingSchedulingStrategy, upstreamInput2 );
        when( operator2.forceInvoke( upstreamInput2, InvocationReason.INPUT_PORT_CLOSED ) ).thenReturn( output );

        final TuplesImpl result = pipeline.invoke();

        verify( operator0 ).invoke( null );
        verify( operator1 ).invoke( upstreamInput1 );
        verify( operator2 ).forceInvoke( upstreamInput2, InvocationReason.INPUT_PORT_CLOSED );
        verify( operator2 ).shutdown();

        assertTrue( result == output );
        assertThat( pipeline.currentHighestInvokableIndex(), equalTo( 0 ) );
        assertTrue( pipeline.isInvokable() );
    }

    @Test
    public void shouldForceInvokeOperatorsSubsequentOfTerminatingSchedulingStrategy2 ()
    {
        pipeline.init( config );

        mockOperator( operator0, null, terminatingSchedulingStrategy, upstreamInput1 );
        when( operator1.forceInvoke( upstreamInput1, InvocationReason.INPUT_PORT_CLOSED ) ).thenReturn( upstreamInput2 );
        when( operator2.forceInvoke( upstreamInput2, InvocationReason.INPUT_PORT_CLOSED ) ).thenReturn( output );

        final TuplesImpl result = pipeline.invoke();

        verify( operator0 ).invoke( null );
        verify( operator1 ).forceInvoke( upstreamInput1, InvocationReason.INPUT_PORT_CLOSED );
        verify( operator1 ).shutdown();
        verify( operator2 ).forceInvoke( upstreamInput2, InvocationReason.INPUT_PORT_CLOSED );
        verify( operator2 ).shutdown();

        assertTrue( result == output );
        assertThat( pipeline.currentHighestInvokableIndex(), equalTo( PipelineInstance.NO_INVOKABLE_INDEX ) );
        assertFalse( pipeline.isInvokable() );
    }

    @Test
    public void shouldForceInvokeOperatorsSubsequentOfTerminatingSchedulingStrategyOnlyOnce ()
    {
        pipeline.init( config );

        mockOperator( operator0, null, continuingSchedulingStrategy, upstreamInput1 );
        mockOperator( operator1, upstreamInput1, terminatingSchedulingStrategy, upstreamInput2 );
        when( operator2.forceInvoke( upstreamInput2, InvocationReason.INPUT_PORT_CLOSED ) ).thenReturn( output );

        final TuplesImpl result1 = pipeline.invoke();
        final TuplesImpl result2 = pipeline.invoke();

        verify( operator0, times( 2 ) ).invoke( null );
        verify( operator1, times( 1 ) ).invoke( upstreamInput1 );
        verify( operator1, times( 1 ) ).invoke( any() );
        verify( operator2, times( 1 ) ).forceInvoke( upstreamInput2, InvocationReason.INPUT_PORT_CLOSED );
        verify( operator2, times( 1 ) ).forceInvoke( any(), any() );
        verify( operator2 ).shutdown();

        assertTrue( result1 == output );
        assertThat( pipeline.currentHighestInvokableIndex(), equalTo( 0 ) );
        assertNull( result2 );
        assertTrue( pipeline.isInvokable() );
    }

    @Test
    public void shouldForceInvokeNotReturnOutputTuplesAfterDownstreamOperatorsAfterAlreadyShutdown ()
    {
        pipeline.init( config );

        when( operator0.invoke( null ) ).thenReturn( upstreamInput1, upstreamInput1 );
        when( operator0.schedulingStrategy() ).thenReturn( continuingSchedulingStrategy, continuingSchedulingStrategy );
        mockOperator( operator1, upstreamInput1, continuingSchedulingStrategy, upstreamInput2 );
        mockOperator( operator2, upstreamInput2, terminatingSchedulingStrategy, output );

        when( operator1.forceInvoke( upstreamInput1, InvocationReason.INPUT_PORT_CLOSED ) ).thenReturn( upstreamInput2 );

        assertNotNull( pipeline.invoke() );

        assertNull( pipeline.invoke() );
    }

    @Test
    public void shouldForceInvokeReturnOutputTuplesOnLastOperator ()
    {
        pipeline.init( config );

        when( operator0.invoke( null ) ).thenReturn( upstreamInput1, upstreamInput1 );
        when( operator0.schedulingStrategy() ).thenReturn( continuingSchedulingStrategy, terminatingSchedulingStrategy );
        mockOperator( operator1, upstreamInput1, terminatingSchedulingStrategy, upstreamInput2 );

        when( operator2.forceInvoke( upstreamInput2, InvocationReason.INPUT_PORT_CLOSED ) ).thenReturn( output );

        final TuplesImpl result = pipeline.invoke();

        assertTrue( result == output );
    }

    private void mockOperator ( final OperatorInstance operator,
                                final TuplesImpl input,
                                final SchedulingStrategy schedulingStrategy,
                                final TuplesImpl output )
    {
        when( operator.invoke( input ) ).thenReturn( output );
        when( operator.schedulingStrategy() ).thenReturn( schedulingStrategy );
    }

}
