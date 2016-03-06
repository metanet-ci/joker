package cs.bilkent.zanza.engine.pipeline;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import cs.bilkent.zanza.engine.exception.InitializationException;
import cs.bilkent.zanza.operator.InvocationContext.InvocationReason;
import cs.bilkent.zanza.operator.InvocationResult;
import cs.bilkent.zanza.operator.PortsToTuples;
import cs.bilkent.zanza.operator.Tuple;
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

    private PipelineInstance pipeline;

    private final SchedulingStrategy continuingSchedulingStrategy = ScheduleWhenAvailable.INSTANCE;

    private final SchedulingStrategy terminatingSchedulingStrategy = ScheduleNever.INSTANCE;

    private final PortsToTuples upstreamInput1 = new PortsToTuples();

    private final PortsToTuples upstreamInput2 = new PortsToTuples();

    private final PortsToTuples output = new PortsToTuples();

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
        pipeline.init();

        verify( operator0 ).init();
        verify( operator1 ).init();
        verify( operator2 ).init();
    }

    @Test
    public void shouldShutdownInitializedOperatorsWhenAnOperatorFailsToInit ()
    {
        doThrow( new InitializationException( "" ) ).when( operator1 ).init();

        try
        {
            pipeline.init();
            fail();
        }
        catch ( InitializationException expected )
        {

        }

        verify( operator0 ).init();
        verify( operator0 ).shutdown();
        verify( operator1 ).init();
        verify( operator1 ).shutdown();
        verify( operator2, never() ).init();
    }

    @Test
    public void shouldShutdownOperators ()
    {
        pipeline.init();
        pipeline.shutdown();

        verify( operator0 ).shutdown();
        verify( operator1 ).shutdown();
        verify( operator2 ).shutdown();
    }

    @Test
    public void shouldShutdownOperatorsOnlyOnce ()
    {
        pipeline.init();
        pipeline.shutdown();
        pipeline.shutdown();

        verify( operator0, times( 1 ) ).shutdown();
        verify( operator1, times( 1 ) ).shutdown();
        verify( operator2, times( 1 ) ).shutdown();
    }

    @Test
    public void shouldInvokeAllOperatorsWithContinuingSchedulingStrategies ()
    {
        pipeline.init();

        when( operator0.invoke( null ) ).thenReturn( continuingResult( upstreamInput1 ) );
        when( operator1.invoke( upstreamInput1 ) ).thenReturn( continuingResult( upstreamInput2 ) );
        when( operator2.invoke( upstreamInput2 ) ).thenReturn( continuingResult( output ) );

        final PortsToTuples result = pipeline.invoke();

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
        pipeline.init();

        when( operator0.invoke( null ) ).thenReturn( continuingResult( upstreamInput1 ) );
        when( operator1.invoke( upstreamInput1 ) ).thenReturn( continuingResult( upstreamInput2 ) );
        when( operator2.invoke( upstreamInput2 ) ).thenReturn( terminatingResult( output ) );

        final PortsToTuples result = pipeline.invoke();

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
        pipeline.init();

        when( operator0.invoke( null ) ).thenReturn( continuingResult( upstreamInput1 ) );
        when( operator1.invoke( upstreamInput1 ) ).thenReturn( terminatingResult( upstreamInput2 ) );
        when( operator2.forceInvoke( upstreamInput2, InvocationReason.INPUT_PORT_CLOSED ) ).thenReturn( output );

        final PortsToTuples result = pipeline.invoke();

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
        pipeline.init();

        when( operator0.invoke( null ) ).thenReturn( terminatingResult( upstreamInput1 ) );
        when( operator1.forceInvoke( upstreamInput1, InvocationReason.INPUT_PORT_CLOSED ) ).thenReturn( upstreamInput2 );
        when( operator2.forceInvoke( upstreamInput2, InvocationReason.INPUT_PORT_CLOSED ) ).thenReturn( output );

        final PortsToTuples result = pipeline.invoke();

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
        pipeline.init();

        when( operator0.invoke( null ) ).thenReturn( continuingResult( upstreamInput1 ) );
        when( operator1.invoke( upstreamInput1 ) ).thenReturn( terminatingResult( upstreamInput2 ) );
        when( operator2.forceInvoke( upstreamInput2, InvocationReason.INPUT_PORT_CLOSED ) ).thenReturn( output );

        final PortsToTuples result1 = pipeline.invoke();
        final PortsToTuples result2 = pipeline.invoke();

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
        pipeline.init();

        when( operator0.invoke( null ) ).thenReturn( continuingResult( upstreamInput1 ), terminatingResult( upstreamInput1 ) );
        when( operator1.invoke( upstreamInput1 ) ).thenReturn( continuingResult( upstreamInput2 ) );
        when( operator2.invoke( upstreamInput2 ) ).thenReturn( terminatingResult( output ) );

        when( operator1.forceInvoke( upstreamInput1, InvocationReason.INPUT_PORT_CLOSED ) ).thenReturn( upstreamInput2 );

        assertNotNull( pipeline.invoke() );

        assertNull( pipeline.invoke() );
    }

    @Test
    public void shouldForceInvokeReturnOutputTuplesOnLastOperator ()
    {
        pipeline.init();

        when( operator0.invoke( null ) ).thenReturn( continuingResult( upstreamInput1 ), terminatingResult( upstreamInput1 ) );
        when( operator1.invoke( upstreamInput1 ) ).thenReturn( terminatingResult( upstreamInput2 ) );

        when( operator2.forceInvoke( upstreamInput2, InvocationReason.INPUT_PORT_CLOSED ) ).thenReturn( output );

        final PortsToTuples result = pipeline.invoke();

        assertTrue( result == output );
    }

    private InvocationResult continuingResult ( final PortsToTuples output )
    {
        return new InvocationResult( continuingSchedulingStrategy, output );
    }

    private InvocationResult terminatingResult ( final PortsToTuples output )
    {
        return new InvocationResult( terminatingSchedulingStrategy, output );
    }

}
