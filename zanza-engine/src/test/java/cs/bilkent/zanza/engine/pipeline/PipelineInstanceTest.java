package cs.bilkent.zanza.engine.pipeline;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import cs.bilkent.zanza.engine.config.ZanzaConfig;
import cs.bilkent.zanza.engine.exception.InitializationException;
import static cs.bilkent.zanza.engine.pipeline.UpstreamConnectionStatus.ACTIVE;
import static cs.bilkent.zanza.engine.pipeline.UpstreamConnectionStatus.CLOSED;
import cs.bilkent.zanza.operator.Tuple;
import cs.bilkent.zanza.operator.impl.TuplesImpl;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyObject;
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

    private final TuplesImpl upstreamInput1 = new TuplesImpl( 1 );

    private final TuplesImpl upstreamInput2 = new TuplesImpl( 1 );

    private final TuplesImpl output = new TuplesImpl( 1 );

    private final UpstreamContext upstreamContext0 = new UpstreamContext( 0, new UpstreamConnectionStatus[] { ACTIVE } );

    private final UpstreamContext upstreamContext1 = new UpstreamContext( 0, new UpstreamConnectionStatus[] { ACTIVE } );

    private final UpstreamContext upstreamContext2 = new UpstreamContext( 0, new UpstreamConnectionStatus[] { ACTIVE } );

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
        when( operator0.getSelfUpstreamContext() ).thenReturn( upstreamContext1 );
        when( operator1.getSelfUpstreamContext() ).thenReturn( upstreamContext2 );

        pipeline.init( config, upstreamContext0 );

        verify( operator0 ).init( config, upstreamContext0 );
        verify( operator1 ).init( config, upstreamContext1 );
        verify( operator2 ).init( config, upstreamContext2 );
    }

    @Test
    public void shouldShutdownInitializedOperatorsWhenAnOperatorFailsToInit ()
    {
        when( operator0.getSelfUpstreamContext() ).thenReturn( upstreamContext1 );
        doThrow( new InitializationException( "" ) ).when( operator1 ).init( config, upstreamContext1 );

        try
        {
            pipeline.init( config, upstreamContext0 );
            fail();
        }
        catch ( InitializationException expected )
        {

        }

        verify( operator0 ).init( config, upstreamContext0 );
        verify( operator0 ).shutdown();
        verify( operator1 ).init( config, upstreamContext1 );
        verify( operator1 ).shutdown();
        verify( operator2, never() ).init( anyObject(), anyObject() );
    }

    @Test
    public void shouldShutdownOperators ()
    {
        when( operator0.getSelfUpstreamContext() ).thenReturn( upstreamContext1 );
        when( operator1.getSelfUpstreamContext() ).thenReturn( upstreamContext2 );
        pipeline.init( config, upstreamContext0 );
        pipeline.shutdown();

        verify( operator0 ).shutdown();
        verify( operator1 ).shutdown();
        verify( operator2 ).shutdown();
    }

    @Test
    public void shouldShutdownOperatorsOnlyOnce ()
    {
        when( operator0.getSelfUpstreamContext() ).thenReturn( upstreamContext1 );
        when( operator1.getSelfUpstreamContext() ).thenReturn( upstreamContext2 );
        pipeline.init( config, upstreamContext0 );
        pipeline.shutdown();
        pipeline.shutdown();

        verify( operator0, times( 1 ) ).shutdown();
        verify( operator1, times( 1 ) ).shutdown();
        verify( operator2, times( 1 ) ).shutdown();
    }

    @Test
    public void shouldInvokeFirstOperatorWithUpdatedUpstreamContext ()
    {
        when( operator0.getSelfUpstreamContext() ).thenReturn( upstreamContext1 );
        when( operator1.getSelfUpstreamContext() ).thenReturn( upstreamContext2 );
        pipeline.init( config, upstreamContext0 );

        final UpstreamContext upstreamContext0New = new UpstreamContext( 1, new UpstreamConnectionStatus[] { CLOSED } );
        pipeline.setPipelineUpstreamContext( upstreamContext0New );
        pipeline.invoke();

        verify( operator0 ).invoke( null, upstreamContext0New );
    }

    @Test
    public void shouldInvokeOperators ()
    {
        when( operator0.getSelfUpstreamContext() ).thenReturn( upstreamContext1 );
        when( operator1.getSelfUpstreamContext() ).thenReturn( upstreamContext2 );
        pipeline.init( config, upstreamContext0 );

        when( operator0.invoke( null, upstreamContext0 ) ).thenReturn( upstreamInput1 );
        when( operator1.invoke( upstreamInput1, upstreamContext1 ) ).thenReturn( upstreamInput2 );
        when( operator2.invoke( upstreamInput2, upstreamContext2 ) ).thenReturn( output );

        final TuplesImpl result = pipeline.invoke();

        assertThat( result, equalTo( output ) );

        verify( operator0 ).invoke( null, upstreamContext0 );
        verify( operator1 ).invoke( upstreamInput1, upstreamContext1 );
        verify( operator2 ).invoke( upstreamInput2, upstreamContext2 );
    }

    @Test
    public void shouldProduceDownstreamTuplesWhenLastOperatorIsInvokeable ()
    {
        when( operator2.isInvokable() ).thenReturn( false );
        assertFalse( pipeline.isProducingDownstreamTuples() );
    }

    @Test
    public void shouldNotProduceDownstreamTuplesWhenLastOperatorIsNonInvokeable ()
    {
        when( operator2.isInvokable() ).thenReturn( true );
        assertTrue( pipeline.isProducingDownstreamTuples() );
    }

    @Test
    public void shouldInvokableOperatorBePresent ()
    {
        when( operator2.isInvokable() ).thenReturn( true );
        assertTrue( pipeline.isInvokableOperatorPresent() );
        assertFalse( pipeline.isInvokableOperatorAbsent() );
    }

    @Test
    public void shouldInvokableOperatorBeAbsent ()
    {
        assertFalse( pipeline.isInvokableOperatorPresent() );
        assertTrue( pipeline.isInvokableOperatorAbsent() );
    }

}
