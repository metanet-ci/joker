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
import cs.bilkent.zanza.engine.tuplequeue.TupleQueueContext;
import cs.bilkent.zanza.flow.OperatorDefinition;
import cs.bilkent.zanza.operator.Tuple;
import cs.bilkent.zanza.operator.impl.TuplesImpl;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith( MockitoJUnitRunner.class )
public class PipelineInstanceTest
{

    @Mock
    private TupleQueueContext upstreamTupleQueueContext;

    @Mock
    private OperatorInstance operator0;

    @Mock
    private OperatorInstance operator1;

    @Mock
    private OperatorInstance operator2;

    private PipelineInstance pipeline;

    private final ZanzaConfig config = new ZanzaConfig();

    private final TuplesImpl upstreamInput1 = new TuplesImpl( 1 );

    private final TuplesImpl upstreamInput2 = new TuplesImpl( 1 );

    private final TuplesImpl output = new TuplesImpl( 1 );

    private final UpstreamContext upstreamContext0 = new UpstreamContext( 0, new UpstreamConnectionStatus[] { ACTIVE } );

    private final UpstreamContext upstreamContext1 = new UpstreamContext( 0, new UpstreamConnectionStatus[] { ACTIVE } );

    private final UpstreamContext upstreamContext2 = new UpstreamContext( 0, new UpstreamConnectionStatus[] { ACTIVE } );

    @Before
    public void before ()
    {
        final OperatorDefinition operatorDefinition0 = mock( OperatorDefinition.class );
        when( operator0.getOperatorDefinition() ).thenReturn( operatorDefinition0 );
        when( operatorDefinition0.inputPortCount() ).thenReturn( 1 );
        pipeline = new PipelineInstance( new PipelineInstanceId( 0, 0, 0 ),
                                         new OperatorInstance[] { operator0, operator1, operator2 },
                                         upstreamTupleQueueContext );

        upstreamInput1.add( new Tuple( "k1", "v1" ) );
        upstreamInput2.add( new Tuple( "k2", "v2" ) );
        output.add( new Tuple( "k3", "v3" ) );
    }

    @Test
    public void shouldInitOperatorsSuccessfully ()
    {
        when( operator0.getSelfUpstreamContext() ).thenReturn( upstreamContext1 );
        when( operator1.getSelfUpstreamContext() ).thenReturn( upstreamContext2 );

        pipeline.init( config, upstreamContext0, null );

        verify( operator0 ).init( config, upstreamContext0, null );
        verify( operator1 ).init( config, upstreamContext1, null );
        verify( operator2 ).init( config, upstreamContext2, null );
    }

    @Test
    public void shouldShutdownInitializedOperatorsWhenAnOperatorFailsToInit ()
    {
        when( operator0.getSelfUpstreamContext() ).thenReturn( upstreamContext1 );
        doThrow( new InitializationException( "" ) ).when( operator1 ).init( config, upstreamContext1, null );

        try
        {
            pipeline.init( config, upstreamContext0, null );
            fail();
        }
        catch ( InitializationException expected )
        {

        }

        verify( operator0 ).init( config, upstreamContext0, null );
        verify( operator0 ).shutdown();
        verify( operator1 ).init( config, upstreamContext1, null );
        verify( operator1 ).shutdown();
        verify( operator2, never() ).init( anyObject(), anyObject(), anyObject() );
    }

    @Test
    public void shouldShutdownOperators ()
    {
        when( operator0.getSelfUpstreamContext() ).thenReturn( upstreamContext1 );
        when( operator1.getSelfUpstreamContext() ).thenReturn( upstreamContext2 );
        pipeline.init( config, upstreamContext0, null );
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
        pipeline.init( config, upstreamContext0, null );
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
        pipeline.init( config, upstreamContext0, null );

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
        pipeline.init( config, upstreamContext0, null );

        when( operator0.invoke( null, upstreamContext0 ) ).thenReturn( upstreamInput1 );
        when( operator1.invoke( upstreamInput1, upstreamContext1 ) ).thenReturn( upstreamInput2 );
        when( operator2.invoke( upstreamInput2, upstreamContext2 ) ).thenReturn( output );

        final TuplesImpl result = pipeline.invoke();

        assertThat( result, equalTo( output ) );

        verify( operator0 ).invoke( null, upstreamContext0 );
        verify( operator1 ).invoke( upstreamInput1, upstreamContext1 );
        verify( operator2 ).invoke( upstreamInput2, upstreamContext2 );
    }

}
