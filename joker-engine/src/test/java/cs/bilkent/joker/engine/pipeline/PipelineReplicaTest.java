package cs.bilkent.joker.engine.pipeline;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import cs.bilkent.joker.engine.config.JokerConfig;
import cs.bilkent.joker.engine.exception.InitializationException;
import cs.bilkent.joker.engine.metric.PipelineReplicaMeter;
import static cs.bilkent.joker.engine.pipeline.OperatorReplicaStatus.INITIAL;
import static cs.bilkent.joker.engine.pipeline.OperatorReplicaStatus.RUNNING;
import static cs.bilkent.joker.engine.pipeline.OperatorReplicaStatus.SHUT_DOWN;
import static cs.bilkent.joker.engine.pipeline.UpstreamConnectionStatus.ACTIVE;
import static cs.bilkent.joker.engine.pipeline.UpstreamConnectionStatus.CLOSED;
import cs.bilkent.joker.engine.tuplequeue.OperatorTupleQueue;
import cs.bilkent.joker.operator.OperatorDef;
import cs.bilkent.joker.operator.Tuple;
import cs.bilkent.joker.operator.impl.TuplesImpl;
import cs.bilkent.joker.test.AbstractJokerTest;
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
public class PipelineReplicaTest extends AbstractJokerTest
{

    @Mock
    private OperatorTupleQueue pipelineTupleQueue;

    @Mock
    private OperatorReplica operator0;

    @Mock
    private OperatorReplica operator1;

    @Mock
    private OperatorReplica operator2;

    private PipelineReplica pipeline;

    private final JokerConfig config = new JokerConfig();

    private final TuplesImpl upstreamInput1 = new TuplesImpl( 1 );

    private final TuplesImpl upstreamInput2 = new TuplesImpl( 1 );

    private final TuplesImpl output = new TuplesImpl( 1 );

    private final UpstreamContext upstreamContext0 = new UpstreamContext( 0, new UpstreamConnectionStatus[] { ACTIVE } );

    private final UpstreamContext upstreamContext1 = new UpstreamContext( 0, new UpstreamConnectionStatus[] { ACTIVE } );

    private final UpstreamContext upstreamContext2 = new UpstreamContext( 0, new UpstreamConnectionStatus[] { ACTIVE } );

    @Before
    public void before ()
    {
        final OperatorDef operatorDef0 = mock( OperatorDef.class );
        final OperatorDef operatorDef1 = mock( OperatorDef.class );
        final OperatorDef operatorDef2 = mock( OperatorDef.class );
        when( operator0.getOperatorDef() ).thenReturn( operatorDef0 );
        when( operatorDef0.inputPortCount() ).thenReturn( 1 );
        when( operatorDef0.id() ).thenReturn( "op0" );
        when( operator1.getOperatorDef() ).thenReturn( operatorDef1 );
        when( operatorDef1.inputPortCount() ).thenReturn( 1 );
        when( operatorDef1.id() ).thenReturn( "op1" );
        when( operator2.getOperatorDef() ).thenReturn( operatorDef2 );
        when( operatorDef2.inputPortCount() ).thenReturn( 1 );
        when( operatorDef2.id() ).thenReturn( "op2" );

        final PipelineReplicaId pipelineReplicaId = new PipelineReplicaId( new PipelineId( 0, 0 ), 0 );
        final PipelineReplicaMeter pipelineReplicaMeter = new PipelineReplicaMeter( config.getMetricManagerConfig().getTickMask(),
                                                                                    pipelineReplicaId,
                                                                                    operatorDef0,
                                                                                    operatorDef2 );
        pipeline = new PipelineReplica( config,
                                        pipelineReplicaId,
                                        new OperatorReplica[] { operator0, operator1, operator2 },
                                        pipelineTupleQueue,
                                        pipelineReplicaMeter );

        final Tuple input1 = new Tuple();
        input1.set( "k1", "v1" );
        upstreamInput1.add( input1 );
        final Tuple input2 = new Tuple();
        input2.set( "k2", "v2" );
        upstreamInput2.add( input2 );
        final Tuple output1 = new Tuple();
        output1.set( "k3", "v3" );
        output.add( output1 );
    }

    @Test
    public void shouldInitOperatorsSuccessfully ()
    {
        when( operator0.getSelfUpstreamContext() ).thenReturn( upstreamContext1 );
        when( operator1.getSelfUpstreamContext() ).thenReturn( upstreamContext2 );

        pipeline.init( upstreamContext0 );

        assertThat( pipeline.getStatus(), equalTo( RUNNING ) );

        verify( operator0 ).init( upstreamContext0 );
        verify( operator1 ).init( upstreamContext1 );
        verify( operator2 ).init( upstreamContext2 );
    }

    @Test
    public void shouldShutdownInitializedOperatorsWhenAnOperatorFailsToInit ()
    {
        when( operator0.getSelfUpstreamContext() ).thenReturn( upstreamContext1 );
        doThrow( new InitializationException( "" ) ).when( operator1 ).init( upstreamContext1 );
        when( operator2.getStatus() ).thenReturn( INITIAL );
        try
        {
            pipeline.init( upstreamContext0 );
            fail();
        }
        catch ( InitializationException expected )
        {
            assertThat( pipeline.getStatus(), equalTo( SHUT_DOWN ) );
        }

        verify( operator0 ).init( upstreamContext0 );
        verify( operator0 ).shutdown();
        verify( operator1 ).init( upstreamContext1 );
        verify( operator1 ).shutdown();
        verify( operator2, never() ).init( anyObject() );
        verify( operator2, never() ).shutdown();
    }

    @Test
    public void shouldShutdownOperators ()
    {
        when( operator0.getSelfUpstreamContext() ).thenReturn( upstreamContext1 );
        when( operator1.getSelfUpstreamContext() ).thenReturn( upstreamContext2 );
        pipeline.init( upstreamContext0 );
        pipeline.shutdown();

        assertThat( pipeline.getStatus(), equalTo( SHUT_DOWN ) );

        verify( operator0 ).shutdown();
        verify( operator1 ).shutdown();
        verify( operator2 ).shutdown();
    }

    @Test
    public void shouldShutdownOperatorsOnlyOnce ()
    {
        when( operator0.getSelfUpstreamContext() ).thenReturn( upstreamContext1 );
        when( operator1.getSelfUpstreamContext() ).thenReturn( upstreamContext2 );
        pipeline.init( upstreamContext0 );
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
        pipeline.init( upstreamContext0 );

        final UpstreamContext upstreamContext0New = new UpstreamContext( 1, new UpstreamConnectionStatus[] { CLOSED } );
        pipeline.setPipelineUpstreamContext( upstreamContext0New );
        pipeline.invoke();

        verify( operator0 ).invoke( true, null, upstreamContext0New );
    }

    @Test
    public void shouldInvokeOperators ()
    {
        when( operator0.getSelfUpstreamContext() ).thenReturn( upstreamContext1 );
        when( operator1.getSelfUpstreamContext() ).thenReturn( upstreamContext2 );
        pipeline.init( upstreamContext0 );

        when( operator0.invoke( true, null, upstreamContext0 ) ).thenReturn( upstreamInput1 );
        when( operator1.invoke( true, upstreamInput1, upstreamContext1 ) ).thenReturn( upstreamInput2 );
        when( operator2.invoke( true, upstreamInput2, upstreamContext2 ) ).thenReturn( output );

        final TuplesImpl result = pipeline.invoke();

        assertThat( result, equalTo( output ) );

        verify( operator0 ).invoke( true, null, upstreamContext0 );
        verify( operator1 ).invoke( true, upstreamInput1, upstreamContext1 );
        verify( operator2 ).invoke( true, upstreamInput2, upstreamContext2 );
    }

}
