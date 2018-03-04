package cs.bilkent.joker.engine.pipeline;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import cs.bilkent.joker.engine.exception.InitializationException;
import cs.bilkent.joker.engine.metric.PipelineReplicaMeter;
import static cs.bilkent.joker.engine.pipeline.OperatorReplicaStatus.INITIAL;
import static cs.bilkent.joker.engine.pipeline.OperatorReplicaStatus.RUNNING;
import static cs.bilkent.joker.engine.pipeline.OperatorReplicaStatus.SHUT_DOWN;
import static cs.bilkent.joker.engine.pipeline.UpstreamContext.newInitialUpstreamContextWithAllPortsConnected;
import cs.bilkent.joker.engine.tuplequeue.OperatorQueue;
import cs.bilkent.joker.operator.OperatorDef;
import cs.bilkent.joker.operator.Tuple;
import cs.bilkent.joker.operator.impl.TuplesImpl;
import static cs.bilkent.joker.operator.scheduling.ScheduleWhenTuplesAvailable.scheduleWhenTuplesAvailableOnDefaultPort;
import cs.bilkent.joker.operator.scheduling.SchedulingStrategy;
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
    private OperatorQueue pipelineQueue;

    @Mock
    private OperatorReplica operator0;

    @Mock
    private OperatorReplica operator1;

    @Mock
    private OperatorReplica operator2;

    private PipelineReplica pipeline;

    private final TuplesImpl upstreamInput1 = new TuplesImpl( 1 );

    private final TuplesImpl upstreamInput2 = new TuplesImpl( 1 );

    private final TuplesImpl output = new TuplesImpl( 1 );

    private final UpstreamContext upstreamContext0 = newInitialUpstreamContextWithAllPortsConnected( 1 );

    private final UpstreamContext upstreamContext1 = newInitialUpstreamContextWithAllPortsConnected( 1 );

    private final UpstreamContext upstreamContext2 = newInitialUpstreamContextWithAllPortsConnected( 1 );

    private final UpstreamContext[][] upstreamContexts = new UpstreamContext[ 3 ][ 1 ];

    private final SchedulingStrategy schedulingStrategy1 = scheduleWhenTuplesAvailableOnDefaultPort( 1 );

    private final SchedulingStrategy schedulingStrategy2 = scheduleWhenTuplesAvailableOnDefaultPort( 2 );

    private final SchedulingStrategy schedulingStrategy3 = scheduleWhenTuplesAvailableOnDefaultPort( 3 );

    private final SchedulingStrategy[][] schedulingStrategies = new SchedulingStrategy[ 3 ][ 1 ];

    @Before
    public void before ()
    {
        upstreamContexts[ 0 ][ 0 ] = upstreamContext0;
        upstreamContexts[ 1 ][ 0 ] = upstreamContext1;
        upstreamContexts[ 2 ][ 0 ] = upstreamContext2;

        schedulingStrategies[ 0 ][ 0 ] = schedulingStrategy1;
        schedulingStrategies[ 1 ][ 0 ] = schedulingStrategy2;
        schedulingStrategies[ 2 ][ 0 ] = schedulingStrategy3;

        final OperatorDef operatorDef0 = mock( OperatorDef.class );
        final OperatorDef operatorDef1 = mock( OperatorDef.class );
        final OperatorDef operatorDef2 = mock( OperatorDef.class );

        when( operator0.getOperatorDef( 0 ) ).thenReturn( operatorDef0 );
        when( operatorDef0.getInputPortCount() ).thenReturn( 1 );
        when( operatorDef0.getId() ).thenReturn( "op0" );
        when( operator1.getOperatorDef( 0 ) ).thenReturn( operatorDef1 );
        when( operatorDef1.getInputPortCount() ).thenReturn( 1 );
        when( operatorDef1.getId() ).thenReturn( "op1" );
        when( operator2.getOperatorDef( 0 ) ).thenReturn( operatorDef2 );
        when( operatorDef2.getInputPortCount() ).thenReturn( 1 );
        when( operatorDef2.getId() ).thenReturn( "op2" );

        final PipelineReplicaId pipelineReplicaId = new PipelineReplicaId( 0, 0, 0 );
        final PipelineReplicaMeter pipelineReplicaMeter = new PipelineReplicaMeter( 1, pipelineReplicaId, operatorDef0 );

        pipeline = new PipelineReplica( pipelineReplicaId, new OperatorReplica[] { operator0, operator1, operator2 }, pipelineQueue,
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
        when( operator0.init( new UpstreamContext[] { upstreamContext0 }, upstreamContext1 ) ).thenReturn( new SchedulingStrategy[] {
                schedulingStrategy1 } );
        when( operator1.init( new UpstreamContext[] { upstreamContext1 }, upstreamContext2 ) ).thenReturn( new SchedulingStrategy[] {
                schedulingStrategy2 } );
        when( operator2.init( new UpstreamContext[] { upstreamContext2 }, null ) ).thenReturn( new SchedulingStrategy[] {
                schedulingStrategy3 } );

        pipeline.init( schedulingStrategies, upstreamContexts );

        assertThat( pipeline.getStatus(), equalTo( RUNNING ) );
        assertThat( pipeline.getUpstreamContext(), equalTo( upstreamContext0 ) );
    }

    @Test
    public void shouldShutdownInitializedOperatorsWhenAnOperatorFailsToInit ()
    {
        when( operator0.init( new UpstreamContext[] { upstreamContext0 }, upstreamContext1 ) ).thenReturn( new SchedulingStrategy[] {
                schedulingStrategy1 } );
        doThrow( new InitializationException( "" ) ).when( operator1 ).init( new UpstreamContext[] { upstreamContext1 }, upstreamContext2 );
        when( operator2.getStatus() ).thenReturn( INITIAL );
        try
        {
            pipeline.init( schedulingStrategies, upstreamContexts );
            fail();
        }
        catch ( InitializationException expected )
        {
            assertThat( pipeline.getStatus(), equalTo( SHUT_DOWN ) );
        }

        verify( operator0 ).init( new UpstreamContext[] { upstreamContext0 }, upstreamContext1 );
        verify( operator0 ).shutdown();

        verify( operator1 ).init( new UpstreamContext[] { upstreamContext1 }, upstreamContext2 );
        verify( operator1 ).shutdown();

        verify( operator2, never() ).init( anyObject(), anyObject() );
        verify( operator2, never() ).shutdown();
    }

    @Test
    public void shouldShutdownOperators ()
    {
        when( operator0.init( new UpstreamContext[] { upstreamContext0 }, upstreamContext1 ) ).thenReturn( new SchedulingStrategy[] {
                schedulingStrategy1 } );
        when( operator1.init( new UpstreamContext[] { upstreamContext1 }, upstreamContext2 ) ).thenReturn( new SchedulingStrategy[] {
                schedulingStrategy2 } );
        when( operator2.init( new UpstreamContext[] { upstreamContext2 }, null ) ).thenReturn( new SchedulingStrategy[] {
                schedulingStrategy3 } );

        pipeline.init( schedulingStrategies, upstreamContexts );
        pipeline.shutdown();

        assertThat( pipeline.getStatus(), equalTo( SHUT_DOWN ) );

        verify( operator0 ).shutdown();
        verify( operator1 ).shutdown();
        verify( operator2 ).shutdown();
    }

    @Test
    public void shouldShutdownOperatorsOnlyOnce ()
    {
        when( operator0.init( new UpstreamContext[] { upstreamContext0 }, upstreamContext1 ) ).thenReturn( new SchedulingStrategy[] {
                schedulingStrategy1 } );
        when( operator1.init( new UpstreamContext[] { upstreamContext1 }, upstreamContext2 ) ).thenReturn( new SchedulingStrategy[] {
                schedulingStrategy2 } );
        when( operator2.init( new UpstreamContext[] { upstreamContext2 }, null ) ).thenReturn( new SchedulingStrategy[] {
                schedulingStrategy3 } );

        pipeline.init( schedulingStrategies, upstreamContexts );
        pipeline.shutdown();
        pipeline.shutdown();

        verify( operator0, times( 1 ) ).shutdown();
        verify( operator1, times( 1 ) ).shutdown();
        verify( operator2, times( 1 ) ).shutdown();
    }

    @Test
    public void shouldInvokeFirstOperatorWithUpdatedUpstreamContext ()
    {
        when( operator0.init( new UpstreamContext[] { upstreamContext0 }, upstreamContext1 ) ).thenReturn( new SchedulingStrategy[] {
                schedulingStrategy1 } );
        when( operator1.init( new UpstreamContext[] { upstreamContext1 }, upstreamContext2 ) ).thenReturn( new SchedulingStrategy[] {
                schedulingStrategy2 } );
        when( operator2.init( new UpstreamContext[] { upstreamContext2 }, null ) ).thenReturn( new SchedulingStrategy[] {
                schedulingStrategy3 } );

        pipeline.init( schedulingStrategies, upstreamContexts );

        final UpstreamContext upstreamContext0New = upstreamContext0.withConnectionClosed( 0 );
        pipeline.setUpstreamContext( upstreamContext0New );
        pipeline.invoke();

        verify( operator0 ).invoke( true, null, upstreamContext0New );
    }

    @Test
    public void shouldInvokeOperators ()
    {
        when( operator0.getDownstreamContext() ).thenReturn( upstreamContext1 );
        when( operator1.getDownstreamContext() ).thenReturn( upstreamContext2 );

        when( operator0.init( new UpstreamContext[] { upstreamContext0 }, upstreamContext1 ) ).thenReturn( new SchedulingStrategy[] {
                schedulingStrategy1 } );
        when( operator1.init( new UpstreamContext[] { upstreamContext1 }, upstreamContext2 ) ).thenReturn( new SchedulingStrategy[] {
                schedulingStrategy2 } );
        when( operator2.init( new UpstreamContext[] { upstreamContext2 }, null ) ).thenReturn( new SchedulingStrategy[] {
                schedulingStrategy3 } );

        pipeline.init( schedulingStrategies, upstreamContexts );

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
