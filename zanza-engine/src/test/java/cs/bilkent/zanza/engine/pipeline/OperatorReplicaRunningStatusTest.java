package cs.bilkent.zanza.engine.pipeline;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import static cs.bilkent.zanza.engine.pipeline.OperatorReplicaInitializationTest.newUpstreamContextInstance;
import static cs.bilkent.zanza.engine.pipeline.OperatorReplicaStatus.COMPLETED;
import static cs.bilkent.zanza.engine.pipeline.OperatorReplicaStatus.COMPLETING;
import static cs.bilkent.zanza.engine.pipeline.OperatorReplicaStatus.RUNNING;
import static cs.bilkent.zanza.engine.pipeline.UpstreamConnectionStatus.ACTIVE;
import static cs.bilkent.zanza.engine.pipeline.UpstreamConnectionStatus.CLOSED;
import static cs.bilkent.zanza.operator.InvocationContext.InvocationReason.INPUT_PORT_CLOSED;
import static cs.bilkent.zanza.operator.InvocationContext.InvocationReason.SHUTDOWN;
import static cs.bilkent.zanza.operator.InvocationContext.InvocationReason.SUCCESS;
import cs.bilkent.zanza.operator.Tuple;
import cs.bilkent.zanza.operator.impl.TuplesImpl;
import cs.bilkent.zanza.operator.scheduling.ScheduleNever;
import cs.bilkent.zanza.operator.scheduling.ScheduleWhenAvailable;
import static cs.bilkent.zanza.operator.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByCount.AT_LEAST;
import static cs.bilkent.zanza.operator.scheduling.ScheduleWhenTuplesAvailable.scheduleWhenTuplesAvailableOnAll;
import static cs.bilkent.zanza.operator.scheduling.ScheduleWhenTuplesAvailable.scheduleWhenTuplesAvailableOnAny;
import static cs.bilkent.zanza.operator.scheduling.ScheduleWhenTuplesAvailable.scheduleWhenTuplesAvailableOnDefaultPort;
import cs.bilkent.zanza.operator.scheduling.SchedulingStrategy;
import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertNull;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyList;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith( MockitoJUnitRunner.class )
public class OperatorReplicaRunningStatusTest extends AbstractOperatorReplicaInvocationTest
{

    @Test
    public void test_ScheduleWhenTuplesAvailable_satisfied_sameUpstreamContext ()
    {
        final int inputPortCount = 1, outputPortCount = 1;
        final SchedulingStrategy initializationStrategy = scheduleWhenTuplesAvailableOnDefaultPort( 1 );
        testSatisfiedSchedulingStrategy( inputPortCount,
                                         outputPortCount,
                                         initializationStrategy,
                                         newUpstreamContextInstance( 0, inputPortCount, ACTIVE ) );
    }

    @Test
    public void test_ScheduleWhenTuplesAvailable_satisfied_newUpstreamContextWithInputPortClosed ()
    {
        final int inputPortCount = 1, outputPortCount = 1;
        final SchedulingStrategy initializationStrategy = scheduleWhenTuplesAvailableOnDefaultPort( 1 );
        final UpstreamContext newUpstreamContext = newUpstreamContextInstance( 1, inputPortCount, CLOSED );
        testSatisfiedSchedulingStrategy( inputPortCount, outputPortCount, initializationStrategy, newUpstreamContext );
    }

    @Test
    public void test_ScheduleWhenAvailable_sameUpstreamContext ()
    {
        final int inputPortCount = 0, outputPortCount = 1;
        final SchedulingStrategy initializationStrategy = ScheduleWhenAvailable.INSTANCE;
        testSatisfiedSchedulingStrategy( inputPortCount,
                                         outputPortCount,
                                         initializationStrategy,
                                         newUpstreamContextInstance( 0, inputPortCount, ACTIVE ) );
    }

    private void testSatisfiedSchedulingStrategy ( final int inputPortCount,
                                                   final int outputPortCount,
                                                   final SchedulingStrategy initializationStrategy,
                                                   final UpstreamContext upstreamContext )
    {
        initializeOperatorReplica( inputPortCount, outputPortCount, initializationStrategy );

        final TuplesImpl operatorInput = new TuplesImpl( inputPortCount );
        when( drainer.getResult() ).thenReturn( operatorInput );

        final TuplesImpl expectedOutput = new TuplesImpl( outputPortCount );
        expectedOutput.add( new Tuple( "f1", "val3" ) );
        when( outputSupplier.get() ).thenReturn( expectedOutput );

        final TuplesImpl upstreamInput = new TuplesImpl( inputPortCount );
        if ( inputPortCount > 0 )
        {
            upstreamInput.add( new Tuple( "f1", "val1" ) );
        }

        final TuplesImpl output = operatorReplica.invoke( upstreamInput, upstreamContext );

        if ( inputPortCount > 0 )
        {
            verify( queue ).offer( 0, singletonList( new Tuple( "f1", "val1" ) ) );
        }
        else
        {
            verify( queue, never() ).offer( anyInt(), anyList() );
        }
        verify( queue ).drain( drainer );
        assertOperatorInvocation();
        verify( drainerPool, never() ).release( drainer );

        assertThat( invocationContext.getReason(), equalTo( SUCCESS ) );
        assertThat( invocationContext.getKVStore(), equalTo( kvStore ) );
        assertThat( invocationContext.getInput(), equalTo( operatorInput ) );

        assertThat( output, equalTo( expectedOutput ) );
        assertThat( operatorReplica.getSchedulingStrategy(), equalTo( initializationStrategy ) );
        assertNull( operatorReplica.getCompletionReason() );
        assertThat( operatorReplica.getUpstreamContext(), equalTo( initializationUpstreamContext ) );
        assertThat( operatorReplica.getStatus(), equalTo( RUNNING ) );
    }

    @Test
    public void test_ScheduleWhenAvailable_newUpstreamContext ()
    {
        final int inputPortCount = 0, outputPortCount = 1;
        final SchedulingStrategy initializationStrategy = ScheduleWhenAvailable.INSTANCE;
        final UpstreamContext newUpstreamContext = newUpstreamContextInstance( 1, inputPortCount, CLOSED );

        initializeOperatorReplica( inputPortCount, outputPortCount, initializationStrategy );

        final TuplesImpl operatorInput = new TuplesImpl( inputPortCount );
        when( drainer.getResult() ).thenReturn( operatorInput );

        final TuplesImpl expectedOutput = new TuplesImpl( outputPortCount );
        expectedOutput.add( new Tuple( "f1", "val3" ) );
        when( outputSupplier.get() ).thenReturn( expectedOutput );

        final TuplesImpl upstreamInput = new TuplesImpl( inputPortCount );
        final TuplesImpl output = operatorReplica.invoke( upstreamInput, newUpstreamContext );

        verify( queue, never() ).offer( anyInt(), anyList() );
        verify( queue ).drain( drainer );
        assertOperatorInvocation();
        verify( drainerPool ).release( drainer );

        assertThat( invocationContext.getReason(), equalTo( SHUTDOWN ) );
        assertThat( invocationContext.getKVStore(), equalTo( kvStore ) );
        assertThat( invocationContext.getInput(), equalTo( operatorInput ) );

        assertThat( output, equalTo( expectedOutput ) );
        assertThat( operatorReplica.getSchedulingStrategy(), equalTo( ScheduleNever.INSTANCE ) );
        assertThat( operatorReplica.getCompletionReason(), equalTo( SHUTDOWN ) );
        assertThat( operatorReplica.getUpstreamContext(), equalTo( newUpstreamContext ) );
        assertThat( operatorReplica.getStatus(), equalTo( COMPLETED ) );
    }

    @Test
    public void test_ScheduleWhenTuplesAvailable_notSatisfied_sameUpstreamContext ()
    {
        final int inputPortCount = 1, outputPortCount = 1;
        final SchedulingStrategy initializationStrategy = scheduleWhenTuplesAvailableOnDefaultPort( 1 );
        initializeOperatorReplica( inputPortCount, outputPortCount, initializationStrategy );

        final TuplesImpl upstreamInput = new TuplesImpl( inputPortCount );
        upstreamInput.add( new Tuple( "f1", "val1" ) );
        final UpstreamContext upstreamContext = newUpstreamContextInstance( 0, inputPortCount, ACTIVE );
        final TuplesImpl output = operatorReplica.invoke( upstreamInput, upstreamContext );

        verify( queue ).offer( 0, singletonList( new Tuple( "f1", "val1" ) ) );
        verify( queue ).drain( drainer );
        verify( drainer ).reset();
        assertNoOperatorInvocation();
        verify( drainerPool, never() ).release( drainer );
        verify( drainerPool ).acquire( initializationStrategy );

        assertNull( output );
        assertThat( operatorReplica.getSchedulingStrategy(), equalTo( initializationStrategy ) );
        assertThat( operatorReplica.getUpstreamContext(), equalTo( upstreamContext ) );
        assertNull( operatorReplica.getCompletionReason() );
        assertThat( operatorReplica.getStatus(), equalTo( RUNNING ) );
    }

    @Test
    public void test_ScheduleWhenTuplesAvailable_notSatisfied_newUpstreamContextWithSingleInputPortClosed ()
    {
        final int inputPortCount = 2, outputPortCount = 1;
        final SchedulingStrategy initializationStrategy = scheduleWhenTuplesAvailableOnAny( 2, 1, 0, 1 );
        initializeOperatorReplica( inputPortCount, outputPortCount, initializationStrategy );
        final UpstreamContext newUpstreamContext = new UpstreamContext( 1, new UpstreamConnectionStatus[] { ACTIVE, CLOSED } );

        final TuplesImpl operatorInput = new TuplesImpl( inputPortCount );
        operatorInput.add( new Tuple( "f1", "val2" ) );
        when( drainer.getResult() ).thenReturn( null, operatorInput );

        final TuplesImpl upstreamInput = new TuplesImpl( inputPortCount );
        upstreamInput.add( new Tuple( "f1", "val1" ) );
        final TuplesImpl output = operatorReplica.invoke( upstreamInput, newUpstreamContext );

        verify( queue ).offer( 0, singletonList( new Tuple( "f1", "val1" ) ) );
        verify( queue ).drain( drainer );
        verify( drainer ).reset();
        assertNoOperatorInvocation();
        verify( drainerPool, never() ).release( drainer );
        verify( drainerPool ).acquire( initializationStrategy );

        assertNull( output );
        assertNull( operatorReplica.getCompletionReason() );
        assertThat( operatorReplica.getUpstreamContext(), equalTo( newUpstreamContext ) );
        assertThat( operatorReplica.getStatus(), equalTo( RUNNING ) );
    }

    @Test
    public void test_ScheduleWhenTuplesAvailable_notSatisfied_newUpstreamContext_operatorMovesToCompleting ()
    {
        final int inputPortCount = 2, outputPortCount = 1;
        final SchedulingStrategy initializationStrategy = scheduleWhenTuplesAvailableOnAll( AT_LEAST, 2, 1, 0, 1 );
        initializeOperatorReplica( inputPortCount, outputPortCount, initializationStrategy );
        final UpstreamContext newUpstreamContext = new UpstreamContext( 1, new UpstreamConnectionStatus[] { ACTIVE, CLOSED } );

        final TuplesImpl operatorInput = new TuplesImpl( inputPortCount );
        operatorInput.add( new Tuple( "f1", "val2" ) );
        when( drainer.getResult() ).thenReturn( null, operatorInput );

        final TuplesImpl upstreamInput = new TuplesImpl( inputPortCount );
        upstreamInput.add( new Tuple( "f1", "val1" ) );
        final TuplesImpl output = operatorReplica.invoke( upstreamInput, newUpstreamContext );

        verify( queue ).offer( 0, singletonList( new Tuple( "f1", "val1" ) ) );
        verify( queue, times( 2 ) ).drain( drainer );
        assertOperatorInvocation();
        verify( drainerPool ).release( drainer );
        verify( drainerPool ).acquire( ScheduleWhenAvailable.INSTANCE );

        assertThat( invocationContext.getReason(), equalTo( INPUT_PORT_CLOSED ) );
        assertThat( invocationContext.getKVStore(), equalTo( kvStore ) );

        assertNull( output );
        assertThat( operatorReplica.getCompletionReason(), equalTo( INPUT_PORT_CLOSED ) );
        assertThat( operatorReplica.getUpstreamContext(), equalTo( newUpstreamContext ) );
        assertThat( operatorReplica.getStatus(), equalTo( COMPLETING ) );
    }

}
