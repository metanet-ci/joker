package cs.bilkent.joker.engine.pipeline;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import static cs.bilkent.joker.engine.pipeline.OperatorReplicaStatus.COMPLETED;
import static cs.bilkent.joker.engine.pipeline.OperatorReplicaStatus.COMPLETING;
import static cs.bilkent.joker.engine.pipeline.OperatorReplicaStatus.RUNNING;
import static cs.bilkent.joker.engine.pipeline.UpstreamContext.newInitialUpstreamContextWithAllPortsConnected;
import static cs.bilkent.joker.engine.pipeline.UpstreamContext.newSourceOperatorShutdownUpstreamContext;
import static cs.bilkent.joker.operator.InvocationContext.InvocationReason.INPUT_PORT_CLOSED;
import static cs.bilkent.joker.operator.InvocationContext.InvocationReason.SHUTDOWN;
import static cs.bilkent.joker.operator.InvocationContext.InvocationReason.SUCCESS;
import cs.bilkent.joker.operator.Tuple;
import cs.bilkent.joker.operator.impl.TuplesImpl;
import cs.bilkent.joker.operator.scheduling.ScheduleWhenAvailable;
import static cs.bilkent.joker.operator.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByCount.AT_LEAST;
import static cs.bilkent.joker.operator.scheduling.ScheduleWhenTuplesAvailable.scheduleWhenTuplesAvailableOnAll;
import static cs.bilkent.joker.operator.scheduling.ScheduleWhenTuplesAvailable.scheduleWhenTuplesAvailableOnAny;
import static cs.bilkent.joker.operator.scheduling.ScheduleWhenTuplesAvailable.scheduleWhenTuplesAvailableOnDefaultPort;
import cs.bilkent.joker.operator.scheduling.SchedulingStrategy;
import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyList;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

@RunWith( MockitoJUnitRunner.class )
public class OperatorReplicaRunningStatusTest extends AbstractOperatorReplicaInvocationTest
{

    @Test
    public void test_ScheduleWhenTuplesAvailable_satisfied_sameUpstreamContext ()
    {
        final int inputPortCount = 1;
        final SchedulingStrategy initializationStrategy = scheduleWhenTuplesAvailableOnDefaultPort( 1 );
        testSatisfiedSchedulingStrategy( inputPortCount,
                                         initializationStrategy,
                                         newInitialUpstreamContextWithAllPortsConnected( inputPortCount ) );
    }

    @Test
    public void test_ScheduleWhenTuplesAvailable_satisfied_newUpstreamContextWithInputPortClosed ()
    {
        final int inputPortCount = 1;
        final SchedulingStrategy initializationStrategy = scheduleWhenTuplesAvailableOnDefaultPort( 1 );
        final UpstreamContext newUpstreamContext = newInitialUpstreamContextWithAllPortsConnected( inputPortCount )
                                                           .withAllUpstreamConnectionsClosed();
        testSatisfiedSchedulingStrategy( inputPortCount, initializationStrategy, newUpstreamContext );
    }

    @Test
    public void test_ScheduleWhenAvailable_sameUpstreamContext ()
    {
        final int inputPortCount = 0;
        final SchedulingStrategy initializationStrategy = ScheduleWhenAvailable.INSTANCE;
        testSatisfiedSchedulingStrategy( inputPortCount,
                                         initializationStrategy,
                                         newInitialUpstreamContextWithAllPortsConnected( inputPortCount ) );
    }

    private void testSatisfiedSchedulingStrategy ( final int inputPortCount,
                                                   final SchedulingStrategy initializationStrategy,
                                                   final UpstreamContext upstreamContext )
    {
        initializeOperatorReplica( inputPortCount, initializationStrategy );

        doAnswer( invocation -> {
            invocationContext.createInputTuples( key );
            return null;
        } ).when( queue ).drain( eq( false ), eq( drainer ), anyObject() );

        final TuplesImpl expectedOutput = new TuplesImpl( outputPortCount );
        final Tuple tuple = new Tuple();
        tuple.set( "f1", "val3" );
        expectedOutput.add( tuple );

        final TuplesImpl upstreamInput = new TuplesImpl( inputPortCount );
        if ( inputPortCount > 0 )
        {
            final Tuple t = new Tuple();
            t.set( "f1", "val1" );
            upstreamInput.add( t );
        }

        doAnswer( invocation -> {
            assertThat( invocationContext.getReason(), equalTo( SUCCESS ) );
            assertThat( invocationContext.getKVStore(), equalTo( kvStore ) );
            assertNotNull( invocationContext.getInput() );
            invocationContext.output( tuple );
            return null;
        } ).when( operator ).invoke( invocationContext );

        final TuplesImpl output = operatorReplica.invoke( upstreamInput, upstreamContext );

        if ( inputPortCount > 0 )
        {
            final Tuple expected = new Tuple();
            expected.set( "f1", "val1" );
            verify( queue ).offer( 0, singletonList( expected ) );
        }
        else
        {
            verify( queue, never() ).offer( anyInt(), anyList() );
        }

        verify( queue ).drain( eq( false ), eq( drainer ), anyObject() );
        assertOperatorInvocation();

        assertThat( output, equalTo( expectedOutput ) );
        assertThat( operatorReplica.getSchedulingStrategy(), equalTo( initializationStrategy ) );
        assertNull( operatorReplica.getCompletionReason() );
        assertThat( operatorReplica.getUpstreamContext(), equalTo( initializationUpstreamContext ) );
        assertThat( operatorReplica.getStatus(), equalTo( RUNNING ) );
    }

    @Test
    public void test_ScheduleWhenAvailable_newUpstreamContext ()
    {
        final int inputPortCount = 0;
        final SchedulingStrategy initializationStrategy = ScheduleWhenAvailable.INSTANCE;
        final UpstreamContext newUpstreamContext = newSourceOperatorShutdownUpstreamContext();

        initializeOperatorReplica( inputPortCount, initializationStrategy );

        doAnswer( invocation -> {
            invocationContext.createInputTuples( key );
            return null;
        } ).when( queue ).drain( eq( false ), eq( drainer ), anyObject() );

        final TuplesImpl expectedOutput = new TuplesImpl( outputPortCount );
        final Tuple tuple = new Tuple();
        tuple.set( "f1", "val3" );
        expectedOutput.add( tuple );

        doAnswer( invocation -> {
            assertThat( invocationContext.getReason(), equalTo( SHUTDOWN ) );
            assertThat( invocationContext.getKVStore(), equalTo( kvStore ) );
            assertNotNull( invocationContext.getInput() );
            invocationContext.output( tuple );
            return null;
        } ).when( operator ).invoke( invocationContext );

        final TuplesImpl upstreamInput = new TuplesImpl( inputPortCount );
        final TuplesImpl output = operatorReplica.invoke( upstreamInput, newUpstreamContext );

        verify( queue, never() ).offer( anyInt(), anyList() );

        verify( queue ).drain( eq( false ), eq( drainer ), anyObject() );
        assertOperatorInvocation();

        assertThat( output, equalTo( expectedOutput ) );
        assertThat( operatorReplica.getCompletionReason(), equalTo( SHUTDOWN ) );
        assertThat( operatorReplica.getUpstreamContext(), equalTo( newUpstreamContext ) );
        assertThat( operatorReplica.getStatus(), equalTo( COMPLETED ) );
    }

    @Test
    public void test_ScheduleWhenTuplesAvailable_notSatisfied_sameUpstreamContext ()
    {
        final int inputPortCount = 1;
        final SchedulingStrategy initializationStrategy = scheduleWhenTuplesAvailableOnDefaultPort( 1 );
        initializeOperatorReplica( inputPortCount, initializationStrategy );

        final TuplesImpl upstreamInput = new TuplesImpl( inputPortCount );
        final Tuple tuple = new Tuple();
        tuple.set( "f1", "val1" );
        upstreamInput.add( tuple );
        final UpstreamContext upstreamContext = newInitialUpstreamContextWithAllPortsConnected( inputPortCount );
        final TuplesImpl output = operatorReplica.invoke( upstreamInput, upstreamContext );

        final Tuple expected = new Tuple();
        expected.set( "f1", "val1" );
        verify( queue ).offer( 0, singletonList( expected ) );

        verify( queue ).drain( eq( false ), eq( drainer ), anyObject() );
        assertNoOperatorInvocation();
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
        final int inputPortCount = 2;
        final SchedulingStrategy initializationStrategy = scheduleWhenTuplesAvailableOnAny( AT_LEAST, inputPortCount, 1, 0, 1 );
        initializeOperatorReplica( inputPortCount, initializationStrategy );
        final UpstreamContext newUpstreamContext = newInitialUpstreamContextWithAllPortsConnected( inputPortCount )
                                                           .withUpstreamConnectionClosed(
                1 );

        final TuplesImpl upstreamInput = new TuplesImpl( inputPortCount );
        final Tuple t2 = new Tuple();
        t2.set( "f1", "val1" );
        upstreamInput.add( t2 );

        final TuplesImpl output = operatorReplica.invoke( upstreamInput, newUpstreamContext );

        final Tuple expected = new Tuple();
        expected.set( "f1", "val1" );
        verify( queue ).offer( 0, singletonList( expected ) );

        verify( queue ).drain( eq( false ), eq( drainer ), anyObject() );
        assertNoOperatorInvocation();
        verify( drainerPool ).acquire( initializationStrategy );

        assertNull( output );
        assertNull( operatorReplica.getCompletionReason() );
        assertThat( operatorReplica.getUpstreamContext(), equalTo( newUpstreamContext ) );
        assertThat( operatorReplica.getStatus(), equalTo( RUNNING ) );
    }

    @Test
    public void test_ScheduleWhenTuplesAvailable_notSatisfied_newUpstreamContext_operatorMovesToCompleting ()
    {
        final int inputPortCount = 2;
        final SchedulingStrategy initializationStrategy = scheduleWhenTuplesAvailableOnAll( AT_LEAST, inputPortCount, 1, 0, 1 );
        initializeOperatorReplica( inputPortCount, initializationStrategy );
        final UpstreamContext newUpstreamContext = newInitialUpstreamContextWithAllPortsConnected( inputPortCount )
                                                           .withUpstreamConnectionClosed(
                0 );

        final Tuple t1 = new Tuple();
        t1.set( "f1", "val2" );

        final int[] drainCount = new int[ 1 ];
        doAnswer( invocation -> {
            if ( drainCount[ 0 ] == 1 )
            {
                final TuplesImpl tuples = invocationContext.createInputTuples( key );
                tuples.add( t1 );
            }
            drainCount[ 0 ]++;
            return null;
        } ).when( queue ).drain( eq( false ), anyObject(), anyObject() );

        final TuplesImpl upstreamInput = new TuplesImpl( inputPortCount );
        final Tuple t2 = new Tuple();
        t2.set( "f1", "val1" );
        upstreamInput.add( t2 );

        final Tuple t3 = new Tuple();
        t3.set( "f3", "val3" );

        doAnswer( invocation -> {
            assertThat( invocationContext.getReason(), equalTo( INPUT_PORT_CLOSED ) );
            assertThat( invocationContext.getKVStore(), equalTo( kvStore ) );
            assertThat( invocationContext.getInput().getTupleOrFail( 0, 0 ), equalTo( t1 ) );
            invocationContext.output( t3 );
            return null;
        } ).when( operator ).invoke( invocationContext );

        final TuplesImpl output = operatorReplica.invoke( upstreamInput, newUpstreamContext );

        final Tuple expected = new Tuple();
        expected.set( "f1", "val1" );
        verify( queue ).offer( 0, singletonList( expected ) );
        verify( operatorKvStore ).getKVStore( key );
        verify( operator ).invoke( invocationContext );
        verify( drainerPool ).acquire( initializationStrategy );

        assertNotNull( output );
        assertThat( output.getTupleOrFail( 0, 0 ), equalTo( t3 ) );
        assertThat( operatorReplica.getCompletionReason(), equalTo( INPUT_PORT_CLOSED ) );
        assertThat( operatorReplica.getUpstreamContext(), equalTo( newUpstreamContext ) );
        assertThat( operatorReplica.getStatus(), equalTo( COMPLETING ) );
    }

}
