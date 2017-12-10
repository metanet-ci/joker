package cs.bilkent.joker.engine.pipeline;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import static cs.bilkent.joker.engine.pipeline.OperatorReplicaStatus.COMPLETED;
import static cs.bilkent.joker.engine.pipeline.OperatorReplicaStatus.COMPLETING;
import static cs.bilkent.joker.engine.pipeline.UpstreamContext.newInitialUpstreamContextWithAllPortsConnected;
import static cs.bilkent.joker.operator.InvocationContext.InvocationReason.INPUT_PORT_CLOSED;
import cs.bilkent.joker.operator.Tuple;
import cs.bilkent.joker.operator.impl.InvocationContextImpl;
import cs.bilkent.joker.operator.impl.TuplesImpl;
import static cs.bilkent.joker.operator.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByCount.AT_LEAST;
import static cs.bilkent.joker.operator.scheduling.ScheduleWhenTuplesAvailable.scheduleWhenTuplesAvailableOnAll;
import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertNull;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith( MockitoJUnitRunner.class )
public class OperatorReplicaCompletingStatusTest extends AbstractOperatorReplicaInvocationTest
{

    @Test
    public void test_satisfied_sameUpstreamContext ()
    {
        final int inputPortCount = 2;
        testSatisfiedSchedulingStrategy( inputPortCount, false );
    }

    @Test
    public void test_satisfied_newUpstreamContext ()
    {
        final int inputPortCount = 2;
        testSatisfiedSchedulingStrategy( inputPortCount, true );
    }

    private void testSatisfiedSchedulingStrategy ( final int inputPortCount, final boolean newUpstreamContext )
    {
        initializeOperatorReplica( inputPortCount );
        final UpstreamContext upstreamContext = operatorReplica.getUpstreamContext();
        final UpstreamContext invocationUpstreamContext = newUpstreamContext
                                                          ? upstreamContext.withUpstreamConnectionClosed( 1 )
                                                          : upstreamContext;

        final Tuple t1 = new Tuple();
        t1.set( "f1", "val1" );
        doAnswer( invocation -> {
            final TuplesImpl tuples = invocationContext.createInputTuples( key );
            tuples.add( t1 );
            return null;
        } ).when( queue ).drain( eq( false ), anyObject(), anyObject() );

        final TuplesImpl expectedOutput = new TuplesImpl( outputPortCount );
        final Tuple t2 = new Tuple();
        t2.set( "f1", "val3" );
        expectedOutput.add( t2 );

        final TuplesImpl upstreamInput = new TuplesImpl( inputPortCount );
        final Tuple t3 = new Tuple();
        t3.set( "f1", "val1" );
        upstreamInput.add( t3 );

        doAnswer( invocation -> {
            final InvocationContextImpl invocationContext = invocation.getArgumentAt( 0, InvocationContextImpl.class );
            assertThat( invocationContext.getReason(), equalTo( INPUT_PORT_CLOSED ) );
            assertThat( invocationContext.getKVStore(), equalTo( kvStore ) );
            assertThat( invocationContext.getInput().getTupleOrFail( 0, 0 ), equalTo( t1 ) );
            invocationContext.output( t2 );
            return null;
        } ).when( operator ).invoke( invocationContext );

        final TuplesImpl output = operatorReplica.invoke( upstreamInput, invocationUpstreamContext );

        final Tuple expected = new Tuple();
        expected.set( "f1", "val1" );
        verify( queue ).offer( 0, singletonList( expected ) );
        assertOperatorInvocation();

        assertThat( output, equalTo( expectedOutput ) );
        assertThat( operatorReplica.getUpstreamContext(), equalTo( upstreamContext ) );
        assertThat( operatorReplica.getStatus(), equalTo( COMPLETING ) );
    }

    @Test
    public void test_notSatisfied_sameUpstreamContext ()
    {
        final int inputPortCount = 2;
        initializeOperatorReplica( inputPortCount );

        final UpstreamContext upstreamContext = operatorReplica.getUpstreamContext();

        final TuplesImpl upstreamInput = new TuplesImpl( inputPortCount );
        final Tuple t1 = new Tuple();
        t1.set( "f1", "val1" );
        upstreamInput.add( t1 );
        final TuplesImpl output = operatorReplica.invoke( upstreamInput, upstreamContext );

        final Tuple expected = new Tuple();
        expected.set( "f1", "val1" );
        verify( queue ).offer( 0, singletonList( expected ) );
        verify( queue ).drain( eq( false ), anyObject(), anyObject() );
        assertNoOperatorInvocation();

        assertNull( output );
        assertThat( operatorReplica.getUpstreamContext(), equalTo( upstreamContext ) );
        assertThat( operatorReplica.getStatus(), equalTo( COMPLETING ) );
    }

    @Test
    public void test_notSatisfied_newUpstreamContext ()
    {
        final int inputPortCount = 3;
        initializeOperatorReplica( inputPortCount );

        final UpstreamContext invocationUpstreamContext = operatorReplica.getUpstreamContext().withUpstreamConnectionClosed( 1 );

        final TuplesImpl upstreamInput = new TuplesImpl( inputPortCount );
        final Tuple t1 = new Tuple();
        t1.set( "f1", "val1" );
        upstreamInput.add( t1 );

        doAnswer( invocation -> {
            assertThat( invocationContext.getReason(), equalTo( INPUT_PORT_CLOSED ) );
            assertThat( invocationContext.getKVStore(), equalTo( null ) );
            assertThat( invocationContext.getInput(), equalTo( new TuplesImpl( inputPortCount ) ) );
            return null;
        } ).when( operator ).invoke( invocationContext );

        final TuplesImpl output = operatorReplica.invoke( upstreamInput, invocationUpstreamContext );

        final Tuple expected = new Tuple();
        expected.set( "f1", "val1" );
        verify( queue ).offer( 0, singletonList( expected ) );
        verify( queue ).drain( eq( false ), anyObject(), anyObject() );
        verify( operatorKvStore ).getKVStore( null );
        verify( operator ).invoke( invocationContext );


        assertNull( output );
        assertThat( operatorReplica.getUpstreamContext(), equalTo( invocationUpstreamContext ) );
        assertThat( operatorReplica.getStatus(), equalTo( COMPLETING ) );
    }

    @Test
    public void test_notSatisfied_newUpstreamContext_operatorCompletes ()
    {
        final int inputPortCount = 2;

        initializeOperatorReplica( inputPortCount );

        final UpstreamContext invocationUpstreamContext = operatorReplica.getUpstreamContext().withUpstreamConnectionClosed( 1 );

        final TuplesImpl expectedOutput = new TuplesImpl( outputPortCount );
        final Tuple t1 = new Tuple();
        t1.set( "f1", "val3" );
        expectedOutput.add( t1 );
        when( outputSupplier.get() ).thenReturn( expectedOutput );

        final TuplesImpl upstreamInput = new TuplesImpl( inputPortCount );
        final Tuple t2 = new Tuple();
        t2.set( "f1", "val1" );
        upstreamInput.add( t2 );

        doAnswer( invocation -> {
            assertThat( invocationContext.getReason(), equalTo( INPUT_PORT_CLOSED ) );
            assertThat( invocationContext.getKVStore(), equalTo( null ) );
            assertThat( invocationContext.getInput(), equalTo( new TuplesImpl( inputPortCount ) ) );
            invocationContext.output( t1 );
            return null;
        } ).when( operator ).invoke( invocationContext );

        final TuplesImpl output = operatorReplica.invoke( upstreamInput, invocationUpstreamContext );

        final Tuple expected = new Tuple();
        expected.set( "f1", "val1" );
        verify( queue ).offer( 0, singletonList( expected ) );
        verify( queue ).drain( eq( false ), anyObject(), anyObject() );
        verify( operatorKvStore ).getKVStore( null );
        verify( operator ).invoke( invocationContext );

        assertThat( output, equalTo( expectedOutput ) );
        assertThat( operatorReplica.getUpstreamContext(), equalTo( invocationUpstreamContext ) );
        assertThat( operatorReplica.getStatus(), equalTo( COMPLETED ) );
    }

    private void initializeOperatorReplica ( final int inputPortCount )
    {
        final int[] counts = new int[ inputPortCount ];
        for ( int i = 0; i < inputPortCount; i++ )
        {
            counts[ i ] = i;
        }
        super.initializeOperatorReplica( inputPortCount, scheduleWhenTuplesAvailableOnAll( AT_LEAST, inputPortCount, 1, counts ) );

        operatorReplica.invoke( null, newInitialUpstreamContextWithAllPortsConnected( inputPortCount ).withUpstreamConnectionClosed( 0 ) );
        assertThat( operatorReplica.getStatus(), equalTo( COMPLETING ) );
        assertThat( operatorReplica.getCompletionReason(), equalTo( INPUT_PORT_CLOSED ) );

        reset( queue );
        reset( drainer );
        reset( drainerPool );
        reset( outputSupplier );
        reset( operator );
        reset( operatorKvStore );
        applyDefaultMocks();
    }

}
