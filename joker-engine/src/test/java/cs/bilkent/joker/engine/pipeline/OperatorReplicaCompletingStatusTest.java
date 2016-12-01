package cs.bilkent.joker.engine.pipeline;

import java.util.Arrays;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import static cs.bilkent.joker.engine.pipeline.OperatorReplicaStatus.COMPLETED;
import static cs.bilkent.joker.engine.pipeline.OperatorReplicaStatus.COMPLETING;
import static cs.bilkent.joker.engine.pipeline.UpstreamConnectionStatus.ACTIVE;
import static cs.bilkent.joker.engine.pipeline.UpstreamConnectionStatus.CLOSED;
import static cs.bilkent.joker.operator.InvocationContext.InvocationReason.INPUT_PORT_CLOSED;
import cs.bilkent.joker.operator.Tuple;
import cs.bilkent.joker.operator.impl.TuplesImpl;
import cs.bilkent.joker.operator.scheduling.ScheduleNever;
import cs.bilkent.joker.operator.scheduling.ScheduleWhenAvailable;
import static cs.bilkent.joker.operator.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByCount.AT_LEAST;
import static cs.bilkent.joker.operator.scheduling.ScheduleWhenTuplesAvailable.scheduleWhenTuplesAvailableOnAll;
import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith( MockitoJUnitRunner.class )
public class OperatorReplicaCompletingStatusTest extends AbstractOperatorReplicaInvocationTest
{

    @Test
    public void test_satisfied_sameUpstreamContext ()
    {
        final int inputPortCount = 2, outputPortCount = 1;
        testSatisfiedSchedulingStrategy( inputPortCount, outputPortCount, false );
    }

    @Test
    public void test_satisfied_newUpstreamContext ()
    {
        final int inputPortCount = 2, outputPortCount = 1;
        testSatisfiedSchedulingStrategy( inputPortCount, outputPortCount, true );
    }

    private void testSatisfiedSchedulingStrategy ( final int inputPortCount, final int outputPortCount, final boolean newUpstreamContext )
    {
        initializeOperatorReplica( inputPortCount, outputPortCount );
        final UpstreamContext upstreamContext = operatorReplica.getUpstreamContext();
        final UpstreamContext invocationUpstreamContext = newUpstreamContext
                                                          ? upstreamContext.withClosedUpstreamConnection( 1 )
                                                          : upstreamContext;

        final TuplesImpl operatorInput = new TuplesImpl( inputPortCount );
        final Tuple t1 = new Tuple();
        t1.set( "f1", "val1" );
        operatorInput.add( t1 );
        when( drainer.getResult() ).thenReturn( operatorInput );

        final TuplesImpl expectedOutput = new TuplesImpl( outputPortCount );
        final Tuple t2 = new Tuple();
        t2.set( "f1", "val3" );
        expectedOutput.add( t2 );
        when( outputSupplier.get() ).thenReturn( expectedOutput );

        final TuplesImpl upstreamInput = new TuplesImpl( inputPortCount );
        final Tuple t3 = new Tuple();
        t3.set( "f1", "val1" );
        upstreamInput.add( t3 );

        final TuplesImpl output = operatorReplica.invoke( upstreamInput, invocationUpstreamContext );

        final Tuple expected = new Tuple();
        expected.set( "f1", "val1" );
        verify( queue ).offer( 0, singletonList( expected ) );
        verify( queue ).drain( false, drainer );
        assertOperatorInvocation();

        assertThat( invocationContext.getReason(), equalTo( INPUT_PORT_CLOSED ) );
        assertThat( invocationContext.getKVStore(), equalTo( kvStore ) );
        assertThat( invocationContext.getInput(), equalTo( operatorInput ) );

        assertThat( output, equalTo( expectedOutput ) );
        assertThat( operatorReplica.getSchedulingStrategy(), equalTo( ScheduleWhenAvailable.INSTANCE ) );
        assertThat( operatorReplica.getUpstreamContext(), equalTo( upstreamContext ) );
        assertThat( operatorReplica.getStatus(), equalTo( COMPLETING ) );
    }

    @Test
    public void test_notSatisfied_sameUpstreamContext ()
    {
        final int inputPortCount = 2, outputPortCount = 1;
        initializeOperatorReplica( inputPortCount, outputPortCount );

        final UpstreamContext upstreamContext = operatorReplica.getUpstreamContext();
        when( drainer.getResult() ).thenReturn( new TuplesImpl( inputPortCount ) );

        final TuplesImpl upstreamInput = new TuplesImpl( inputPortCount );
        final Tuple t1 = new Tuple();
        t1.set( "f1", "val1" );
        upstreamInput.add( t1 );
        final TuplesImpl output = operatorReplica.invoke( upstreamInput, upstreamContext );

        final Tuple expected = new Tuple();
        expected.set( "f1", "val1" );
        verify( queue ).offer( 0, singletonList( expected ) );
        verify( queue ).drain( false, drainer );
        assertNoOperatorInvocation();
        verify( drainerPool, never() ).release( drainer );
        verify( drainer ).reset();

        assertNull( output );
        assertThat( operatorReplica.getSchedulingStrategy(), equalTo( ScheduleWhenAvailable.INSTANCE ) );
        assertThat( operatorReplica.getUpstreamContext(), equalTo( upstreamContext ) );
        assertThat( operatorReplica.getStatus(), equalTo( COMPLETING ) );
    }

    @Test
    public void test_notSatisfied_newUpstreamContext ()
    {
        final int inputPortCount = 3, outputPortCount = 1;
        initializeOperatorReplica( inputPortCount, outputPortCount );

        final UpstreamContext invocationUpstreamContext = operatorReplica.getUpstreamContext().withClosedUpstreamConnection( 1 );

        when( drainer.getResult() ).thenReturn( new TuplesImpl( inputPortCount ) );

        final TuplesImpl upstreamInput = new TuplesImpl( inputPortCount );
        final Tuple t1 = new Tuple();
        t1.set( "f1", "val1" );
        upstreamInput.add( t1 );
        final TuplesImpl output = operatorReplica.invoke( upstreamInput, invocationUpstreamContext );

        final Tuple expected = new Tuple();
        expected.set( "f1", "val1" );
        verify( queue ).offer( 0, singletonList( expected ) );
        verify( queue ).drain( false, drainer );
        verify( operatorKvStore ).getKVStore( null );
        verify( operator ).invoke( invocationContext );
        verify( drainer ).reset();
        verify( drainerPool, never() ).release( drainer );

        assertThat( invocationContext.getReason(), equalTo( INPUT_PORT_CLOSED ) );
        assertThat( invocationContext.getKVStore(), equalTo( null ) );
        assertThat( invocationContext.getInput(), equalTo( new TuplesImpl( inputPortCount ) ) );

        assertNull( output );
        assertThat( operatorReplica.getSchedulingStrategy(), equalTo( ScheduleWhenAvailable.INSTANCE ) );
        assertThat( operatorReplica.getUpstreamContext(), equalTo( invocationUpstreamContext ) );
        assertThat( operatorReplica.getStatus(), equalTo( COMPLETING ) );
    }

    @Test
    public void test_notSatisfied_newUpstreamContext_operatorCompletes ()
    {
        final int inputPortCount = 2, outputPortCount = 1;

        initializeOperatorReplica( inputPortCount, outputPortCount );

        final UpstreamContext invocationUpstreamContext = operatorReplica.getUpstreamContext().withClosedUpstreamConnection( 1 );

        final TuplesImpl input = new TuplesImpl( inputPortCount );
        when( drainer.getResult() ).thenReturn( input );

        final TuplesImpl expectedOutput = new TuplesImpl( outputPortCount );
        final Tuple t1 = new Tuple();
        t1.set( "f1", "val3" );
        expectedOutput.add( t1 );
        when( outputSupplier.get() ).thenReturn( expectedOutput );

        final TuplesImpl upstreamInput = new TuplesImpl( inputPortCount );
        final Tuple t2 = new Tuple();
        t2.set( "f1", "val1" );
        upstreamInput.add( t2 );
        final TuplesImpl output = operatorReplica.invoke( upstreamInput, invocationUpstreamContext );

        final Tuple expected = new Tuple();
        expected.set( "f1", "val1" );
        verify( queue ).offer( 0, singletonList( expected ) );
        verify( queue ).drain( false, drainer );
        verify( operatorKvStore ).getKVStore( null );
        verify( operator ).invoke( invocationContext );
        verify( drainer ).reset();
        verify( drainerPool ).release( drainer );

        assertThat( invocationContext.getReason(), equalTo( INPUT_PORT_CLOSED ) );
        assertThat( invocationContext.getKVStore(), equalTo( null ) );
        assertThat( invocationContext.getInput(), equalTo( new TuplesImpl( inputPortCount ) ) );

        assertThat( output, equalTo( expectedOutput ) );
        assertThat( operatorReplica.getSchedulingStrategy(), equalTo( ScheduleNever.INSTANCE ) );
        assertThat( operatorReplica.getUpstreamContext(), equalTo( invocationUpstreamContext ) );
        assertThat( operatorReplica.getStatus(), equalTo( COMPLETED ) );
    }

    private void initializeOperatorReplica ( final int inputPortCount, final int outputPortCount )
    {
        final int[] counts = new int[ inputPortCount ];
        for ( int i = 0; i < inputPortCount; i++ )
        {
            counts[ i ] = i;
        }
        super.initializeOperatorReplica( inputPortCount,
                                         outputPortCount,
                                         scheduleWhenTuplesAvailableOnAll( AT_LEAST, inputPortCount, 1, counts ) );

        final UpstreamConnectionStatus[] statuses = new UpstreamConnectionStatus[ inputPortCount ];
        Arrays.fill( statuses, ACTIVE );
        statuses[ 0 ] = CLOSED;
        operatorReplica.invoke( null, new UpstreamContext( 1, statuses ) );
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
