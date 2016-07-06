package cs.bilkent.zanza.engine.pipeline;

import java.util.Arrays;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import static cs.bilkent.zanza.engine.pipeline.OperatorReplicaStatus.COMPLETED;
import static cs.bilkent.zanza.engine.pipeline.OperatorReplicaStatus.COMPLETING;
import static cs.bilkent.zanza.engine.pipeline.UpstreamConnectionStatus.ACTIVE;
import static cs.bilkent.zanza.engine.pipeline.UpstreamConnectionStatus.CLOSED;
import static cs.bilkent.zanza.operator.InvocationContext.InvocationReason.INPUT_PORT_CLOSED;
import cs.bilkent.zanza.operator.Tuple;
import cs.bilkent.zanza.operator.impl.TuplesImpl;
import cs.bilkent.zanza.operator.scheduling.ScheduleNever;
import cs.bilkent.zanza.operator.scheduling.ScheduleWhenAvailable;
import static cs.bilkent.zanza.operator.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByCount.AT_LEAST;
import static cs.bilkent.zanza.operator.scheduling.ScheduleWhenTuplesAvailable.scheduleWhenTuplesAvailableOnAll;
import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith( MockitoJUnitRunner.class )
public class OperatorReplicaCompletingStatusTest extends AbstractOperatorInstanceInvocationTest
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
        initializeOperatorInstance( inputPortCount, outputPortCount );
        final UpstreamContext upstreamContext = operatorReplica.getUpstreamContext();
        final UpstreamContext invocationUpstreamContext = newUpstreamContext
                                                          ? upstreamContext.withUpstreamConnectionStatus( 1, CLOSED )
                                                          : upstreamContext;

        final TuplesImpl operatorInput = new TuplesImpl( inputPortCount );
        operatorInput.add( new Tuple( "f1", "val1" ) );
        when( drainer.getResult() ).thenReturn( operatorInput );

        final TuplesImpl expectedOutput = new TuplesImpl( outputPortCount );
        expectedOutput.add( new Tuple( "f1", "val3" ) );
        when( outputSupplier.get() ).thenReturn( expectedOutput );

        final TuplesImpl upstreamInput = new TuplesImpl( inputPortCount );
        upstreamInput.add( new Tuple( "f1", "val1" ) );

        final TuplesImpl output = operatorReplica.invoke( upstreamInput, invocationUpstreamContext );

        verify( queue ).offer( 0, singletonList( new Tuple( "f1", "val1" ) ) );
        verify( queue ).drain( drainer );
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
        initializeOperatorInstance( inputPortCount, outputPortCount );

        final UpstreamContext upstreamContext = operatorReplica.getUpstreamContext();
        when( drainer.getResult() ).thenReturn( new TuplesImpl( inputPortCount ) );

        final TuplesImpl upstreamInput = new TuplesImpl( inputPortCount );
        upstreamInput.add( new Tuple( "f1", "val1" ) );
        final TuplesImpl output = operatorReplica.invoke( upstreamInput, upstreamContext );

        verify( queue ).offer( 0, singletonList( new Tuple( "f1", "val1" ) ) );
        verify( queue ).drain( drainer );
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
        initializeOperatorInstance( inputPortCount, outputPortCount );

        final UpstreamContext invocationUpstreamContext = operatorReplica.getUpstreamContext().withUpstreamConnectionStatus( 1, CLOSED );

        when( drainer.getResult() ).thenReturn( new TuplesImpl( inputPortCount ) );

        final TuplesImpl upstreamInput = new TuplesImpl( inputPortCount );
        upstreamInput.add( new Tuple( "f1", "val1" ) );
        final TuplesImpl output = operatorReplica.invoke( upstreamInput, invocationUpstreamContext );

        verify( queue ).offer( 0, singletonList( new Tuple( "f1", "val1" ) ) );
        verify( queue ).drain( drainer );
        verify( kvStoreContext ).getKVStore( null );
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

        initializeOperatorInstance( inputPortCount, outputPortCount );

        final UpstreamContext invocationUpstreamContext = operatorReplica.getUpstreamContext().withUpstreamConnectionStatus( 1, CLOSED );

        final TuplesImpl input = new TuplesImpl( inputPortCount );
        when( drainer.getResult() ).thenReturn( input );

        final TuplesImpl expectedOutput = new TuplesImpl( outputPortCount );
        expectedOutput.add( new Tuple( "f1", "val3" ) );
        when( outputSupplier.get() ).thenReturn( expectedOutput );

        final TuplesImpl upstreamInput = new TuplesImpl( inputPortCount );
        upstreamInput.add( new Tuple( "f1", "val1" ) );
        final TuplesImpl output = operatorReplica.invoke( upstreamInput, invocationUpstreamContext );

        verify( queue ).offer( 0, singletonList( new Tuple( "f1", "val1" ) ) );
        verify( queue ).drain( drainer );
        verify( kvStoreContext ).getKVStore( null );
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

    private void initializeOperatorInstance ( final int inputPortCount, final int outputPortCount )
    {
        final int[] counts = new int[ inputPortCount ];
        for ( int i = 0; i < inputPortCount; i++ )
        {
            counts[ i ] = i;
        }
        super.initializeOperatorInstance( inputPortCount,
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
        reset( kvStoreContext );
        applyDefaultMocks();
    }

}
