package cs.bilkent.zanza.engine.pipeline;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import static cs.bilkent.zanza.engine.pipeline.OperatorInstanceInitializationTest.newUpstreamContextInstance;
import static cs.bilkent.zanza.engine.pipeline.OperatorInstanceStatus.COMPLETED;
import static cs.bilkent.zanza.engine.pipeline.OperatorInstanceStatus.COMPLETING;
import static cs.bilkent.zanza.engine.pipeline.UpstreamConnectionStatus.ACTIVE;
import static cs.bilkent.zanza.engine.pipeline.UpstreamConnectionStatus.CLOSED;
import static cs.bilkent.zanza.operator.InvocationContext.InvocationReason.OPERATOR_REQUESTED_SHUTDOWN;
import static cs.bilkent.zanza.operator.InvocationContext.InvocationReason.SUCCESS;
import cs.bilkent.zanza.operator.Tuple;
import cs.bilkent.zanza.operator.impl.TuplesImpl;
import cs.bilkent.zanza.operator.scheduling.ScheduleNever;
import cs.bilkent.zanza.operator.scheduling.ScheduleWhenAvailable;
import static cs.bilkent.zanza.operator.scheduling.ScheduleWhenTuplesAvailable.scheduleWhenTuplesAvailableOnDefaultPort;
import cs.bilkent.zanza.operator.scheduling.SchedulingStrategy;
import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith( MockitoJUnitRunner.class )
public class OperatorInstanceCompletingStatusTest extends AbstractOperatorInstanceInvocationTest
{


    @Test
    public void test_ScheduleWhenTuplesAvailable_satisfied_sameUpstreamContext ()
    {
        final int inputPortCount = 1, outputPortCount = 1;
        final SchedulingStrategy initializationStrategy = scheduleWhenTuplesAvailableOnDefaultPort( 1 );
        testSatisfiedSchedulingStrategy( inputPortCount,
                                         outputPortCount,
                                         initializationStrategy,
                                         newUpstreamContextInstance( 0, inputPortCount, ACTIVE ),
                                         null );
    }

    @Test
    public void test_ScheduleWhenAvailable_satisfied_sameUpstreamContext ()
    {
        final int inputPortCount = 1, outputPortCount = 1;
        final SchedulingStrategy initializationStrategy = ScheduleWhenAvailable.INSTANCE;
        testSatisfiedSchedulingStrategy( inputPortCount,
                                         outputPortCount,
                                         initializationStrategy,
                                         newUpstreamContextInstance( 0, inputPortCount, ACTIVE ),
                                         null );
    }

    @Test
    public void test_ScheduleWhenTuplesAvailable_satisfied_newUpstreamContext ()
    {
        final int inputPortCount = 1, outputPortCount = 1;
        final SchedulingStrategy initializationStrategy = scheduleWhenTuplesAvailableOnDefaultPort( 1 );
        testSatisfiedSchedulingStrategy( inputPortCount,
                                         outputPortCount,
                                         initializationStrategy,
                                         newUpstreamContextInstance( 1, inputPortCount, CLOSED ),
                                         null );
    }

    @Test
    public void test_ScheduleWhenAvailable_satisfied_newUpstreamContext ()
    {
        final int inputPortCount = 1, outputPortCount = 1;
        final SchedulingStrategy initializationStrategy = ScheduleWhenAvailable.INSTANCE;
        testSatisfiedSchedulingStrategy( inputPortCount,
                                         outputPortCount,
                                         initializationStrategy,
                                         newUpstreamContextInstance( 1, inputPortCount, CLOSED ),
                                         null );
    }

    @Test
    public void test_ScheduleWhenTuplesAvailable_satisfied_sameUpstreamContext_newSchedulingStrategyIgnored ()
    {
        final int inputPortCount = 1, outputPortCount = 1;
        final SchedulingStrategy initializationStrategy = scheduleWhenTuplesAvailableOnDefaultPort( 1 );
        testSatisfiedSchedulingStrategy( inputPortCount,
                                         outputPortCount,
                                         initializationStrategy,
                                         newUpstreamContextInstance( 0, inputPortCount, ACTIVE ),
                                         scheduleWhenTuplesAvailableOnDefaultPort( 2 ) );
    }

    private void testSatisfiedSchedulingStrategy ( final int inputPortCount,
                                                   final int outputPortCount,
                                                   final SchedulingStrategy initializationStrategy,
                                                   final UpstreamContext upstreamContext,
                                                   final SchedulingStrategy newSchedulingStrategy )
    {
        initializeOperatorInstance( inputPortCount, outputPortCount, initializationStrategy );

        final TuplesImpl operatorInput = TuplesImpl.newInstanceWithSinglePort( new Tuple( "f1", "val2" ) );
        when( drainer.getResult() ).thenReturn( operatorInput );

        final TuplesImpl expectedOutput = TuplesImpl.newInstanceWithSinglePort( new Tuple( "f1", "val3" ) );
        when( outputSupplier.get() ).thenReturn( expectedOutput );

        invocationContext.setNewSchedulingStrategy( newSchedulingStrategy );

        final TuplesImpl upstreamInput = TuplesImpl.newInstanceWithSinglePort( new Tuple( "f1", "val1" ) );
        final TuplesImpl output = operatorInstance.invoke( upstreamInput, upstreamContext );

        verify( queue ).offer( 0, singletonList( new Tuple( "f1", "val1" ) ) );
        verify( queue ).drain( drainer );
        assertOperatorInvocation();
        verify( drainerPool, never() ).release( drainer );
        verify( drainerPool ).acquire( initializationStrategy );

        assertThat( invocationContext.getReason(), equalTo( SUCCESS ) );
        assertThat( invocationContext.getKVStore(), equalTo( kvStore ) );
        assertThat( invocationContext.getInput(), equalTo( operatorInput ) );

        assertThat( output, equalTo( expectedOutput ) );
        assertThat( operatorInstance.getSchedulingStrategy(), equalTo( initializationStrategy ) );
        assertThat( operatorInstance.getCompletionReason(), equalTo( OPERATOR_REQUESTED_SHUTDOWN ) );
        assertThat( operatorInstance.getUpstreamContext(), equalTo( upstreamContext ) );
        assertThat( operatorInstance.getStatus(), equalTo( COMPLETING ) );
    }

    @Test
    public void test_ScheduleWhenTuplesAvailable_notSatisfied_switchesTo_ScheduleWhenAvailable_satisfied ()
    {
        final int inputPortCount = 1, outputPortCount = 1;
        final SchedulingStrategy initializationStrategy = scheduleWhenTuplesAvailableOnDefaultPort( 1 );
        final UpstreamContext upstreamContext = newUpstreamContextInstance( 0, inputPortCount, ACTIVE );
        initializeOperatorInstance( inputPortCount, outputPortCount, initializationStrategy );

        final TuplesImpl operatorInput = TuplesImpl.newInstanceWithSinglePort( new Tuple( "f1", "val2" ) );
        when( drainer.getResult() ).thenReturn( null, operatorInput );

        final TuplesImpl expectedOutput = TuplesImpl.newInstanceWithSinglePort( new Tuple( "f1", "val3" ) );
        when( outputSupplier.get() ).thenReturn( expectedOutput );

        final TuplesImpl upstreamInput = TuplesImpl.newInstanceWithSinglePort( new Tuple( "f1", "val1" ) );
        final TuplesImpl output = operatorInstance.invoke( upstreamInput, upstreamContext );

        verify( queue ).offer( 0, singletonList( new Tuple( "f1", "val1" ) ) );
        verify( queue, times( 2 ) ).drain( drainer );
        assertOperatorInvocation();
        verify( drainerPool ).release( drainer );
        verify( drainerPool ).acquire( initializationStrategy );
        verify( drainerPool ).acquire( ScheduleWhenAvailable.INSTANCE );

        assertThat( invocationContext.getReason(), equalTo( OPERATOR_REQUESTED_SHUTDOWN ) );
        assertThat( invocationContext.getKVStore(), equalTo( kvStore ) );
        assertThat( invocationContext.getInput(), equalTo( operatorInput ) );

        assertThat( output, equalTo( expectedOutput ) );
        assertThat( operatorInstance.getSchedulingStrategy(), equalTo( ScheduleWhenAvailable.INSTANCE ) );
        assertThat( operatorInstance.getCompletionReason(), equalTo( OPERATOR_REQUESTED_SHUTDOWN ) );
        assertThat( operatorInstance.getUpstreamContext(), equalTo( upstreamContext ) );
        assertThat( operatorInstance.getStatus(), equalTo( COMPLETING ) );
    }

    @Test
    public void test_ScheduleWhenTuplesAvailable_notSatisfied_switchesTo_ScheduleWhenAvailable_notSatisfied ()
    {
        final int inputPortCount = 1, outputPortCount = 1;
        final SchedulingStrategy initializationStrategy = scheduleWhenTuplesAvailableOnDefaultPort( 1 );
        final UpstreamContext upstreamContext = newUpstreamContextInstance( 0, inputPortCount, ACTIVE );
        initializeOperatorInstance( inputPortCount, outputPortCount, initializationStrategy );

        final TuplesImpl expectedOutput = TuplesImpl.newInstanceWithSinglePort( new Tuple( "f1", "val3" ) );
        when( outputSupplier.get() ).thenReturn( expectedOutput );

        final TuplesImpl upstreamInput = TuplesImpl.newInstanceWithSinglePort( new Tuple( "f1", "val1" ) );
        final TuplesImpl output = operatorInstance.invoke( upstreamInput, upstreamContext );

        verify( queue ).offer( 0, singletonList( new Tuple( "f1", "val1" ) ) );
        verify( queue, times( 2 ) ).drain( drainer );
        assertNoOperatorInvocation();
        verify( drainerPool, times( 2 ) ).release( drainer );
        verify( drainerPool ).acquire( initializationStrategy );
        verify( drainerPool ).acquire( ScheduleWhenAvailable.INSTANCE );

        assertNull( output );
        assertThat( operatorInstance.getSchedulingStrategy(), equalTo( ScheduleNever.INSTANCE ) );
        assertThat( operatorInstance.getCompletionReason(), equalTo( OPERATOR_REQUESTED_SHUTDOWN ) );
        assertThat( operatorInstance.getUpstreamContext(), equalTo( upstreamContext ) );
        assertThat( operatorInstance.getStatus(), equalTo( COMPLETED ) );
        assertThat( operatorInstance.getSelfUpstreamContext(), equalTo( newUpstreamContextInstance( 1, inputPortCount, CLOSED ) ) );
    }

    @Test
    public void test_ScheduleWhenAvailable_notSatisfied ()
    {
        final int inputPortCount = 1, outputPortCount = 1;
        final SchedulingStrategy initializationStrategy = ScheduleWhenAvailable.INSTANCE;
        final UpstreamContext upstreamContext = newUpstreamContextInstance( 0, inputPortCount, ACTIVE );
        initializeOperatorInstance( inputPortCount, outputPortCount, initializationStrategy );

        final TuplesImpl operatorInput = TuplesImpl.newInstanceWithSinglePort( new Tuple( "f1", "val2" ) );
        when( drainer.getResult() ).thenReturn( null, operatorInput );

        final TuplesImpl expectedOutput = TuplesImpl.newInstanceWithSinglePort( new Tuple( "f1", "val3" ) );
        when( outputSupplier.get() ).thenReturn( expectedOutput );

        final TuplesImpl upstreamInput = TuplesImpl.newInstanceWithSinglePort( new Tuple( "f1", "val1" ) );
        final TuplesImpl output = operatorInstance.invoke( upstreamInput, upstreamContext );

        verify( queue ).offer( 0, singletonList( new Tuple( "f1", "val1" ) ) );
        verify( queue ).drain( drainer );
        assertNoOperatorInvocation();
        verify( drainerPool ).release( drainer );
        verify( drainerPool ).acquire( initializationStrategy );

        assertNull( output );
        assertThat( operatorInstance.getSchedulingStrategy(), equalTo( ScheduleNever.INSTANCE ) );
        assertThat( operatorInstance.getCompletionReason(), equalTo( OPERATOR_REQUESTED_SHUTDOWN ) );
        assertThat( operatorInstance.getUpstreamContext(), equalTo( upstreamContext ) );
        assertThat( operatorInstance.getStatus(), equalTo( COMPLETED ) );
        assertThat( operatorInstance.getSelfUpstreamContext(), equalTo( newUpstreamContextInstance( 1, inputPortCount, CLOSED ) ) );
    }

    protected void initializeOperatorInstance ( final int inputPortCount,
                                                final int outputPortCount,
                                                final SchedulingStrategy schedulingStrategy )
    {
        super.initializeOperatorInstance( inputPortCount, outputPortCount, schedulingStrategy );
        moveOperatorInstanceToStatus( COMPLETING );
    }

}
