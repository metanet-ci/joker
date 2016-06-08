package cs.bilkent.zanza.engine.pipeline;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import static cs.bilkent.zanza.engine.pipeline.OperatorInstanceInitializationTest.newUpstreamContextInstance;
import static cs.bilkent.zanza.engine.pipeline.OperatorInstanceStatus.COMPLETING;
import static cs.bilkent.zanza.engine.pipeline.OperatorInstanceStatus.RUNNING;
import static cs.bilkent.zanza.engine.pipeline.UpstreamConnectionStatus.ACTIVE;
import static cs.bilkent.zanza.engine.pipeline.UpstreamConnectionStatus.CLOSED;
import cs.bilkent.zanza.operator.InvocationContext.InvocationReason;
import static cs.bilkent.zanza.operator.InvocationContext.InvocationReason.INPUT_PORT_CLOSED;
import static cs.bilkent.zanza.operator.InvocationContext.InvocationReason.OPERATOR_REQUESTED_SHUTDOWN;
import static cs.bilkent.zanza.operator.InvocationContext.InvocationReason.SHUTDOWN;
import static cs.bilkent.zanza.operator.InvocationContext.InvocationReason.SUCCESS;
import cs.bilkent.zanza.operator.Tuple;
import cs.bilkent.zanza.operator.impl.TuplesImpl;
import cs.bilkent.zanza.operator.scheduling.ScheduleWhenAvailable;
import static cs.bilkent.zanza.operator.scheduling.ScheduleWhenTuplesAvailable.scheduleWhenTuplesAvailableOnAll;
import static cs.bilkent.zanza.operator.scheduling.ScheduleWhenTuplesAvailable.scheduleWhenTuplesAvailableOnAny;
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
public class OperatorInstanceRunningStatusTest extends AbstractOperatorInstanceInvocationTest
{

    @Test
    public void test_ScheduleWhenTuplesAvailable_satisfied_sameUpstreamContext ()
    {
        final int inputPortCount = 1, outputPortCount = 1;
        final SchedulingStrategy initializationStrategy = scheduleWhenTuplesAvailableOnDefaultPort( 1 );
        final SchedulingStrategy expectedOutputStrategy = scheduleWhenTuplesAvailableOnDefaultPort( 2 );
        testSatisfiedSchedulingStrategy( inputPortCount,
                                         outputPortCount,
                                         initializationStrategy,
                                         expectedOutputStrategy,
                                         newUpstreamContextInstance( 0, inputPortCount, ACTIVE ) );
    }

    @Test
    public void test_ScheduleWhenTuplesAvailable_satisfied_sameUpstreamContext_invalidNewSchedulingStrategy ()
    {
        final int inputPortCount = 1, outputPortCount = 1;
        final SchedulingStrategy initializationStrategy = scheduleWhenTuplesAvailableOnDefaultPort( 1 );
        initializeOperatorInstance( inputPortCount, outputPortCount, initializationStrategy );
        final SchedulingStrategy invalidNewSchedulingStrategy = scheduleWhenTuplesAvailableOnAll( 2, 1, 0, 1 );
        final UpstreamContext upstreamContext = newUpstreamContextInstance( 0, inputPortCount, ACTIVE );

        final TuplesImpl operatorInput = TuplesImpl.newInstanceWithSinglePort( new Tuple( "f1", "val2" ) );
        when( drainer.getResult() ).thenReturn( operatorInput );
        final TuplesImpl expectedOutput = TuplesImpl.newInstanceWithSinglePort( new Tuple( "f1", "val3" ) );
        when( outputSupplier.get() ).thenReturn( expectedOutput );

        invocationContext.setNewSchedulingStrategy( invalidNewSchedulingStrategy );

        final TuplesImpl upstreamInput = TuplesImpl.newInstanceWithSinglePort( new Tuple( "f1", "val1" ) );
        final TuplesImpl output = operatorInstance.invoke( upstreamInput, upstreamContext );

        verify( queue ).offer( 0, singletonList( new Tuple( "f1", "val1" ) ) );
        verify( queue ).drain( drainer );
        assertOperatorInvocation();
        verify( drainerPool, never() ).release( drainer );

        assertThat( invocationContext.getReason(), equalTo( SUCCESS ) );
        assertThat( invocationContext.getKVStore(), equalTo( kvStore ) );

        assertThat( output, equalTo( expectedOutput ) );
        assertThat( operatorInstance.getSchedulingStrategy(), equalTo( initializationStrategy ) );
        assertThat( operatorInstance.getCompletionReason(), equalTo( OPERATOR_REQUESTED_SHUTDOWN ) );
        assertThat( operatorInstance.getUpstreamContext(), equalTo( upstreamContext ) );
        assertThat( operatorInstance.getStatus(), equalTo( COMPLETING ) );
    }

    @Test
    public void test_ScheduleWhenTuplesAvailable_satisfied_newUpstreamContextWithInputPortClosed ()
    {
        final int inputPortCount = 1, outputPortCount = 1;
        final SchedulingStrategy initializationStrategy = scheduleWhenTuplesAvailableOnDefaultPort( 1 );
        final SchedulingStrategy expectedOutputStrategy = scheduleWhenTuplesAvailableOnDefaultPort( 2 );
        final UpstreamContext newUpstreamContext = newUpstreamContextInstance( 1, inputPortCount, CLOSED );
        testSatisfiedSchedulingStrategy( inputPortCount,
                                         outputPortCount,
                                         initializationStrategy,
                                         expectedOutputStrategy,
                                         newUpstreamContext );
    }

    @Test
    public void test_ScheduleWhenAvailable_satisfied_sameUpstreamContext ()
    {
        final int inputPortCount = 1, outputPortCount = 1;
        final SchedulingStrategy initializationStrategy = ScheduleWhenAvailable.INSTANCE;
        final SchedulingStrategy expectedOutputStrategy = scheduleWhenTuplesAvailableOnDefaultPort( 2 );
        testSatisfiedSchedulingStrategy( inputPortCount,
                                         outputPortCount,
                                         initializationStrategy,
                                         expectedOutputStrategy,
                                         newUpstreamContextInstance( 0, inputPortCount, ACTIVE ) );
    }

    @Test
    public void test_ScheduleWhenAvailable_satisfied_sameUpstreamContext_invalidNewSchedulingStrategy ()
    {
        final int inputPortCount = 1, outputPortCount = 1;
        final SchedulingStrategy initializationStrategy = ScheduleWhenAvailable.INSTANCE;
        final SchedulingStrategy invalidOutputStrategy = scheduleWhenTuplesAvailableOnAny( 2, 1, 0, 1 );
        initializeOperatorInstance( inputPortCount, outputPortCount, initializationStrategy );

        final TuplesImpl operatorInput = TuplesImpl.newInstanceWithSinglePort( new Tuple( "f1", "val2" ) );
        when( drainer.getResult() ).thenReturn( operatorInput );

        final TuplesImpl expectedOutput = TuplesImpl.newInstanceWithSinglePort( new Tuple( "f1", "val3" ) );
        when( outputSupplier.get() ).thenReturn( expectedOutput );

        invocationContext.setNewSchedulingStrategy( invalidOutputStrategy );

        final TuplesImpl upstreamInput = TuplesImpl.newInstanceWithSinglePort( new Tuple( "f1", "val1" ) );
        final TuplesImpl output = operatorInstance.invoke( upstreamInput, initializationUpstreamContext );

        verify( queue ).offer( 0, singletonList( new Tuple( "f1", "val1" ) ) );
        verify( queue ).drain( drainer );
        assertOperatorInvocation();
        verify( drainerPool, never() ).release( drainer );

        assertThat( invocationContext.getReason(), equalTo( SUCCESS ) );
        assertThat( invocationContext.getKVStore(), equalTo( kvStore ) );
        assertThat( invocationContext.getInput(), equalTo( operatorInput ) );

        assertThat( output, equalTo( expectedOutput ) );
        assertThat( operatorInstance.getSchedulingStrategy(), equalTo( ScheduleWhenAvailable.INSTANCE ) );
        assertThat( operatorInstance.getCompletionReason(), equalTo( OPERATOR_REQUESTED_SHUTDOWN ) );
        assertThat( operatorInstance.getUpstreamContext(), equalTo( initializationUpstreamContext ) );
        assertThat( operatorInstance.getStatus(), equalTo( COMPLETING ) );
    }

    private void testSatisfiedSchedulingStrategy ( final int inputPortCount,
                                                   final int outputPortCount,
                                                   final SchedulingStrategy initializationStrategy,
                                                   final SchedulingStrategy expectedOutputStrategy,
                                                   final UpstreamContext upstreamContext )
    {
        initializeOperatorInstance( inputPortCount, outputPortCount, initializationStrategy );

        final TuplesImpl operatorInput = TuplesImpl.newInstanceWithSinglePort( new Tuple( "f1", "val2" ) );
        when( drainer.getResult() ).thenReturn( operatorInput );

        final TuplesImpl expectedOutput = TuplesImpl.newInstanceWithSinglePort( new Tuple( "f1", "val3" ) );
        when( outputSupplier.get() ).thenReturn( expectedOutput );

        invocationContext.setNewSchedulingStrategy( expectedOutputStrategy );

        final TuplesImpl upstreamInput = TuplesImpl.newInstanceWithSinglePort( new Tuple( "f1", "val1" ) );
        final TuplesImpl output = operatorInstance.invoke( upstreamInput, upstreamContext );

        verify( queue ).offer( 0, singletonList( new Tuple( "f1", "val1" ) ) );
        verify( queue ).drain( drainer );
        assertOperatorInvocation();
        verify( drainerPool ).release( drainer );
        verify( drainerPool ).acquire( expectedOutputStrategy );

        assertThat( invocationContext.getReason(), equalTo( SUCCESS ) );
        assertThat( invocationContext.getKVStore(), equalTo( kvStore ) );
        assertThat( invocationContext.getInput(), equalTo( operatorInput ) );

        assertThat( output, equalTo( expectedOutput ) );
        assertThat( operatorInstance.getSchedulingStrategy(), equalTo( expectedOutputStrategy ) );
        assertNull( operatorInstance.getCompletionReason() );
        assertThat( operatorInstance.getUpstreamContext(), equalTo( initializationUpstreamContext ) );
        assertThat( operatorInstance.getStatus(), equalTo( RUNNING ) );
    }

    @Test
    public void test_ScheduleWhenAvailable_satisfied_newUpstreamContextWithSingleInputPortClosed ()
    {
        final int inputPortCount = 2, outputPortCount = 1;
        final SchedulingStrategy initializationStrategy = ScheduleWhenAvailable.INSTANCE;
        final SchedulingStrategy expectedOutputStrategy = scheduleWhenTuplesAvailableOnAny( inputPortCount, 1, 0 );
        final UpstreamContext newUpstreamContext = new UpstreamContext( 1, new UpstreamConnectionStatus[] { ACTIVE, CLOSED } );

        initializeOperatorInstance( inputPortCount, outputPortCount, initializationStrategy );

        final TuplesImpl operatorInput = TuplesImpl.newInstanceWithSinglePort( new Tuple( "f1", "val2" ) );
        when( drainer.getResult() ).thenReturn( operatorInput );

        final TuplesImpl expectedOutput = TuplesImpl.newInstanceWithSinglePort( new Tuple( "f1", "val3" ) );
        when( outputSupplier.get() ).thenReturn( expectedOutput );

        invocationContext.setNewSchedulingStrategy( expectedOutputStrategy );

        final TuplesImpl upstreamInput = TuplesImpl.newInstanceWithSinglePort( new Tuple( "f1", "val1" ) );
        final TuplesImpl output = operatorInstance.invoke( upstreamInput, newUpstreamContext );

        verify( queue ).offer( 0, singletonList( new Tuple( "f1", "val1" ) ) );
        verify( queue ).drain( drainer );
        assertOperatorInvocation();
        verify( drainerPool ).release( drainer );
        verify( drainerPool ).acquire( expectedOutputStrategy );

        assertThat( invocationContext.getReason(), equalTo( INPUT_PORT_CLOSED ) );
        assertThat( invocationContext.getKVStore(), equalTo( kvStore ) );
        assertThat( invocationContext.getInput(), equalTo( operatorInput ) );

        assertThat( output, equalTo( expectedOutput ) );
        assertThat( operatorInstance.getSchedulingStrategy(), equalTo( expectedOutputStrategy ) );
        assertNull( operatorInstance.getCompletionReason() );
        assertThat( operatorInstance.getUpstreamContext(), equalTo( newUpstreamContext ) );
        assertThat( operatorInstance.getStatus(), equalTo( RUNNING ) );
    }

    @Test
    public void test_ScheduleWhenAvailable_satisfied_newUpstreamContextWithSingleInputPortClosed_noNewSchedulingStrategy ()
    {
        final int inputPortCount = 2, outputPortCount = 1;
        final SchedulingStrategy initializationStrategy = ScheduleWhenAvailable.INSTANCE;
        final UpstreamContext newUpstreamContext = new UpstreamContext( 1, new UpstreamConnectionStatus[] { ACTIVE, CLOSED } );

        initializeOperatorInstance( inputPortCount, outputPortCount, initializationStrategy );

        final TuplesImpl operatorInput = TuplesImpl.newInstanceWithSinglePort( new Tuple( "f1", "val2" ) );
        when( drainer.getResult() ).thenReturn( operatorInput );

        final TuplesImpl expectedOutput = TuplesImpl.newInstanceWithSinglePort( new Tuple( "f1", "val3" ) );
        when( outputSupplier.get() ).thenReturn( expectedOutput );

        final TuplesImpl upstreamInput = TuplesImpl.newInstanceWithSinglePort( new Tuple( "f1", "val1" ) );
        final TuplesImpl output = operatorInstance.invoke( upstreamInput, newUpstreamContext );

        verify( queue ).offer( 0, singletonList( new Tuple( "f1", "val1" ) ) );
        verify( queue ).drain( drainer );
        assertOperatorInvocation();
        verify( drainerPool, never() ).release( drainer );

        assertThat( invocationContext.getReason(), equalTo( INPUT_PORT_CLOSED ) );
        assertThat( invocationContext.getKVStore(), equalTo( kvStore ) );
        assertThat( invocationContext.getInput(), equalTo( operatorInput ) );

        assertThat( output, equalTo( expectedOutput ) );
        assertThat( operatorInstance.getSchedulingStrategy(), equalTo( ScheduleWhenAvailable.INSTANCE ) );
        assertNull( operatorInstance.getCompletionReason() );
        assertThat( operatorInstance.getUpstreamContext(), equalTo( newUpstreamContext ) );
        assertThat( operatorInstance.getStatus(), equalTo( RUNNING ) );
    }

    @Test
    public void test_ScheduleWhenAvailable_satisfied_newUpstreamContextWithAllInputPortsClosed ()
    {
        final int inputPortCount = 2, outputPortCount = 1;
        final SchedulingStrategy initializationStrategy = ScheduleWhenAvailable.INSTANCE;
        final UpstreamContext newUpstreamContext = new UpstreamContext( 1, new UpstreamConnectionStatus[] { CLOSED, CLOSED } );

        testScheduleWhenAvailableSatisfiedWithNewUpstreamContextStatusMovesToCompleting( inputPortCount,
                                                                                         outputPortCount,
                                                                                         initializationStrategy,
                                                                                         newUpstreamContext );
    }

    @Test
    public void test_ScheduleWhenAvailable_satisfied_newUpstreamContext_inputPortCount0 ()
    {
        final int inputPortCount = 0, outputPortCount = 1;
        final SchedulingStrategy initializationStrategy = ScheduleWhenAvailable.INSTANCE;
        final UpstreamContext newUpstreamContext = new UpstreamContext( 1, new UpstreamConnectionStatus[] {} );

        testScheduleWhenAvailableSatisfiedWithNewUpstreamContextStatusMovesToCompleting( inputPortCount,
                                                                                         outputPortCount,
                                                                                         initializationStrategy,
                                                                                         newUpstreamContext );
    }

    private void testScheduleWhenAvailableSatisfiedWithNewUpstreamContextStatusMovesToCompleting ( final int inputPortCount,
                                                                                                   final int outputPortCount,
                                                                                                   final SchedulingStrategy
                                                                                                           initializationStrategy,
                                                                                                   final UpstreamContext
                                                                                                           newUpstreamContext )
    {
        initializeOperatorInstance( inputPortCount, outputPortCount, initializationStrategy );

        final TuplesImpl operatorInput = TuplesImpl.newInstanceWithSinglePort( new Tuple( "f1", "val2" ) );
        when( drainer.getResult() ).thenReturn( operatorInput );

        final TuplesImpl expectedOutput = TuplesImpl.newInstanceWithSinglePort( new Tuple( "f1", "val3" ) );
        when( outputSupplier.get() ).thenReturn( expectedOutput );

        final TuplesImpl upstreamInput = TuplesImpl.newInstanceWithSinglePort( new Tuple( "f1", "val1" ) );
        final TuplesImpl output = operatorInstance.invoke( upstreamInput, newUpstreamContext );

        verify( queue ).offer( 0, singletonList( new Tuple( "f1", "val1" ) ) );
        verify( queue ).drain( drainer );
        assertOperatorInvocation();

        final InvocationReason expectedReason = inputPortCount == 0 ? SHUTDOWN : INPUT_PORT_CLOSED;

        assertThat( invocationContext.getReason(), equalTo( expectedReason ) );
        assertThat( invocationContext.getKVStore(), equalTo( kvStore ) );
        assertThat( invocationContext.getInput(), equalTo( operatorInput ) );

        assertThat( output, equalTo( expectedOutput ) );
        assertThat( operatorInstance.getSchedulingStrategy(), equalTo( initializationStrategy ) );
        assertThat( operatorInstance.getUpstreamContext(), equalTo( newUpstreamContext ) );
        assertThat( operatorInstance.getCompletionReason(), equalTo( expectedReason ) );
        assertThat( operatorInstance.getStatus(), equalTo( COMPLETING ) );
    }

    @Test
    public void test_ScheduleWhenTuplesAvailable_satisfied_newUpstreamContextWithSingleInputPortClosed_invalidNewSchedulingStrategy ()
    {
        final int inputPortCount = 2, outputPortCount = 1;
        final SchedulingStrategy initializationStrategy = scheduleWhenTuplesAvailableOnAll( 2, 1, 0, 1 );
        initializeOperatorInstance( inputPortCount, outputPortCount, initializationStrategy );
        final UpstreamContext newUpstreamContext = new UpstreamContext( 1, new UpstreamConnectionStatus[] { ACTIVE, CLOSED } );

        final TuplesImpl operatorInput = TuplesImpl.newInstanceWithSinglePort( new Tuple( "f1", "val2" ) );
        when( drainer.getResult() ).thenReturn( null, operatorInput );
        final TuplesImpl expectedOutput = TuplesImpl.newInstanceWithSinglePort( new Tuple( "f1", "val3" ) );
        when( outputSupplier.get() ).thenReturn( expectedOutput );

        invocationContext.setNewSchedulingStrategy( initializationStrategy );

        final TuplesImpl upstreamInput = TuplesImpl.newInstanceWithSinglePort( new Tuple( "f1", "val1" ) );
        final TuplesImpl output = operatorInstance.invoke( upstreamInput, newUpstreamContext );

        verify( queue ).offer( 0, singletonList( new Tuple( "f1", "val1" ) ) );
        verify( queue, times( 2 ) ).drain( drainer );
        assertOperatorInvocation();
        verify( drainerPool ).release( drainer );
        verify( drainerPool ).acquire( ScheduleWhenAvailable.INSTANCE );

        assertThat( invocationContext.getReason(), equalTo( INPUT_PORT_CLOSED ) );
        assertThat( invocationContext.getKVStore(), equalTo( kvStore ) );

        assertThat( output, equalTo( expectedOutput ) );
        assertThat( operatorInstance.getSchedulingStrategy(), equalTo( ScheduleWhenAvailable.INSTANCE ) );
        assertThat( operatorInstance.getCompletionReason(), equalTo( OPERATOR_REQUESTED_SHUTDOWN ) );
        assertThat( operatorInstance.getUpstreamContext(), equalTo( newUpstreamContext ) );
        assertThat( operatorInstance.getStatus(), equalTo( COMPLETING ) );
    }

    @Test
    public void test_ScheduleWhenTuplesAvailable_notSatisfied_sameUpstreamContext ()
    {
        final int inputPortCount = 1, outputPortCount = 1;
        final SchedulingStrategy initializationStrategy = scheduleWhenTuplesAvailableOnDefaultPort( 1 );
        initializeOperatorInstance( inputPortCount, outputPortCount, initializationStrategy );

        final TuplesImpl upstreamInput = TuplesImpl.newInstanceWithSinglePort( new Tuple( "f1", "val1" ) );
        final UpstreamContext upstreamContext = newUpstreamContextInstance( 0, inputPortCount, ACTIVE );
        final TuplesImpl output = operatorInstance.invoke( upstreamInput, upstreamContext );

        verify( queue ).offer( 0, singletonList( new Tuple( "f1", "val1" ) ) );
        verify( queue ).drain( drainer );
        verify( drainer ).reset();
        assertNoOperatorInvocation();
        assertNull( output );
        assertThat( operatorInstance.getSchedulingStrategy(), equalTo( initializationStrategy ) );
        assertThat( operatorInstance.getUpstreamContext(), equalTo( upstreamContext ) );
        assertNull( operatorInstance.getCompletionReason() );
        assertThat( operatorInstance.getStatus(), equalTo( RUNNING ) );
    }

    @Test
    public void test_ScheduleWhenTuplesAvailable_notSatisfied_newUpstreamContextWithSingleInputPortClosed ()
    {
        final int inputPortCount = 2, outputPortCount = 1;
        final SchedulingStrategy initializationStrategy = scheduleWhenTuplesAvailableOnAll( 2, 1, 0, 1 );
        initializeOperatorInstance( inputPortCount, outputPortCount, initializationStrategy );
        final SchedulingStrategy expectedOutputStrategy = scheduleWhenTuplesAvailableOnAny( inputPortCount, 1, 0 );
        final UpstreamContext newUpstreamContext = new UpstreamContext( 1, new UpstreamConnectionStatus[] { ACTIVE, CLOSED } );

        final TuplesImpl operatorInput = TuplesImpl.newInstanceWithSinglePort( new Tuple( "f1", "val2" ) );
        when( drainer.getResult() ).thenReturn( null, operatorInput );
        final TuplesImpl expectedOutput = TuplesImpl.newInstanceWithSinglePort( new Tuple( "f1", "val3" ) );
        when( outputSupplier.get() ).thenReturn( expectedOutput );

        invocationContext.setNewSchedulingStrategy( expectedOutputStrategy );

        final TuplesImpl upstreamInput = TuplesImpl.newInstanceWithSinglePort( new Tuple( "f1", "val1" ) );
        final TuplesImpl output = operatorInstance.invoke( upstreamInput, newUpstreamContext );

        verify( queue ).offer( 0, singletonList( new Tuple( "f1", "val1" ) ) );
        verify( queue, times( 2 ) ).drain( drainer );
        assertOperatorInvocation();
        verify( drainerPool, times( 2 ) ).release( drainer );
        verify( drainerPool ).acquire( ScheduleWhenAvailable.INSTANCE );
        verify( drainerPool ).acquire( expectedOutputStrategy );

        assertThat( invocationContext.getReason(), equalTo( INPUT_PORT_CLOSED ) );
        assertThat( invocationContext.getKVStore(), equalTo( kvStore ) );

        assertThat( output, equalTo( expectedOutput ) );
        assertThat( operatorInstance.getSchedulingStrategy(), equalTo( expectedOutputStrategy ) );
        assertNull( operatorInstance.getCompletionReason() );
        assertThat( operatorInstance.getUpstreamContext(), equalTo( newUpstreamContext ) );
        assertThat( operatorInstance.getStatus(), equalTo( RUNNING ) );
    }

    @Test
    public void test_ScheduleWhenTuplesAvailable_notSatisfied_newUpstreamContextWithSingleInputPortClosed_invalidNewSchedulingStrategy ()
    {
        final int inputPortCount = 2, outputPortCount = 1;
        final SchedulingStrategy initializationStrategy = scheduleWhenTuplesAvailableOnAll( 2, 1, 0, 1 );
        initializeOperatorInstance( inputPortCount, outputPortCount, initializationStrategy );
        final UpstreamContext newUpstreamContext = new UpstreamContext( 1, new UpstreamConnectionStatus[] { ACTIVE, CLOSED } );

        final TuplesImpl operatorInput = TuplesImpl.newInstanceWithSinglePort( new Tuple( "f1", "val2" ) );
        when( drainer.getResult() ).thenReturn( null, operatorInput );
        final TuplesImpl expectedOutput = TuplesImpl.newInstanceWithSinglePort( new Tuple( "f1", "val3" ) );
        when( outputSupplier.get() ).thenReturn( expectedOutput );

        invocationContext.setNewSchedulingStrategy( initializationStrategy );

        final TuplesImpl upstreamInput = TuplesImpl.newInstanceWithSinglePort( new Tuple( "f1", "val1" ) );
        final TuplesImpl output = operatorInstance.invoke( upstreamInput, newUpstreamContext );

        verify( queue ).offer( 0, singletonList( new Tuple( "f1", "val1" ) ) );
        verify( queue, times( 2 ) ).drain( drainer );
        assertOperatorInvocation();
        verify( drainerPool ).release( drainer );
        verify( drainerPool ).acquire( ScheduleWhenAvailable.INSTANCE );

        assertThat( invocationContext.getReason(), equalTo( INPUT_PORT_CLOSED ) );
        assertThat( invocationContext.getKVStore(), equalTo( kvStore ) );

        assertThat( output, equalTo( expectedOutput ) );
        assertThat( operatorInstance.getSchedulingStrategy(), equalTo( ScheduleWhenAvailable.INSTANCE ) );
        assertThat( operatorInstance.getCompletionReason(), equalTo( OPERATOR_REQUESTED_SHUTDOWN ) );
        assertThat( operatorInstance.getUpstreamContext(), equalTo( newUpstreamContext ) );
        assertThat( operatorInstance.getStatus(), equalTo( COMPLETING ) );
    }

    @Test
    public void test_ScheduleWhenTuplesAvailable_notSatisfied_newUpstreamContextWithSingleInputPortClosed_noNewSchedulingStrategy ()
    {
        final int inputPortCount = 2, outputPortCount = 1;
        final SchedulingStrategy initializationStrategy = scheduleWhenTuplesAvailableOnAll( 2, 1, 0, 1 );
        initializeOperatorInstance( inputPortCount, outputPortCount, initializationStrategy );
        final UpstreamContext newUpstreamContext = new UpstreamContext( 1, new UpstreamConnectionStatus[] { ACTIVE, CLOSED } );

        final TuplesImpl operatorInput = TuplesImpl.newInstanceWithSinglePort( new Tuple( "f1", "val2" ) );
        when( drainer.getResult() ).thenReturn( null, operatorInput );
        final TuplesImpl expectedOutput = TuplesImpl.newInstanceWithSinglePort( new Tuple( "f1", "val3" ) );
        when( outputSupplier.get() ).thenReturn( expectedOutput );

        final TuplesImpl upstreamInput = TuplesImpl.newInstanceWithSinglePort( new Tuple( "f1", "val1" ) );
        final TuplesImpl output = operatorInstance.invoke( upstreamInput, newUpstreamContext );

        verify( queue ).offer( 0, singletonList( new Tuple( "f1", "val1" ) ) );
        verify( queue, times( 2 ) ).drain( drainer );
        assertOperatorInvocation();
        verify( drainerPool ).release( drainer );
        verify( drainerPool ).acquire( ScheduleWhenAvailable.INSTANCE );

        assertThat( invocationContext.getReason(), equalTo( INPUT_PORT_CLOSED ) );
        assertThat( invocationContext.getKVStore(), equalTo( kvStore ) );

        assertThat( output, equalTo( expectedOutput ) );
        assertThat( operatorInstance.getSchedulingStrategy(), equalTo( ScheduleWhenAvailable.INSTANCE ) );
        assertNull( operatorInstance.getCompletionReason() );
        assertThat( operatorInstance.getUpstreamContext(), equalTo( newUpstreamContext ) );
        assertThat( operatorInstance.getStatus(), equalTo( RUNNING ) );
    }

    @Test
    public void test_ScheduleWhenTuplesAvailable_notSatisfied_newUpstreamContextWithAllInputPortsClosed ()
    {
        final int inputPortCount = 2, outputPortCount = 1;
        final SchedulingStrategy initializationStrategy = scheduleWhenTuplesAvailableOnAll( 2, 1, 0, 1 );
        initializeOperatorInstance( inputPortCount, outputPortCount, initializationStrategy );
        final UpstreamContext newUpstreamContext = new UpstreamContext( 1, new UpstreamConnectionStatus[] { CLOSED, CLOSED } );

        final TuplesImpl operatorInput = TuplesImpl.newInstanceWithSinglePort( new Tuple( "f1", "val2" ) );
        when( drainer.getResult() ).thenReturn( null, operatorInput );
        final TuplesImpl expectedOutput = TuplesImpl.newInstanceWithSinglePort( new Tuple( "f1", "val3" ) );
        when( outputSupplier.get() ).thenReturn( expectedOutput );

        final TuplesImpl upstreamInput = TuplesImpl.newInstanceWithSinglePort( new Tuple( "f1", "val1" ) );
        final TuplesImpl output = operatorInstance.invoke( upstreamInput, newUpstreamContext );

        verify( queue ).offer( 0, singletonList( new Tuple( "f1", "val1" ) ) );
        verify( queue, times( 2 ) ).drain( drainer );
        assertOperatorInvocation();
        verify( drainerPool ).release( drainer );
        verify( drainerPool ).acquire( ScheduleWhenAvailable.INSTANCE );

        assertThat( invocationContext.getReason(), equalTo( INPUT_PORT_CLOSED ) );
        assertThat( invocationContext.getKVStore(), equalTo( kvStore ) );

        assertThat( output, equalTo( expectedOutput ) );
        assertThat( operatorInstance.getSchedulingStrategy(), equalTo( ScheduleWhenAvailable.INSTANCE ) );
        assertThat( operatorInstance.getCompletionReason(), equalTo( INPUT_PORT_CLOSED ) );
        assertThat( operatorInstance.getUpstreamContext(), equalTo( newUpstreamContext ) );
        assertThat( operatorInstance.getStatus(), equalTo( COMPLETING ) );
    }

    @Test
    public void test_ScheduleWhenAvailable_notSatisfied_sameUpstreamContext ()
    {
        final int inputPortCount = 1, outputPortCount = 1;
        final SchedulingStrategy initializationStrategy = ScheduleWhenAvailable.INSTANCE;
        initializeOperatorInstance( inputPortCount, outputPortCount, initializationStrategy );

        final TuplesImpl upstreamInput = TuplesImpl.newInstanceWithSinglePort( new Tuple( "f1", "val1" ) );
        final UpstreamContext upstreamContext = newUpstreamContextInstance( 0, inputPortCount, ACTIVE );
        final TuplesImpl output = operatorInstance.invoke( upstreamInput, upstreamContext );

        verify( queue ).offer( 0, singletonList( new Tuple( "f1", "val1" ) ) );
        verify( queue ).drain( drainer );
        verify( drainer ).reset();
        assertNoOperatorInvocation();
        assertNull( output );
        assertThat( operatorInstance.getSchedulingStrategy(), equalTo( initializationStrategy ) );
        assertThat( operatorInstance.getUpstreamContext(), equalTo( upstreamContext ) );
        assertNull( operatorInstance.getCompletionReason() );
        assertThat( operatorInstance.getStatus(), equalTo( RUNNING ) );
    }

}
