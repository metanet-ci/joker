package cs.bilkent.joker.engine.pipeline;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static cs.bilkent.joker.engine.pipeline.UpstreamContext.UpstreamConnectionStatus.CLOSED;
import static cs.bilkent.joker.engine.pipeline.UpstreamContext.UpstreamConnectionStatus.OPEN;
import static cs.bilkent.joker.engine.pipeline.UpstreamContext.newInitialUpstreamContext;
import static cs.bilkent.joker.engine.pipeline.UpstreamContext.newInitialUpstreamContextWithAllPortsConnected;
import static cs.bilkent.joker.engine.pipeline.UpstreamContext.newSourceOperatorInitialUpstreamContext;
import static cs.bilkent.joker.engine.pipeline.UpstreamContext.newSourceOperatorShutdownUpstreamContext;
import cs.bilkent.joker.operator.OperatorDef;
import cs.bilkent.joker.operator.scheduling.ScheduleWhenAvailable;
import static cs.bilkent.joker.operator.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByCount.AT_LEAST;
import static cs.bilkent.joker.operator.scheduling.ScheduleWhenTuplesAvailable.scheduleWhenTuplesAvailableOnAll;
import static cs.bilkent.joker.operator.scheduling.ScheduleWhenTuplesAvailable.scheduleWhenTuplesAvailableOnAny;
import cs.bilkent.joker.operator.scheduling.SchedulingStrategy;
import cs.bilkent.joker.test.AbstractJokerTest;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

@RunWith( MockitoJUnitRunner.class )
public class UpstreamContextTest extends AbstractJokerTest
{

    @Mock
    private OperatorDef operatorDef;

    private UpstreamContext upstreamContext;

    @Test
    public void shouldVerifyInitializableWhenScheduleWhenAvailableWithZeroInputPortOperator ()
    {
        upstreamContext = newSourceOperatorInitialUpstreamContext();

        upstreamContext.verifyInitializable( operatorDef, ScheduleWhenAvailable.INSTANCE );
    }

    @Test( expected = IllegalStateException.class )
    public void shouldNotVerifyInitializableWhenScheduleWhenAvailableWithNonZeroUpstreamContextVersion ()
    {
        upstreamContext = newSourceOperatorShutdownUpstreamContext();

        upstreamContext.verifyInitializable( operatorDef, ScheduleWhenAvailable.INSTANCE );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotVerifyInitializableWhenScheduleWhenAvailableWithNonZeroInputPortOperator ()
    {
        upstreamContext = newSourceOperatorInitialUpstreamContext();
        when( operatorDef.getInputPortCount() ).thenReturn( 1 );

        upstreamContext.verifyInitializable( operatorDef, ScheduleWhenAvailable.INSTANCE );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotVerifyInitializableWhenTuplesRequestedForZeroInputPortOperator ()
    {
        upstreamContext = newSourceOperatorInitialUpstreamContext();
        final SchedulingStrategy strategy = scheduleWhenTuplesAvailableOnAll( AT_LEAST, 2, 1, 0, 1 );

        upstreamContext.verifyInitializable( operatorDef, strategy );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotVerifyInitializableWhenTuplesRequestedForClosedPortInAllPorts ()
    {
        upstreamContext = newInitialUpstreamContext( OPEN, CLOSED );
        when( operatorDef.getInputPortCount() ).thenReturn( 2 );
        final SchedulingStrategy strategy = scheduleWhenTuplesAvailableOnAll( AT_LEAST, 2, 1, 0, 1 );

        upstreamContext.verifyInitializable( operatorDef, strategy );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotVerifyInitializableWhenTuplesRequestedForClosedPortInAnyPort ()
    {
        upstreamContext = newInitialUpstreamContext( OPEN, CLOSED );
        when( operatorDef.getInputPortCount() ).thenReturn( 2 );
        final SchedulingStrategy strategy = scheduleWhenTuplesAvailableOnAny( AT_LEAST, 2, 1, 0, 1 );

        upstreamContext.verifyInitializable( operatorDef, strategy );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotVerifyInitializableWhenNoTupleRequestedForOpenPortInAllPorts ()
    {
        upstreamContext = newInitialUpstreamContextWithAllPortsConnected( 2 );
        when( operatorDef.getInputPortCount() ).thenReturn( 2 );
        final SchedulingStrategy strategy = scheduleWhenTuplesAvailableOnAll( AT_LEAST, 2, 1, 0 );

        upstreamContext.verifyInitializable( operatorDef, strategy );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotVerifyInitializableWhenNoTupleRequestedForOpenPortInAnyPort ()
    {
        upstreamContext = newInitialUpstreamContextWithAllPortsConnected( 2 );
        when( operatorDef.getInputPortCount() ).thenReturn( 2 );
        final SchedulingStrategy strategy = scheduleWhenTuplesAvailableOnAny( AT_LEAST, 2, 1, 0 );

        upstreamContext.verifyInitializable( operatorDef, strategy );
    }

    @Test
    public void shouldVerifyInitializableWhenTuplesRequestedInAnyPortWhenAllPortsOpen ()
    {
        upstreamContext = newInitialUpstreamContextWithAllPortsConnected( 3 );
        when( operatorDef.getInputPortCount() ).thenReturn( 3 );
        final SchedulingStrategy strategy = scheduleWhenTuplesAvailableOnAny( AT_LEAST, 3, 1, 0, 1, 2 );

        upstreamContext.verifyInitializable( operatorDef, strategy );
    }

    @Test
    public void shouldVerifyInitializableWhenTuplesRequestedWhenAllPortsOpen ()
    {
        upstreamContext = newInitialUpstreamContextWithAllPortsConnected( 3 );
        when( operatorDef.getInputPortCount() ).thenReturn( 3 );
        final SchedulingStrategy strategy = scheduleWhenTuplesAvailableOnAll( AT_LEAST, 3, 1, 0, 1, 2 );

        upstreamContext.verifyInitializable( operatorDef, strategy );
    }

    @Test
    public void shouldVerifyInitializableWhenTuplesRequestedInAnyPortWhenSomePortsOpen ()
    {
        upstreamContext = newInitialUpstreamContext( OPEN, OPEN, CLOSED );
        when( operatorDef.getInputPortCount() ).thenReturn( 3 );
        final SchedulingStrategy strategy = scheduleWhenTuplesAvailableOnAny( AT_LEAST, 3, 1, 0, 1 );

        upstreamContext.verifyInitializable( operatorDef, strategy );
    }

    @Test
    public void shouldVerifyInitializableWhenTuplesRequestedWhenSomePortsOpen ()
    {
        upstreamContext = newInitialUpstreamContext( OPEN, OPEN, CLOSED );
        when( operatorDef.getInputPortCount() ).thenReturn( 3 );
        final SchedulingStrategy strategy = scheduleWhenTuplesAvailableOnAll( AT_LEAST, 3, 1, 0, 1 );

        upstreamContext.verifyInitializable( operatorDef, strategy );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotVerifyInitializableWhenOperatorHaveDifferentInputPortCount ()
    {
        upstreamContext = newInitialUpstreamContextWithAllPortsConnected( 2 );
        when( operatorDef.getInputPortCount() ).thenReturn( 1 );
        final SchedulingStrategy strategy = scheduleWhenTuplesAvailableOnAll( AT_LEAST, 2, 1, 0, 1 );

        upstreamContext.verifyInitializable( operatorDef, strategy );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotBeInvokableWhenOperatorHaveDifferentInputPortCount ()
    {
        upstreamContext = newInitialUpstreamContextWithAllPortsConnected( 2 );
        when( operatorDef.getInputPortCount() ).thenReturn( 1 );
        final SchedulingStrategy strategy = scheduleWhenTuplesAvailableOnAll( AT_LEAST, 2, 1, 0, 1 );

        upstreamContext.isInvokable( operatorDef, strategy );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotBeInvokableWhenScheduleWhenAvailableForNonZeroInputPortOperator ()
    {
        upstreamContext = newInitialUpstreamContextWithAllPortsConnected( 2 );
        when( operatorDef.getInputPortCount() ).thenReturn( 2 );

        upstreamContext.isInvokable( operatorDef, ScheduleWhenAvailable.INSTANCE );
    }

    @Test
    public void shouldBeInvokableWhenVersionIsZeroForScheduleWhenAvailableForZeroInputPortOperator ()
    {
        upstreamContext = newSourceOperatorInitialUpstreamContext();

        assertTrue( upstreamContext.isInvokable( operatorDef, ScheduleWhenAvailable.INSTANCE ) );
    }

    @Test
    public void shouldNotBeInvokableWhenVersionIsNonZeroForScheduleWhenAvailableForZeroInputPortOperator ()
    {
        upstreamContext = newSourceOperatorShutdownUpstreamContext();

        assertFalse( upstreamContext.isInvokable( operatorDef, ScheduleWhenAvailable.INSTANCE ) );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotBeInvokableWhenOperatorDefAndSchedulingStrategyHasDifferentInputPortCounts ()
    {
        upstreamContext = newInitialUpstreamContextWithAllPortsConnected( 1 );
        when( operatorDef.getInputPortCount() ).thenReturn( 1 );
        final SchedulingStrategy strategy = scheduleWhenTuplesAvailableOnAll( AT_LEAST, 2, 1, 0, 1 );

        upstreamContext.isInvokable( operatorDef, strategy );
    }

    @Test
    public void shouldBeInvokableWhenThereIsOpenPortForTuplesRequiredForAnyPort ()
    {
        upstreamContext = newInitialUpstreamContext( CLOSED, OPEN );
        when( operatorDef.getInputPortCount() ).thenReturn( 2 );
        final SchedulingStrategy strategy = scheduleWhenTuplesAvailableOnAny( AT_LEAST, 2, 1, 0, 1 );

        assertTrue( upstreamContext.isInvokable( operatorDef, strategy ) );
    }

    @Test
    public void shouldNotBeInvokableWhenThereIsNoOpenPortForTuplesRequiredForAnyPort ()
    {
        upstreamContext = newInitialUpstreamContext( CLOSED, CLOSED );
        when( operatorDef.getInputPortCount() ).thenReturn( 2 );
        final SchedulingStrategy strategy = scheduleWhenTuplesAvailableOnAny( AT_LEAST, 2, 1, 0, 1 );

        assertFalse( upstreamContext.isInvokable( operatorDef, strategy ) );
    }

    @Test
    public void shouldBeInvokableWhenAllRequiredPortsAreOpenForTuplesRequiredForAllPort ()
    {
        upstreamContext = newInitialUpstreamContext( CLOSED, OPEN, OPEN );
        when( operatorDef.getInputPortCount() ).thenReturn( 3 );
        final SchedulingStrategy strategy = scheduleWhenTuplesAvailableOnAll( AT_LEAST, 3, 1, 1, 2 );

        assertTrue( upstreamContext.isInvokable( operatorDef, strategy ) );
    }

    @Test
    public void shouldNotBeInvokableWhenAllRequiredPortsAreNotOpenForTuplesRequiredForAnyPort ()
    {
        upstreamContext = newInitialUpstreamContext( CLOSED, OPEN, CLOSED );
        when( operatorDef.getInputPortCount() ).thenReturn( 3 );
        final SchedulingStrategy strategy = scheduleWhenTuplesAvailableOnAll( AT_LEAST, 3, 1, 1, 2 );

        assertFalse( upstreamContext.isInvokable( operatorDef, strategy ) );
    }

}
