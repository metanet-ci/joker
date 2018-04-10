package cs.bilkent.joker.engine.pipeline;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static cs.bilkent.joker.engine.pipeline.UpstreamCtx.ConnectionStatus.CLOSED;
import static cs.bilkent.joker.engine.pipeline.UpstreamCtx.ConnectionStatus.OPEN;
import static cs.bilkent.joker.engine.pipeline.UpstreamCtx.createInitialClosedUpstreamCtx;
import static cs.bilkent.joker.engine.pipeline.UpstreamCtx.createSourceOperatorInitialUpstreamCtx;
import static cs.bilkent.joker.engine.pipeline.UpstreamCtx.createSourceOperatorShutdownUpstreamCtx;
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
public class UpstreamCtxTest extends AbstractJokerTest
{

    @Mock
    private OperatorDef operatorDef;

    private UpstreamCtx upstreamCtx;

    @Test
    public void shouldVerifyInitializableWhenScheduleWhenAvailableWithZeroInputPortOperator ()
    {
        upstreamCtx = createSourceOperatorInitialUpstreamCtx();

        upstreamCtx.verifyInitializable( operatorDef, ScheduleWhenAvailable.INSTANCE );
    }

    @Test( expected = IllegalStateException.class )
    public void shouldNotVerifyInitializableWhenScheduleWhenAvailableWithNonZeroUpstreamContextVersion ()
    {
        upstreamCtx = createSourceOperatorShutdownUpstreamCtx();

        upstreamCtx.verifyInitializable( operatorDef, ScheduleWhenAvailable.INSTANCE );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotVerifyInitializableWhenScheduleWhenAvailableWithNonZeroInputPortOperator ()
    {
        upstreamCtx = createSourceOperatorInitialUpstreamCtx();
        when( operatorDef.getInputPortCount() ).thenReturn( 1 );

        upstreamCtx.verifyInitializable( operatorDef, ScheduleWhenAvailable.INSTANCE );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotVerifyInitializableWhenTuplesRequestedForZeroInputPortOperator ()
    {
        upstreamCtx = createSourceOperatorInitialUpstreamCtx();
        final SchedulingStrategy strategy = scheduleWhenTuplesAvailableOnAll( AT_LEAST, 2, 1, 0, 1 );

        upstreamCtx.verifyInitializable( operatorDef, strategy );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotVerifyInitializableWhenTuplesRequestedForClosedPortInAllPorts ()
    {
        upstreamCtx = UpstreamCtx.createInitialUpstreamCtx( OPEN, CLOSED );
        when( operatorDef.getInputPortCount() ).thenReturn( 2 );
        final SchedulingStrategy strategy = scheduleWhenTuplesAvailableOnAll( AT_LEAST, 2, 1, 0, 1 );

        upstreamCtx.verifyInitializable( operatorDef, strategy );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotVerifyInitializableWhenTuplesRequestedForClosedPortInAnyPort ()
    {
        upstreamCtx = UpstreamCtx.createInitialUpstreamCtx( OPEN, CLOSED );
        when( operatorDef.getInputPortCount() ).thenReturn( 2 );
        final SchedulingStrategy strategy = scheduleWhenTuplesAvailableOnAny( AT_LEAST, 2, 1, 0, 1 );

        upstreamCtx.verifyInitializable( operatorDef, strategy );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotVerifyInitializableWhenNoTupleRequestedForOpenPortInAllPorts ()
    {
        upstreamCtx = createInitialClosedUpstreamCtx( 2 );
        when( operatorDef.getInputPortCount() ).thenReturn( 2 );
        final SchedulingStrategy strategy = scheduleWhenTuplesAvailableOnAll( AT_LEAST, 2, 1, 0 );

        upstreamCtx.verifyInitializable( operatorDef, strategy );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotVerifyInitializableWhenNoTupleRequestedForOpenPortInAnyPort ()
    {
        upstreamCtx = createInitialClosedUpstreamCtx( 2 );
        when( operatorDef.getInputPortCount() ).thenReturn( 2 );
        final SchedulingStrategy strategy = scheduleWhenTuplesAvailableOnAny( AT_LEAST, 2, 1, 0 );

        upstreamCtx.verifyInitializable( operatorDef, strategy );
    }

    @Test
    public void shouldVerifyInitializableWhenTuplesRequestedInAnyPortWhenAllPortsOpen ()
    {
        upstreamCtx = createInitialClosedUpstreamCtx( 3 );
        when( operatorDef.getInputPortCount() ).thenReturn( 3 );
        final SchedulingStrategy strategy = scheduleWhenTuplesAvailableOnAny( AT_LEAST, 3, 1, 0, 1, 2 );

        upstreamCtx.verifyInitializable( operatorDef, strategy );
    }

    @Test
    public void shouldVerifyInitializableWhenTuplesRequestedWhenAllPortsOpen ()
    {
        upstreamCtx = createInitialClosedUpstreamCtx( 3 );
        when( operatorDef.getInputPortCount() ).thenReturn( 3 );
        final SchedulingStrategy strategy = scheduleWhenTuplesAvailableOnAll( AT_LEAST, 3, 1, 0, 1, 2 );

        upstreamCtx.verifyInitializable( operatorDef, strategy );
    }

    @Test
    public void shouldVerifyInitializableWhenTuplesRequestedInAnyPortWhenSomePortsOpen ()
    {
        upstreamCtx = UpstreamCtx.createInitialUpstreamCtx( OPEN, OPEN, CLOSED );
        when( operatorDef.getInputPortCount() ).thenReturn( 3 );
        final SchedulingStrategy strategy = scheduleWhenTuplesAvailableOnAny( AT_LEAST, 3, 1, 0, 1 );

        upstreamCtx.verifyInitializable( operatorDef, strategy );
    }

    @Test
    public void shouldVerifyInitializableWhenTuplesRequestedWhenSomePortsOpen ()
    {
        upstreamCtx = UpstreamCtx.createInitialUpstreamCtx( OPEN, OPEN, CLOSED );
        when( operatorDef.getInputPortCount() ).thenReturn( 3 );
        final SchedulingStrategy strategy = scheduleWhenTuplesAvailableOnAll( AT_LEAST, 3, 1, 0, 1 );

        upstreamCtx.verifyInitializable( operatorDef, strategy );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotVerifyInitializableWhenOperatorHaveDifferentInputPortCount ()
    {
        upstreamCtx = createInitialClosedUpstreamCtx( 2 );
        when( operatorDef.getInputPortCount() ).thenReturn( 1 );
        final SchedulingStrategy strategy = scheduleWhenTuplesAvailableOnAll( AT_LEAST, 2, 1, 0, 1 );

        upstreamCtx.verifyInitializable( operatorDef, strategy );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotBeInvokableWhenOperatorHaveDifferentInputPortCount ()
    {
        upstreamCtx = createInitialClosedUpstreamCtx( 2 );
        when( operatorDef.getInputPortCount() ).thenReturn( 1 );
        final SchedulingStrategy strategy = scheduleWhenTuplesAvailableOnAll( AT_LEAST, 2, 1, 0, 1 );

        upstreamCtx.isInvokable( operatorDef, strategy );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotBeInvokableWhenScheduleWhenAvailableForNonZeroInputPortOperator ()
    {
        upstreamCtx = createInitialClosedUpstreamCtx( 2 );
        when( operatorDef.getInputPortCount() ).thenReturn( 2 );

        upstreamCtx.isInvokable( operatorDef, ScheduleWhenAvailable.INSTANCE );
    }

    @Test
    public void shouldBeInvokableWhenVersionIsZeroForScheduleWhenAvailableForZeroInputPortOperator ()
    {
        upstreamCtx = createSourceOperatorInitialUpstreamCtx();

        assertTrue( upstreamCtx.isInvokable( operatorDef, ScheduleWhenAvailable.INSTANCE ) );
    }

    @Test
    public void shouldNotBeInvokableWhenVersionIsNonZeroForScheduleWhenAvailableForZeroInputPortOperator ()
    {
        upstreamCtx = createSourceOperatorShutdownUpstreamCtx();

        assertFalse( upstreamCtx.isInvokable( operatorDef, ScheduleWhenAvailable.INSTANCE ) );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotBeInvokableWhenOperatorDefAndSchedulingStrategyHasDifferentInputPortCounts ()
    {
        upstreamCtx = createInitialClosedUpstreamCtx( 1 );
        when( operatorDef.getInputPortCount() ).thenReturn( 1 );
        final SchedulingStrategy strategy = scheduleWhenTuplesAvailableOnAll( AT_LEAST, 2, 1, 0, 1 );

        upstreamCtx.isInvokable( operatorDef, strategy );
    }

    @Test
    public void shouldBeInvokableWhenThereIsOpenPortForTuplesRequiredForAnyPort ()
    {
        upstreamCtx = UpstreamCtx.createInitialUpstreamCtx( CLOSED, OPEN );
        when( operatorDef.getInputPortCount() ).thenReturn( 2 );
        final SchedulingStrategy strategy = scheduleWhenTuplesAvailableOnAny( AT_LEAST, 2, 1, 0, 1 );

        assertTrue( upstreamCtx.isInvokable( operatorDef, strategy ) );
    }

    @Test
    public void shouldNotBeInvokableWhenThereIsNoOpenPortForTuplesRequiredForAnyPort ()
    {
        upstreamCtx = UpstreamCtx.createInitialUpstreamCtx( CLOSED, CLOSED );
        when( operatorDef.getInputPortCount() ).thenReturn( 2 );
        final SchedulingStrategy strategy = scheduleWhenTuplesAvailableOnAny( AT_LEAST, 2, 1, 0, 1 );

        assertFalse( upstreamCtx.isInvokable( operatorDef, strategy ) );
    }

    @Test
    public void shouldBeInvokableWhenAllRequiredPortsAreOpenForTuplesRequiredForAllPort ()
    {
        upstreamCtx = UpstreamCtx.createInitialUpstreamCtx( CLOSED, OPEN, OPEN );
        when( operatorDef.getInputPortCount() ).thenReturn( 3 );
        final SchedulingStrategy strategy = scheduleWhenTuplesAvailableOnAll( AT_LEAST, 3, 1, 1, 2 );

        assertTrue( upstreamCtx.isInvokable( operatorDef, strategy ) );
    }

    @Test
    public void shouldNotBeInvokableWhenAllRequiredPortsAreNotOpenForTuplesRequiredForAnyPort ()
    {
        upstreamCtx = UpstreamCtx.createInitialUpstreamCtx( CLOSED, OPEN, CLOSED );
        when( operatorDef.getInputPortCount() ).thenReturn( 3 );
        final SchedulingStrategy strategy = scheduleWhenTuplesAvailableOnAll( AT_LEAST, 3, 1, 1, 2 );

        assertFalse( upstreamCtx.isInvokable( operatorDef, strategy ) );
    }

}
