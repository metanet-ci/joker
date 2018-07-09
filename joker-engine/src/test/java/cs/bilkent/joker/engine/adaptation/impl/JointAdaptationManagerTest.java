package cs.bilkent.joker.engine.adaptation.impl;

import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import cs.bilkent.joker.engine.adaptation.AdaptationAction;
import cs.bilkent.joker.engine.adaptation.AdaptationManager;
import cs.bilkent.joker.engine.flow.RegionExecPlan;
import cs.bilkent.joker.engine.metric.FlowMetrics;
import cs.bilkent.joker.test.AbstractJokerTest;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith( MockitoJUnitRunner.class )
public class JointAdaptationManagerTest extends AbstractJokerTest
{

    @Mock
    private AdaptationManager throughputAdaptationManager;

    @Mock
    private AdaptationManager latencyAdaptationManager;

    @Mock
    private RegionExecPlan execPlan;

    @Mock
    private FlowMetrics metrics;

    @Mock
    private AdaptationAction action;

    private JointAdaptationManager jointAdaptationManager;

    @Before
    public void init ()
    {
        jointAdaptationManager = new JointAdaptationManager( throughputAdaptationManager, latencyAdaptationManager );
    }

    @Test
    public void when_throughputOptimizerReturnsAction_then_latencyOptimizerNotCalled ()
    {
        when( throughputAdaptationManager.adapt( singletonList( execPlan ), metrics ) ).thenReturn( singletonList( action ) );

        final List<AdaptationAction> actions = jointAdaptationManager.adapt( singletonList( execPlan ), metrics );

        verify( throughputAdaptationManager ).adapt( singletonList( execPlan ), metrics );
        verify( latencyAdaptationManager, never() ).adapt( singletonList( execPlan ), metrics );
        assertThat( actions, equalTo( singletonList( action ) ) );
        assertThat( jointAdaptationManager.isOptimizingThroughput(), equalTo( true ) );
    }

    @Test
    public void when_throughputOptimizationIsOngoing_then_latencyOptimizerNotCalled ()
    {
        when( throughputAdaptationManager.adapt( singletonList( execPlan ), metrics ) ).thenReturn( singletonList( action ),
                                                                                                    singletonList( action ) );

        jointAdaptationManager.adapt( singletonList( execPlan ), metrics );
        final List<AdaptationAction> actions = jointAdaptationManager.adapt( singletonList( execPlan ), metrics );

        verify( throughputAdaptationManager, times( 2 ) ).adapt( singletonList( execPlan ), metrics );
        verify( latencyAdaptationManager, never() ).adapt( singletonList( execPlan ), metrics );
        assertThat( actions, equalTo( singletonList( action ) ) );
        assertThat( jointAdaptationManager.isOptimizingThroughput(), equalTo( true ) );
    }

    @Test
    public void when_throughputOptimizationJustCompletes_then_latencyOptimizerNotCalled ()
    {
        when( throughputAdaptationManager.adapt( singletonList( execPlan ), metrics ) ).thenReturn( singletonList( action ), emptyList() );

        jointAdaptationManager.adapt( singletonList( execPlan ), metrics );
        final List<AdaptationAction> actions = jointAdaptationManager.adapt( singletonList( execPlan ), metrics );

        verify( throughputAdaptationManager, times( 2 ) ).adapt( singletonList( execPlan ), metrics );
        verify( latencyAdaptationManager, never() ).adapt( singletonList( execPlan ), metrics );
        assertThat( actions, hasSize( 0 ) );
        assertThat( jointAdaptationManager.isOptimizingThroughput(), equalTo( true ) );
    }

    @Test
    public void when_throughputOptimizerReturnsNoAction_then_latencyOptimizerCalled ()
    {
        when( throughputAdaptationManager.adapt( singletonList( execPlan ), metrics ) ).thenReturn( emptyList() );

        final List<AdaptationAction> actions = jointAdaptationManager.adapt( singletonList( execPlan ), metrics );

        verify( throughputAdaptationManager ).adapt( singletonList( execPlan ), metrics );
        verify( latencyAdaptationManager ).adapt( singletonList( execPlan ), metrics );
        assertThat( actions, equalTo( emptyList() ) );
        assertThat( jointAdaptationManager.isOptimizingThroughput(), equalTo( true ) );
    }

    @Test
    public void when_latencyOptimizerReturnAction_then_movesToLatencyOptimizationStage ()
    {
        when( throughputAdaptationManager.adapt( singletonList( execPlan ), metrics ) ).thenReturn( emptyList() );
        when( latencyAdaptationManager.adapt( singletonList( execPlan ), metrics ) ).thenReturn( singletonList( action ) );

        final List<AdaptationAction> actions = jointAdaptationManager.adapt( singletonList( execPlan ), metrics );

        verify( throughputAdaptationManager ).adapt( singletonList( execPlan ), metrics );
        verify( latencyAdaptationManager ).adapt( singletonList( execPlan ), metrics );
        assertThat( actions, equalTo( singletonList( action ) ) );
        assertThat( jointAdaptationManager.isOptimizingThroughput(), equalTo( false ) );
    }

    @Test
    public void when_latencyOptimizationStarted_then_throughputOptimizerNotCalled ()
    {
        when( throughputAdaptationManager.adapt( singletonList( execPlan ), metrics ) ).thenReturn( emptyList() );
        when( latencyAdaptationManager.adapt( singletonList( execPlan ), metrics ) ).thenReturn( singletonList( action ), emptyList() );

        jointAdaptationManager.adapt( singletonList( execPlan ), metrics );
        jointAdaptationManager.adapt( singletonList( execPlan ), metrics );

        verify( throughputAdaptationManager ).adapt( singletonList( execPlan ), metrics );
        verify( latencyAdaptationManager, times( 2 ) ).adapt( singletonList( execPlan ), metrics );
        assertThat( jointAdaptationManager.isOptimizingThroughput(), equalTo( true ) );
    }

    @Test
    public void when_latencyOptimizationIsOngoing_then_throughputOptimizerNotCalled ()
    {
        when( throughputAdaptationManager.adapt( singletonList( execPlan ), metrics ) ).thenReturn( emptyList() );
        when( latencyAdaptationManager.adapt( singletonList( execPlan ), metrics ) ).thenReturn( singletonList( action ),
                                                                                                 singletonList( action ) );

        jointAdaptationManager.adapt( singletonList( execPlan ), metrics );
        jointAdaptationManager.adapt( singletonList( execPlan ), metrics );

        verify( throughputAdaptationManager ).adapt( singletonList( execPlan ), metrics );
        verify( latencyAdaptationManager, times( 2 ) ).adapt( singletonList( execPlan ), metrics );
        assertThat( jointAdaptationManager.isOptimizingThroughput(), equalTo( false ) );
    }

}
