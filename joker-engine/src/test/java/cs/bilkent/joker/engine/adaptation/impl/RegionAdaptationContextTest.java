package cs.bilkent.joker.engine.adaptation.impl;

import java.util.List;
import java.util.function.BiPredicate;
import java.util.function.Predicate;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import cs.bilkent.joker.engine.adaptation.AdaptationAction;
import cs.bilkent.joker.engine.adaptation.BottleneckResolver;
import cs.bilkent.joker.engine.flow.PipelineId;
import cs.bilkent.joker.engine.flow.RegionDef;
import cs.bilkent.joker.engine.flow.RegionExecPlan;
import cs.bilkent.joker.engine.metric.PipelineMetrics;
import cs.bilkent.joker.test.AbstractJokerTest;
import cs.bilkent.joker.utils.Pair;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith( MockitoJUnitRunner.class )
public class RegionAdaptationContextTest extends AbstractJokerTest
{

    private final int regionId = 10;

    private final int pipelineStartIndex0 = 0;

    private final int pipelineStartIndex1 = 3;

    private final PipelineId pipelineId0 = new PipelineId( regionId, pipelineStartIndex0 );

    private final PipelineId pipelineId1 = new PipelineId( regionId, pipelineStartIndex1 );

    @Mock
    private RegionDef regionDef;

    @Mock
    private RegionExecPlan regionExecPlan;

    @Mock
    private RegionExecPlan newRegionExecPlan;

    @Mock
    private BiPredicate<PipelineMetrics, PipelineMetrics> loadChangePredicate;

    @Mock
    private Predicate<PipelineMetrics> bottleneckPredicate;

    @Mock
    private BottleneckResolver bottleneckResolver0;

    @Mock
    private BottleneckResolver bottleneckResolver1;

    @Mock
    private BiPredicate<PipelineMetrics, PipelineMetrics> adaptationEvaluationPredicate;

    public RegionAdaptationContext context;

    @Before
    public void init ()
    {
        when( regionDef.getRegionId() ).thenReturn( regionId );
        when( regionExecPlan.getRegionDef() ).thenReturn( regionDef );
        when( regionExecPlan.getPipelineIds() ).thenReturn( singletonList( pipelineId0 ) );
        when( newRegionExecPlan.getRegionDef() ).thenReturn( regionDef );

        context = new RegionAdaptationContext( regionExecPlan );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotUpdatePipelineMetricsWithInvalidPipelineId ()
    {
        final PipelineMetrics pipelineMetrics = mock( PipelineMetrics.class );
        when( pipelineMetrics.getPipelineId() ).thenReturn( pipelineId1 );

        context.updateRegionMetrics( singletonList( pipelineMetrics ), loadChangePredicate );
    }

    @Test
    public void shouldSetInitialPipelineMetrics ()
    {
        final PipelineMetrics pipelineMetrics = mock( PipelineMetrics.class );
        when( pipelineMetrics.getPipelineId() ).thenReturn( pipelineId0 );

        context.updateRegionMetrics( singletonList( pipelineMetrics ), loadChangePredicate );

        assertThat( context.getPipelineMetrics( pipelineId0 ), equalTo( pipelineMetrics ) );
    }

    @Test
    public void shouldUpdatePipelineMetrics ()
    {
        final PipelineMetrics pipelineMetrics1 = mock( PipelineMetrics.class );
        when( pipelineMetrics1.getPipelineId() ).thenReturn( pipelineId0 );

        final PipelineMetrics pipelineMetrics2 = mock( PipelineMetrics.class );
        when( pipelineMetrics2.getPipelineId() ).thenReturn( pipelineId0 );

        context.updateRegionMetrics( singletonList( pipelineMetrics1 ), loadChangePredicate );
        context.updateRegionMetrics( singletonList( pipelineMetrics2 ), loadChangePredicate );

        assertThat( context.getPipelineMetrics( pipelineId0 ), equalTo( pipelineMetrics2 ) );
    }

    @Test
    public void shouldNotResolveBottleneckForSourceRegion ()
    {
        when( regionDef.isSource() ).thenReturn( true );

        final List<AdaptationAction> actions = context.resolveIfBottleneck( bottleneckPredicate, singletonList( bottleneckResolver0 ) );

        assertTrue( actions.isEmpty() );
    }

    @Test
    public void shouldNotSetAdaptationStateWhenNoBottleneckPipeline ()
    {
        final PipelineMetrics pipelineMetrics1 = mock( PipelineMetrics.class );
        when( pipelineMetrics1.getPipelineId() ).thenReturn( pipelineId0 );

        context.updateRegionMetrics( singletonList( pipelineMetrics1 ), loadChangePredicate );

        final List<AdaptationAction> actions = context.resolveIfBottleneck( bottleneckPredicate, singletonList( bottleneckResolver0 ) );

        assertTrue( actions.isEmpty() );
        verify( bottleneckPredicate ).test( pipelineMetrics1 );
        assertNull( context.getBaseExecPlan() );
        assertTrue( context.getAdaptationActions().isEmpty() );
    }

    @Test
    public void shouldBlacklistNonResolvedBottleneckPipeline ()
    {
        final PipelineMetrics pipelineMetrics1 = mock( PipelineMetrics.class );
        when( pipelineMetrics1.getPipelineId() ).thenReturn( pipelineId0 );
        when( bottleneckPredicate.test( pipelineMetrics1 ) ).thenReturn( true );

        context.updateRegionMetrics( singletonList( pipelineMetrics1 ), loadChangePredicate );

        final List<AdaptationAction> actions = context.resolveIfBottleneck( bottleneckPredicate, singletonList( bottleneckResolver0 ) );

        assertTrue( actions.isEmpty() );
        verify( bottleneckPredicate ).test( pipelineMetrics1 );
        assertTrue( context.getAdaptationActions().isEmpty() );
        assertTrue( context.getAdaptingPipelineIds().isEmpty() );
        assertThat( context.getNonResolvablePipelineIds(), contains( pipelineId0 ) );
    }

    @Test
    public void shouldResolveBottleneckPipeline ()
    {
        final PipelineMetrics pipelineMetrics1 = mock( PipelineMetrics.class );
        when( pipelineMetrics1.getPipelineId() ).thenReturn( pipelineId0 );
        when( bottleneckPredicate.test( pipelineMetrics1 ) ).thenReturn( true );

        context.updateRegionMetrics( singletonList( pipelineMetrics1 ), loadChangePredicate );

        final AdaptationAction action = mock( AdaptationAction.class );
        final List<Pair<AdaptationAction, List<PipelineId>>> actions = singletonList( Pair.of( action, singletonList( pipelineId0 ) ) );
        when( bottleneckResolver0.resolve( regionExecPlan, singletonList( pipelineMetrics1 ) ) ).thenReturn( actions );
        when( action.getCurrentExecPlan() ).thenReturn( regionExecPlan );
        when( action.getNewExecPlan() ).thenReturn( newRegionExecPlan );

        final List<AdaptationAction> result = context.resolveIfBottleneck( bottleneckPredicate, singletonList( bottleneckResolver0 ) );

        assertThat( result, equalTo( singletonList( action ) ) );
        verify( bottleneckResolver0 ).resolve( regionExecPlan, singletonList( pipelineMetrics1 ) );
        assertThat( context.getAdaptationActions(), equalTo( singletonList( action ) ) );
        assertThat( context.getBaseExecPlan(), equalTo( regionExecPlan ) );
        assertThat( context.getCurrentExecPlan(), equalTo( newRegionExecPlan ) );
        assertThat( context.getAdaptingPipelineIds(), equalTo( singletonList( pipelineId0 ) ) );
    }

    @Test( expected = IllegalStateException.class )
    public void shouldNotResolveNewBottleneckPipelineWhenThereIsAlreadyAdaptation ()
    {
        try
        {
            final PipelineMetrics pipelineMetrics1 = mock( PipelineMetrics.class );
            when( pipelineMetrics1.getPipelineId() ).thenReturn( pipelineId0 );
            when( bottleneckPredicate.test( pipelineMetrics1 ) ).thenReturn( true );

            context.updateRegionMetrics( singletonList( pipelineMetrics1 ), loadChangePredicate );

            final AdaptationAction action = mock( AdaptationAction.class );
            final List<Pair<AdaptationAction, List<PipelineId>>> result = singletonList( Pair.of( action, singletonList( pipelineId0 ) ) );
            when( bottleneckResolver0.resolve( regionExecPlan, singletonList( pipelineMetrics1 ) ) ).thenReturn( result );
            when( action.getNewExecPlan() ).thenReturn( newRegionExecPlan );

            context.resolveIfBottleneck( bottleneckPredicate, singletonList( bottleneckResolver0 ) );
        }
        catch ( Exception e )
        {
            fail();
        }

        context.resolveIfBottleneck( bottleneckPredicate, singletonList( bottleneckResolver0 ) );
    }

    @Test( expected = IllegalStateException.class )
    public void shouldNotEvaluateWhenThereIsNoAdaptation ()
    {
        final PipelineMetrics pipelineMetrics1 = mock( PipelineMetrics.class );
        when( pipelineMetrics1.getPipelineId() ).thenReturn( pipelineId0 );

        context.isAdaptationSuccessful( singletonList( pipelineMetrics1 ), adaptationEvaluationPredicate );
    }

    @Test
    public void shouldSucceedAdaptation ()
    {
        final PipelineMetrics pipelineMetrics1 = mock( PipelineMetrics.class );
        when( pipelineMetrics1.getPipelineId() ).thenReturn( pipelineId0 );
        when( bottleneckPredicate.test( pipelineMetrics1 ) ).thenReturn( true );

        context.updateRegionMetrics( singletonList( pipelineMetrics1 ), loadChangePredicate );

        final AdaptationAction action = mock( AdaptationAction.class );
        final List<Pair<AdaptationAction, List<PipelineId>>> actions = singletonList( Pair.of( action, singletonList( pipelineId0 ) ) );
        when( bottleneckResolver0.resolve( regionExecPlan, singletonList( pipelineMetrics1 ) ) ).thenReturn( actions );
        when( action.getCurrentExecPlan() ).thenReturn( regionExecPlan );
        when( action.getNewExecPlan() ).thenReturn( newRegionExecPlan );
        when( newRegionExecPlan.getPipelineIds() ).thenReturn( asList( pipelineId0, pipelineId1 ) );

        context.resolveIfBottleneck( bottleneckPredicate, singletonList( bottleneckResolver0 ) );

        final PipelineMetrics pipelineMetrics2 = mock( PipelineMetrics.class );
        when( pipelineMetrics2.getPipelineId() ).thenReturn( pipelineId0 );
        when( adaptationEvaluationPredicate.test( pipelineMetrics1, pipelineMetrics2 ) ).thenReturn( true );

        final boolean success = context.isAdaptationSuccessful( singletonList( pipelineMetrics2 ), adaptationEvaluationPredicate );
        context.finalizeAdaptation( singletonList( pipelineMetrics2 ) );

        assertTrue( success );
        verify( adaptationEvaluationPredicate ).test( pipelineMetrics1, pipelineMetrics2 );
        assertTrue( context.getAdaptingPipelineIds().isEmpty() );
        assertTrue( context.getAdaptationActions().isEmpty() );
        assertNull( context.getBaseExecPlan() );
        assertThat( context.getCurrentExecPlan(), equalTo( newRegionExecPlan ) );
        assertThat( context.getPipelineMetrics( pipelineId0 ), equalTo( pipelineMetrics2 ) );
    }

    @Test
    public void shouldRevertAdaptationAfterEvaluation ()
    {
        reset( regionExecPlan );
        when( regionExecPlan.getRegionDef() ).thenReturn( regionDef );
        when( regionExecPlan.getPipelineIds() ).thenReturn( asList( pipelineId0, pipelineId1 ) );

        final PipelineMetrics pipelineMetrics1 = mock( PipelineMetrics.class );
        when( pipelineMetrics1.getPipelineId() ).thenReturn( pipelineId0 );
        when( bottleneckPredicate.test( pipelineMetrics1 ) ).thenReturn( true );

        final PipelineMetrics pipelineMetrics2 = mock( PipelineMetrics.class );
        when( pipelineMetrics2.getPipelineId() ).thenReturn( pipelineId1 );
        when( bottleneckPredicate.test( pipelineMetrics2 ) ).thenReturn( true );

        context.updateRegionMetrics( asList( pipelineMetrics1, pipelineMetrics2 ), loadChangePredicate );

        final RegionExecPlan newestRegionExecPlan = mock( RegionExecPlan.class );
        final AdaptationAction action1 = mock( AdaptationAction.class );
        final AdaptationAction action2 = mock( AdaptationAction.class );
        final List<Pair<AdaptationAction, List<PipelineId>>> actions = asList( Pair.of( action1, singletonList( pipelineId0 ) ),
                                                                               Pair.of( action2, singletonList( pipelineId1 ) ) );
        when( bottleneckResolver0.resolve( regionExecPlan, asList( pipelineMetrics1, pipelineMetrics2 ) ) ).thenReturn( actions );
        when( action1.getCurrentExecPlan() ).thenReturn( regionExecPlan );
        when( action1.getNewExecPlan() ).thenReturn( newRegionExecPlan );
        when( action2.getCurrentExecPlan() ).thenReturn( newRegionExecPlan );
        when( action2.getNewExecPlan() ).thenReturn( newestRegionExecPlan );
        when( newestRegionExecPlan.getPipelineIds() ).thenReturn( asList( pipelineId0, pipelineId1 ) );

        context.resolveIfBottleneck( bottleneckPredicate, singletonList( bottleneckResolver0 ) );

        final PipelineMetrics pipelineMetrics3 = mock( PipelineMetrics.class );
        when( pipelineMetrics3.getPipelineId() ).thenReturn( pipelineId0 );
        final PipelineMetrics pipelineMetrics4 = mock( PipelineMetrics.class );
        when( pipelineMetrics4.getPipelineId() ).thenReturn( pipelineId1 );

        final AdaptationAction revert1 = mock( AdaptationAction.class );
        when( action1.revert() ).thenReturn( revert1 );
        final AdaptationAction revert2 = mock( AdaptationAction.class );
        when( action2.revert() ).thenReturn( revert2 );

        final boolean success = context.isAdaptationSuccessful( singletonList( pipelineMetrics2 ), adaptationEvaluationPredicate );
        final List<AdaptationAction> result = context.revertAdaptation();

        assertFalse( success );
        assertThat( result, equalTo( asList( revert2, revert1 ) ) );
        assertTrue( context.getAdaptingPipelineIds().isEmpty() );
        assertTrue( context.getAdaptationActions().isEmpty() );
        assertNull( context.getBaseExecPlan() );
        assertThat( context.getCurrentExecPlan(), equalTo( regionExecPlan ) );
        assertThat( context.getBlacklist( pipelineId0 ), contains( action1 ) );
        assertThat( context.getBlacklist( pipelineId1 ), contains( action2 ) );
    }

    @Test( expected = IllegalStateException.class )
    public void shouldFailRevertIfNoRevertAction ()
    {
        final PipelineMetrics pipelineMetrics2 = mock( PipelineMetrics.class );
        when( pipelineMetrics2.getPipelineId() ).thenReturn( pipelineId0 );

        try
        {
            final PipelineMetrics pipelineMetrics1 = mock( PipelineMetrics.class );
            when( pipelineMetrics1.getPipelineId() ).thenReturn( pipelineId0 );
            when( bottleneckPredicate.test( pipelineMetrics1 ) ).thenReturn( true );

            context.updateRegionMetrics( singletonList( pipelineMetrics1 ), loadChangePredicate );

            final AdaptationAction action = mock( AdaptationAction.class );
            final List<Pair<AdaptationAction, List<PipelineId>>> actions = singletonList( Pair.of( action, singletonList( pipelineId0 ) ) );
            when( bottleneckResolver0.resolve( regionExecPlan, singletonList( pipelineMetrics1 ) ) ).thenReturn( actions );
            when( action.getCurrentExecPlan() ).thenReturn( regionExecPlan );
            when( action.getNewExecPlan() ).thenReturn( newRegionExecPlan );
            when( newRegionExecPlan.getPipelineIds() ).thenReturn( asList( pipelineId0, pipelineId1 ) );

            context.resolveIfBottleneck( bottleneckPredicate, singletonList( bottleneckResolver0 ) );
        }
        catch ( Exception e )
        {
            fail();
        }

        context.revertAdaptation();
    }

    @Test
    public void shouldProvideNewResolutionOnceCurrentAdaptationEvaluationFails ()
    {
        final PipelineMetrics pipelineMetrics1 = mock( PipelineMetrics.class );
        when( pipelineMetrics1.getPipelineId() ).thenReturn( pipelineId0 );
        when( bottleneckPredicate.test( pipelineMetrics1 ) ).thenReturn( true );

        context.updateRegionMetrics( singletonList( pipelineMetrics1 ), loadChangePredicate );

        final AdaptationAction action = mock( AdaptationAction.class );
        final List<Pair<AdaptationAction, List<PipelineId>>> actions = singletonList( Pair.of( action, singletonList( pipelineId0 ) ) );
        when( bottleneckResolver0.resolve( regionExecPlan, singletonList( pipelineMetrics1 ) ) ).thenReturn( actions );
        when( action.getCurrentExecPlan() ).thenReturn( regionExecPlan );
        when( action.getNewExecPlan() ).thenReturn( newRegionExecPlan );
        when( newRegionExecPlan.getPipelineIds() ).thenReturn( asList( pipelineId0, pipelineId1 ) );

        context.resolveIfBottleneck( bottleneckPredicate, singletonList( bottleneckResolver0 ) );

        final PipelineMetrics pipelineMetrics2 = mock( PipelineMetrics.class );
        when( pipelineMetrics2.getPipelineId() ).thenReturn( pipelineId0 );

        final AdaptationAction revert = mock( AdaptationAction.class );
        when( action.revert() ).thenReturn( revert );

        context.revertAdaptation();

        final AdaptationAction action2 = mock( AdaptationAction.class );
        final List<Pair<AdaptationAction, List<PipelineId>>> actions2 = singletonList( Pair.of( action2, singletonList( pipelineId0 ) ) );
        when( bottleneckResolver1.resolve( regionExecPlan, singletonList( pipelineMetrics1 ) ) ).thenReturn( actions2 );
        final RegionExecPlan newRegionExecPlan2 = mock( RegionExecPlan.class );
        when( action2.getCurrentExecPlan() ).thenReturn( regionExecPlan );
        when( action2.getNewExecPlan() ).thenReturn( newRegionExecPlan2 );
        when( newRegionExecPlan2.getPipelineIds() ).thenReturn( asList( pipelineId0, pipelineId1 ) );

        final List<AdaptationAction> result = context.resolveIfBottleneck( bottleneckPredicate,
                                                                           asList( bottleneckResolver0, bottleneckResolver1 ) );

        assertThat( result, equalTo( singletonList( action2 ) ) );
        verify( bottleneckResolver1 ).resolve( regionExecPlan, singletonList( pipelineMetrics1 ) );
        assertThat( context.getBaseExecPlan(), equalTo( regionExecPlan ) );
        assertThat( context.getCurrentExecPlan(), equalTo( newRegionExecPlan2 ) );
        assertThat( context.getAdaptingPipelineIds(), equalTo( singletonList( pipelineId0 ) ) );
        assertThat( context.getAdaptationActions(), equalTo( result ) );
    }

    @Test
    public void shouldGiveUpAdaptationOnBottleneckPipeline ()
    {
        final PipelineMetrics pipelineMetrics1 = mock( PipelineMetrics.class );
        when( pipelineMetrics1.getPipelineId() ).thenReturn( pipelineId0 );
        when( bottleneckPredicate.test( pipelineMetrics1 ) ).thenReturn( true );

        context.updateRegionMetrics( singletonList( pipelineMetrics1 ), loadChangePredicate );

        final AdaptationAction action = mock( AdaptationAction.class );
        final List<Pair<AdaptationAction, List<PipelineId>>> actions = singletonList( Pair.of( action, singletonList( pipelineId0 ) ) );
        when( bottleneckResolver0.resolve( regionExecPlan, singletonList( pipelineMetrics1 ) ) ).thenReturn( actions );
        when( action.getCurrentExecPlan() ).thenReturn( regionExecPlan );
        when( action.getNewExecPlan() ).thenReturn( newRegionExecPlan );
        when( newRegionExecPlan.getPipelineIds() ).thenReturn( asList( pipelineId0, pipelineId1 ) );

        context.resolveIfBottleneck( bottleneckPredicate, singletonList( bottleneckResolver0 ) );

        final PipelineMetrics pipelineMetrics2 = mock( PipelineMetrics.class );
        when( pipelineMetrics2.getPipelineId() ).thenReturn( pipelineId0 );

        final AdaptationAction revert = mock( AdaptationAction.class );
        when( action.revert() ).thenReturn( revert );

        context.revertAdaptation();

        final List<AdaptationAction> result = context.resolveIfBottleneck( bottleneckPredicate, singletonList( bottleneckResolver0 ) );

        assertTrue( result.isEmpty() );
        verify( bottleneckResolver0, times( 2 ) ).resolve( regionExecPlan, singletonList( pipelineMetrics1 ) );
        assertNull( context.getBaseExecPlan() );
        assertThat( context.getCurrentExecPlan(), equalTo( regionExecPlan ) );
        assertTrue( context.getAdaptingPipelineIds().isEmpty() );
        assertTrue( context.getAdaptationActions().isEmpty() );
        assertThat( context.getNonResolvablePipelineIds(), contains( pipelineId0 ) );
        assertThat( context.getBlacklist( pipelineId0 ), contains( action ) );
    }

    @Test
    public void shouldDiscardNonResolvablePipelineIdsOnLoadChange ()
    {
        final PipelineMetrics pipelineMetrics1 = mock( PipelineMetrics.class );
        when( pipelineMetrics1.getPipelineId() ).thenReturn( pipelineId0 );
        when( bottleneckPredicate.test( pipelineMetrics1 ) ).thenReturn( true );

        context.updateRegionMetrics( singletonList( pipelineMetrics1 ), loadChangePredicate );

        context.resolveIfBottleneck( bottleneckPredicate, singletonList( bottleneckResolver0 ) );

        assertThat( context.getNonResolvablePipelineIds(), contains( pipelineId0 ) );

        final PipelineMetrics pipelineMetrics2 = mock( PipelineMetrics.class );
        when( pipelineMetrics2.getPipelineId() ).thenReturn( pipelineId0 );
        when( loadChangePredicate.test( pipelineMetrics1, pipelineMetrics2 ) ).thenReturn( true );

        context.updateRegionMetrics( singletonList( pipelineMetrics2 ), loadChangePredicate );

        assertTrue( context.getNonResolvablePipelineIds().isEmpty() );
    }

    @Test
    public void shouldDiscardBlacklistOnLoadChange ()
    {
        final PipelineMetrics pipelineMetrics1 = mock( PipelineMetrics.class );
        when( pipelineMetrics1.getPipelineId() ).thenReturn( pipelineId0 );
        when( bottleneckPredicate.test( pipelineMetrics1 ) ).thenReturn( true );

        context.updateRegionMetrics( singletonList( pipelineMetrics1 ), loadChangePredicate );

        final AdaptationAction action = mock( AdaptationAction.class );
        final List<Pair<AdaptationAction, List<PipelineId>>> actions = singletonList( Pair.of( action, singletonList( pipelineId0 ) ) );
        when( bottleneckResolver0.resolve( regionExecPlan, singletonList( pipelineMetrics1 ) ) ).thenReturn( actions );
        when( action.getCurrentExecPlan() ).thenReturn( regionExecPlan );
        when( action.getNewExecPlan() ).thenReturn( newRegionExecPlan );
        when( newRegionExecPlan.getPipelineIds() ).thenReturn( asList( pipelineId0, pipelineId1 ) );

        context.resolveIfBottleneck( bottleneckPredicate, singletonList( bottleneckResolver0 ) );

        final PipelineMetrics pipelineMetrics2 = mock( PipelineMetrics.class );
        when( pipelineMetrics2.getPipelineId() ).thenReturn( pipelineId0 );

        final AdaptationAction revert = mock( AdaptationAction.class );
        when( action.revert() ).thenReturn( revert );

        context.isAdaptationSuccessful( singletonList( pipelineMetrics2 ), adaptationEvaluationPredicate );
        context.revertAdaptation();

        context.resolveIfBottleneck( bottleneckPredicate, singletonList( bottleneckResolver0 ) );

        assertThat( context.getBlacklist( pipelineId0 ), contains( action ) );

        final PipelineMetrics pipelineMetrics3 = mock( PipelineMetrics.class );
        when( pipelineMetrics3.getPipelineId() ).thenReturn( pipelineId0 );
        when( loadChangePredicate.test( pipelineMetrics1, pipelineMetrics3 ) ).thenReturn( true );

        context.updateRegionMetrics( singletonList( pipelineMetrics3 ), loadChangePredicate );

        assertTrue( context.getBlacklist( pipelineId0 ).isEmpty() );
    }

}
