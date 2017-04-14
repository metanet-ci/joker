package cs.bilkent.joker.engine.adaptation.impl;

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
import cs.bilkent.joker.engine.flow.RegionExecutionPlan;
import cs.bilkent.joker.engine.metric.PipelineMetricsSnapshot;
import cs.bilkent.joker.test.AbstractJokerTest;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
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
    private RegionExecutionPlan regionExecutionPlan;

    @Mock
    private RegionExecutionPlan newRegionExecutionPlan;

    @Mock
    private BiPredicate<PipelineMetricsSnapshot, PipelineMetricsSnapshot> loadChangePredicate;

    @Mock
    private Predicate<PipelineMetricsSnapshot> bottleneckPredicate;

    @Mock
    private BottleneckResolver bottleneckResolver0;

    @Mock
    private BottleneckResolver bottleneckResolver1;

    @Mock
    private BiPredicate<PipelineMetricsSnapshot, PipelineMetricsSnapshot> adaptationEvaluationPredicate;

    public RegionAdaptationContext context;

    @Before
    public void init ()
    {
        when( regionDef.getRegionId() ).thenReturn( regionId );
        when( regionExecutionPlan.getRegionDef() ).thenReturn( regionDef );
        when( regionExecutionPlan.getPipelineIds() ).thenReturn( singletonList( pipelineId0 ) );
        when( newRegionExecutionPlan.getRegionDef() ).thenReturn( regionDef );

        context = new RegionAdaptationContext( regionExecutionPlan );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotPipelineMetricsWithInvalidPipelineId ()
    {
        final PipelineMetricsSnapshot pipelineMetrics = mock( PipelineMetricsSnapshot.class );
        when( pipelineMetrics.getPipelineId() ).thenReturn( pipelineId1 );

        context.updateRegionMetrics( regionExecutionPlan, singletonList( pipelineMetrics ), loadChangePredicate );
    }

    @Test
    public void shouldSetInitialPipelineMetrics ()
    {
        final PipelineMetricsSnapshot pipelineMetrics = mock( PipelineMetricsSnapshot.class );
        when( pipelineMetrics.getPipelineId() ).thenReturn( pipelineId0 );

        context.updateRegionMetrics( regionExecutionPlan, singletonList( pipelineMetrics ), loadChangePredicate );

        assertThat( context.getPipelineMetrics( pipelineId0 ), equalTo( pipelineMetrics ) );
    }

    @Test
    public void shouldUpdatePipelineMetrics ()
    {
        final PipelineMetricsSnapshot pipelineMetrics1 = mock( PipelineMetricsSnapshot.class );
        when( pipelineMetrics1.getPipelineId() ).thenReturn( pipelineId0 );

        final PipelineMetricsSnapshot pipelineMetrics2 = mock( PipelineMetricsSnapshot.class );
        when( pipelineMetrics2.getPipelineId() ).thenReturn( pipelineId0 );

        context.updateRegionMetrics( regionExecutionPlan, singletonList( pipelineMetrics1 ), loadChangePredicate );

        context.updateRegionMetrics( regionExecutionPlan, singletonList( pipelineMetrics2 ), loadChangePredicate );

        assertThat( context.getPipelineMetrics( pipelineId0 ), equalTo( pipelineMetrics2 ) );
    }

    @Test
    public void shouldUpdatePipelineMetricsWithNewExecutionPlan ()
    {
        final PipelineMetricsSnapshot pipelineMetrics1 = mock( PipelineMetricsSnapshot.class );
        when( pipelineMetrics1.getPipelineId() ).thenReturn( pipelineId0 );

        final PipelineMetricsSnapshot pipelineMetrics2 = mock( PipelineMetricsSnapshot.class );
        when( pipelineMetrics2.getPipelineId() ).thenReturn( pipelineId1 );

        when( newRegionExecutionPlan.getPipelineIds() ).thenReturn( asList( pipelineId0, pipelineId1 ) );

        context.updateRegionMetrics( newRegionExecutionPlan, asList( pipelineMetrics1, pipelineMetrics2 ), loadChangePredicate );

        assertThat( context.getPipelineMetrics( pipelineId0 ), equalTo( pipelineMetrics1 ) );
        assertThat( context.getPipelineMetrics( pipelineId1 ), equalTo( pipelineMetrics2 ) );
        assertThat( context.getCurrentExecutionPlan(), equalTo( newRegionExecutionPlan ) );
    }

    @Test
    public void shouldDeleteObsoletePipelineMetricsWithNewExecutionPlan ()
    {
        final PipelineMetricsSnapshot pipelineMetrics1 = mock( PipelineMetricsSnapshot.class );
        when( pipelineMetrics1.getPipelineId() ).thenReturn( pipelineId0 );

        final PipelineMetricsSnapshot pipelineMetrics2 = mock( PipelineMetricsSnapshot.class );
        when( pipelineMetrics2.getPipelineId() ).thenReturn( pipelineId1 );

        when( newRegionExecutionPlan.getPipelineIds() ).thenReturn( asList( pipelineId0, pipelineId1 ) );

        context.updateRegionMetrics( newRegionExecutionPlan, asList( pipelineMetrics1, pipelineMetrics2 ), loadChangePredicate );

        context.updateRegionMetrics( regionExecutionPlan, singletonList( pipelineMetrics1 ), loadChangePredicate );

        assertThat( context.getPipelineMetrics( pipelineId0 ), equalTo( pipelineMetrics1 ) );
        assertThat( context.getCurrentExecutionPlan(), equalTo( regionExecutionPlan ) );
    }

    @Test
    public void shouldNotResolveBottleneckForSourceRegion ()
    {
        when( regionDef.isSource() ).thenReturn( true );

        final AdaptationAction action = context.resolveIfBottleneck( regionExecutionPlan,
                                                                     bottleneckPredicate,
                                                                     singletonList( bottleneckResolver0 ) );

        assertNull( action );
    }

    @Test
    public void shouldNotSetAdaptationStateWhenNoBottleneckPipeline ()
    {
        final PipelineMetricsSnapshot pipelineMetrics1 = mock( PipelineMetricsSnapshot.class );
        when( pipelineMetrics1.getPipelineId() ).thenReturn( pipelineId0 );

        context.updateRegionMetrics( regionExecutionPlan, singletonList( pipelineMetrics1 ), loadChangePredicate );

        final AdaptationAction action = context.resolveIfBottleneck( regionExecutionPlan,
                                                                     bottleneckPredicate,
                                                                     singletonList( bottleneckResolver0 ) );

        assertNull( action );
        verify( bottleneckPredicate ).test( pipelineMetrics1 );
        assertNull( context.getAdaptationAction() );
        assertNull( context.getAdaptingPipelineId() );
    }

    @Test
    public void shouldBlacklistNonResolvedBottleneckPipeline ()
    {
        final PipelineMetricsSnapshot pipelineMetrics1 = mock( PipelineMetricsSnapshot.class );
        when( pipelineMetrics1.getPipelineId() ).thenReturn( pipelineId0 );
        when( bottleneckPredicate.test( pipelineMetrics1 ) ).thenReturn( true );

        context.updateRegionMetrics( regionExecutionPlan, singletonList( pipelineMetrics1 ), loadChangePredicate );

        final AdaptationAction action = context.resolveIfBottleneck( regionExecutionPlan,
                                                                     bottleneckPredicate,
                                                                     singletonList( bottleneckResolver0 ) );

        assertNull( action );
        verify( bottleneckPredicate ).test( pipelineMetrics1 );
        assertNull( context.getAdaptationAction() );
        assertNull( context.getAdaptingPipelineId() );
        assertThat( context.getNonResolvablePipelineIds(), contains( pipelineId0 ) );
    }

    @Test
    public void shouldResolveBottleneckPipeline ()
    {
        final PipelineMetricsSnapshot pipelineMetrics1 = mock( PipelineMetricsSnapshot.class );
        when( pipelineMetrics1.getPipelineId() ).thenReturn( pipelineId0 );
        when( bottleneckPredicate.test( pipelineMetrics1 ) ).thenReturn( true );

        context.updateRegionMetrics( regionExecutionPlan, singletonList( pipelineMetrics1 ), loadChangePredicate );

        final AdaptationAction action = mock( AdaptationAction.class );
        when( bottleneckResolver0.resolve( regionExecutionPlan, pipelineMetrics1 ) ).thenReturn( action );
        when( action.getNewRegionExecutionPlan() ).thenReturn( newRegionExecutionPlan );

        final AdaptationAction result = context.resolveIfBottleneck( regionExecutionPlan,
                                                                     bottleneckPredicate,
                                                                     singletonList( bottleneckResolver0 ) );

        assertThat( result, equalTo( action ) );
        verify( bottleneckResolver0 ).resolve( regionExecutionPlan, pipelineMetrics1 );
        assertThat( context.getAdaptationAction(), equalTo( action ) );
        assertThat( context.getBaseExecutionPlan(), equalTo( regionExecutionPlan ) );
        assertThat( context.getCurrentExecutionPlan(), equalTo( newRegionExecutionPlan ) );
        assertThat( context.getAdaptingPipelineId(), equalTo( pipelineId0 ) );
    }

    @Test( expected = IllegalStateException.class )
    public void shouldNotResolveNewBottleneckPipelineWhenThereIsAlreadyAdaptation ()
    {
        try
        {
            final PipelineMetricsSnapshot pipelineMetrics1 = mock( PipelineMetricsSnapshot.class );
            when( pipelineMetrics1.getPipelineId() ).thenReturn( pipelineId0 );
            when( bottleneckPredicate.test( pipelineMetrics1 ) ).thenReturn( true );

            context.updateRegionMetrics( regionExecutionPlan, singletonList( pipelineMetrics1 ), loadChangePredicate );

            final AdaptationAction action = mock( AdaptationAction.class );
            when( bottleneckResolver0.resolve( regionExecutionPlan, pipelineMetrics1 ) ).thenReturn( action );
            when( action.getNewRegionExecutionPlan() ).thenReturn( newRegionExecutionPlan );

            context.resolveIfBottleneck( regionExecutionPlan, bottleneckPredicate, singletonList( bottleneckResolver0 ) );
        }
        catch ( Exception e )
        {
            fail();
        }

        context.resolveIfBottleneck( regionExecutionPlan, bottleneckPredicate, singletonList( bottleneckResolver0 ) );
    }

    @Test( expected = IllegalStateException.class )
    public void shouldNotEvaluateWhenThereIsNoAdaptation ()
    {
        final PipelineMetricsSnapshot pipelineMetrics1 = mock( PipelineMetricsSnapshot.class );
        when( pipelineMetrics1.getPipelineId() ).thenReturn( pipelineId0 );

        context.evaluateAdaptation( regionExecutionPlan, pipelineMetrics1, adaptationEvaluationPredicate );
    }

    @Test
    public void shouldSucceedAdaptation ()
    {
        final PipelineMetricsSnapshot pipelineMetrics1 = mock( PipelineMetricsSnapshot.class );
        when( pipelineMetrics1.getPipelineId() ).thenReturn( pipelineId0 );
        when( bottleneckPredicate.test( pipelineMetrics1 ) ).thenReturn( true );

        context.updateRegionMetrics( regionExecutionPlan, singletonList( pipelineMetrics1 ), loadChangePredicate );

        final AdaptationAction action = mock( AdaptationAction.class );
        when( bottleneckResolver0.resolve( regionExecutionPlan, pipelineMetrics1 ) ).thenReturn( action );
        when( action.getNewRegionExecutionPlan() ).thenReturn( newRegionExecutionPlan );
        when( newRegionExecutionPlan.getPipelineIds() ).thenReturn( asList( pipelineId0, pipelineId1 ) );

        context.resolveIfBottleneck( regionExecutionPlan, bottleneckPredicate, singletonList( bottleneckResolver0 ) );

        final PipelineMetricsSnapshot pipelineMetrics2 = mock( PipelineMetricsSnapshot.class );
        when( pipelineMetrics2.getPipelineId() ).thenReturn( pipelineId0 );
        when( adaptationEvaluationPredicate.test( pipelineMetrics1, pipelineMetrics2 ) ).thenReturn( true );

        final AdaptationAction rollback = context.evaluateAdaptation( newRegionExecutionPlan,
                                                                      pipelineMetrics2,
                                                                      adaptationEvaluationPredicate );

        assertNull( rollback );
        verify( adaptationEvaluationPredicate ).test( pipelineMetrics1, pipelineMetrics2 );
        assertNull( context.getAdaptingPipelineId() );
        assertNull( context.getAdaptationAction() );
        assertNull( context.getBaseExecutionPlan() );
        assertThat( context.getCurrentExecutionPlan(), equalTo( newRegionExecutionPlan ) );
        assertThat( context.getPipelineMetrics( pipelineId0 ), equalTo( pipelineMetrics2 ) );
    }

    @Test
    public void shouldRollbackAdaptation ()
    {
        final PipelineMetricsSnapshot pipelineMetrics1 = mock( PipelineMetricsSnapshot.class );
        when( pipelineMetrics1.getPipelineId() ).thenReturn( pipelineId0 );
        when( bottleneckPredicate.test( pipelineMetrics1 ) ).thenReturn( true );

        context.updateRegionMetrics( regionExecutionPlan, singletonList( pipelineMetrics1 ), loadChangePredicate );

        final AdaptationAction action = mock( AdaptationAction.class );
        when( bottleneckResolver0.resolve( regionExecutionPlan, pipelineMetrics1 ) ).thenReturn( action );
        when( action.getNewRegionExecutionPlan() ).thenReturn( newRegionExecutionPlan );
        when( newRegionExecutionPlan.getPipelineIds() ).thenReturn( asList( pipelineId0, pipelineId1 ) );

        context.resolveIfBottleneck( regionExecutionPlan, bottleneckPredicate, singletonList( bottleneckResolver0 ) );

        final PipelineMetricsSnapshot pipelineMetrics2 = mock( PipelineMetricsSnapshot.class );
        when( pipelineMetrics2.getPipelineId() ).thenReturn( pipelineId0 );

        final AdaptationAction rollback = mock( AdaptationAction.class );
        when( action.rollback() ).thenReturn( rollback );

        final AdaptationAction result = context.evaluateAdaptation( newRegionExecutionPlan,
                                                                    pipelineMetrics2,
                                                                    adaptationEvaluationPredicate );

        assertThat( result, equalTo( rollback ) );
        assertThat( context.getAdaptingPipelineId(), equalTo( pipelineId0 ) );
        assertNull( context.getAdaptationAction() );
        assertNull( context.getBaseExecutionPlan() );
        assertThat( context.getCurrentExecutionPlan(), equalTo( regionExecutionPlan ) );
        assertThat( context.getBlacklist( pipelineId0 ), contains( newRegionExecutionPlan ) );
    }

    @Test( expected = IllegalStateException.class )
    public void shouldNotEvaluateAdaptationWithMismatchingRegionExecutionPlan ()
    {
        final PipelineMetricsSnapshot pipelineMetrics2 = mock( PipelineMetricsSnapshot.class );
        when( pipelineMetrics2.getPipelineId() ).thenReturn( pipelineId0 );

        try
        {
            final PipelineMetricsSnapshot pipelineMetrics1 = mock( PipelineMetricsSnapshot.class );
            when( pipelineMetrics1.getPipelineId() ).thenReturn( pipelineId0 );
            when( bottleneckPredicate.test( pipelineMetrics1 ) ).thenReturn( true );

            context.updateRegionMetrics( regionExecutionPlan, singletonList( pipelineMetrics1 ), loadChangePredicate );

            final AdaptationAction action = mock( AdaptationAction.class );
            when( bottleneckResolver0.resolve( regionExecutionPlan, pipelineMetrics1 ) ).thenReturn( action );
            when( action.getNewRegionExecutionPlan() ).thenReturn( newRegionExecutionPlan );
            when( newRegionExecutionPlan.getPipelineIds() ).thenReturn( asList( pipelineId0, pipelineId1 ) );

            context.resolveIfBottleneck( regionExecutionPlan, bottleneckPredicate, singletonList( bottleneckResolver0 ) );
        }
        catch ( Exception e )
        {
            fail();
        }

        context.evaluateAdaptation( regionExecutionPlan, pipelineMetrics2, adaptationEvaluationPredicate );
    }

    @Test( expected = IllegalStateException.class )
    public void shouldFailEvaluationIfNoRollbackWhenAdaptationNotSuccessful ()
    {
        final PipelineMetricsSnapshot pipelineMetrics2 = mock( PipelineMetricsSnapshot.class );
        when( pipelineMetrics2.getPipelineId() ).thenReturn( pipelineId0 );

        try
        {
            final PipelineMetricsSnapshot pipelineMetrics1 = mock( PipelineMetricsSnapshot.class );
            when( pipelineMetrics1.getPipelineId() ).thenReturn( pipelineId0 );
            when( bottleneckPredicate.test( pipelineMetrics1 ) ).thenReturn( true );

            context.updateRegionMetrics( regionExecutionPlan, singletonList( pipelineMetrics1 ), loadChangePredicate );

            final AdaptationAction action = mock( AdaptationAction.class );
            when( bottleneckResolver0.resolve( regionExecutionPlan, pipelineMetrics1 ) ).thenReturn( action );
            when( action.getNewRegionExecutionPlan() ).thenReturn( newRegionExecutionPlan );
            when( newRegionExecutionPlan.getPipelineIds() ).thenReturn( asList( pipelineId0, pipelineId1 ) );

            context.resolveIfBottleneck( regionExecutionPlan, bottleneckPredicate, singletonList( bottleneckResolver0 ) );
        }
        catch ( Exception e )
        {
            fail();
        }

        context.evaluateAdaptation( newRegionExecutionPlan, pipelineMetrics2, adaptationEvaluationPredicate );
    }

    @Test
    public void shouldProvideNewResolutionOnceCurrentAdaptationEvaluationFails ()
    {
        final PipelineMetricsSnapshot pipelineMetrics1 = mock( PipelineMetricsSnapshot.class );
        when( pipelineMetrics1.getPipelineId() ).thenReturn( pipelineId0 );
        when( bottleneckPredicate.test( pipelineMetrics1 ) ).thenReturn( true );

        context.updateRegionMetrics( regionExecutionPlan, singletonList( pipelineMetrics1 ), loadChangePredicate );

        final AdaptationAction action = mock( AdaptationAction.class );
        when( bottleneckResolver0.resolve( regionExecutionPlan, pipelineMetrics1 ) ).thenReturn( action );
        when( action.getNewRegionExecutionPlan() ).thenReturn( newRegionExecutionPlan );
        when( newRegionExecutionPlan.getPipelineIds() ).thenReturn( asList( pipelineId0, pipelineId1 ) );

        context.resolveIfBottleneck( regionExecutionPlan, bottleneckPredicate, singletonList( bottleneckResolver0 ) );

        final PipelineMetricsSnapshot pipelineMetrics2 = mock( PipelineMetricsSnapshot.class );
        when( pipelineMetrics2.getPipelineId() ).thenReturn( pipelineId0 );

        final AdaptationAction rollback = mock( AdaptationAction.class );
        when( action.rollback() ).thenReturn( rollback );

        context.evaluateAdaptation( newRegionExecutionPlan, pipelineMetrics2, adaptationEvaluationPredicate );

        final AdaptationAction action2 = mock( AdaptationAction.class );
        when( bottleneckResolver1.resolve( regionExecutionPlan, pipelineMetrics1 ) ).thenReturn( action2 );
        final RegionExecutionPlan newRegionExecutionPlan2 = mock( RegionExecutionPlan.class );
        when( action2.getNewRegionExecutionPlan() ).thenReturn( newRegionExecutionPlan2 );
        when( newRegionExecutionPlan2.getPipelineIds() ).thenReturn( asList( pipelineId0, pipelineId1 ) );

        final AdaptationAction result = context.resolveIfBottleneck( regionExecutionPlan,
                                                                     bottleneckPredicate,
                                                                     asList( bottleneckResolver0, bottleneckResolver1 ) );

        assertThat( result, equalTo( action2 ) );
        verify( bottleneckResolver1 ).resolve( regionExecutionPlan, pipelineMetrics1 );
        assertThat( context.getBaseExecutionPlan(), equalTo( regionExecutionPlan ) );
        assertThat( context.getCurrentExecutionPlan(), equalTo( newRegionExecutionPlan2 ) );
        assertThat( context.getAdaptingPipelineId(), equalTo( pipelineId0 ) );
        assertThat( context.getAdaptationAction(), equalTo( result ) );
    }

    @Test
    public void shouldGiveUpAdaptationOnBottleneckPipeline ()
    {
        final PipelineMetricsSnapshot pipelineMetrics1 = mock( PipelineMetricsSnapshot.class );
        when( pipelineMetrics1.getPipelineId() ).thenReturn( pipelineId0 );
        when( bottleneckPredicate.test( pipelineMetrics1 ) ).thenReturn( true );

        context.updateRegionMetrics( regionExecutionPlan, singletonList( pipelineMetrics1 ), loadChangePredicate );

        final AdaptationAction action = mock( AdaptationAction.class );
        when( bottleneckResolver0.resolve( regionExecutionPlan, pipelineMetrics1 ) ).thenReturn( action );
        when( action.getNewRegionExecutionPlan() ).thenReturn( newRegionExecutionPlan );
        when( newRegionExecutionPlan.getPipelineIds() ).thenReturn( asList( pipelineId0, pipelineId1 ) );

        context.resolveIfBottleneck( regionExecutionPlan, bottleneckPredicate, singletonList( bottleneckResolver0 ) );

        final PipelineMetricsSnapshot pipelineMetrics2 = mock( PipelineMetricsSnapshot.class );
        when( pipelineMetrics2.getPipelineId() ).thenReturn( pipelineId0 );

        final AdaptationAction rollback = mock( AdaptationAction.class );
        when( action.rollback() ).thenReturn( rollback );

        context.evaluateAdaptation( newRegionExecutionPlan, pipelineMetrics2, adaptationEvaluationPredicate );

        final AdaptationAction result = context.resolveIfBottleneck( regionExecutionPlan,
                                                                     bottleneckPredicate,
                                                                     singletonList( bottleneckResolver0 ) );

        assertNull( result );
        verify( bottleneckResolver0, times( 2 ) ).resolve( regionExecutionPlan, pipelineMetrics1 );
        assertNull( context.getBaseExecutionPlan() );
        assertThat( context.getCurrentExecutionPlan(), equalTo( regionExecutionPlan ) );
        assertNull( context.getAdaptingPipelineId() );
        assertNull( context.getAdaptationAction() );
        assertThat( context.getNonResolvablePipelineIds(), contains( pipelineId0 ) );
        assertThat( context.getBlacklist( pipelineId0 ), contains( newRegionExecutionPlan ) );
    }

    @Test
    public void shouldDiscardNonResolvablePipelineIdsOnLoadChange ()
    {
        final PipelineMetricsSnapshot pipelineMetrics1 = mock( PipelineMetricsSnapshot.class );
        when( pipelineMetrics1.getPipelineId() ).thenReturn( pipelineId0 );
        when( bottleneckPredicate.test( pipelineMetrics1 ) ).thenReturn( true );

        context.updateRegionMetrics( regionExecutionPlan, singletonList( pipelineMetrics1 ), loadChangePredicate );

        context.resolveIfBottleneck( regionExecutionPlan, bottleneckPredicate, singletonList( bottleneckResolver0 ) );

        assertThat( context.getNonResolvablePipelineIds(), contains( pipelineId0 ) );

        final PipelineMetricsSnapshot pipelineMetrics2 = mock( PipelineMetricsSnapshot.class );
        when( pipelineMetrics2.getPipelineId() ).thenReturn( pipelineId0 );
        when( loadChangePredicate.test( pipelineMetrics1, pipelineMetrics2 ) ).thenReturn( true );

        context.updateRegionMetrics( regionExecutionPlan, singletonList( pipelineMetrics2 ), loadChangePredicate );

        assertThat( context.getNonResolvablePipelineIds(), hasSize( 0 ) );
    }

    @Test
    public void shouldDiscardBlacklistOnLoadChange ()
    {
        final PipelineMetricsSnapshot pipelineMetrics1 = mock( PipelineMetricsSnapshot.class );
        when( pipelineMetrics1.getPipelineId() ).thenReturn( pipelineId0 );
        when( bottleneckPredicate.test( pipelineMetrics1 ) ).thenReturn( true );

        context.updateRegionMetrics( regionExecutionPlan, singletonList( pipelineMetrics1 ), loadChangePredicate );

        final AdaptationAction action = mock( AdaptationAction.class );
        when( bottleneckResolver0.resolve( regionExecutionPlan, pipelineMetrics1 ) ).thenReturn( action );
        when( action.getNewRegionExecutionPlan() ).thenReturn( newRegionExecutionPlan );
        when( newRegionExecutionPlan.getPipelineIds() ).thenReturn( asList( pipelineId0, pipelineId1 ) );

        context.resolveIfBottleneck( regionExecutionPlan, bottleneckPredicate, singletonList( bottleneckResolver0 ) );

        final PipelineMetricsSnapshot pipelineMetrics2 = mock( PipelineMetricsSnapshot.class );
        when( pipelineMetrics2.getPipelineId() ).thenReturn( pipelineId0 );

        final AdaptationAction rollback = mock( AdaptationAction.class );
        when( action.rollback() ).thenReturn( rollback );

        context.evaluateAdaptation( newRegionExecutionPlan, pipelineMetrics2, adaptationEvaluationPredicate );

        context.resolveIfBottleneck( regionExecutionPlan, bottleneckPredicate, singletonList( bottleneckResolver0 ) );

        assertThat( context.getBlacklist( pipelineId0 ), contains( newRegionExecutionPlan ) );

        final PipelineMetricsSnapshot pipelineMetrics3 = mock( PipelineMetricsSnapshot.class );
        when( pipelineMetrics3.getPipelineId() ).thenReturn( pipelineId0 );
        when( loadChangePredicate.test( pipelineMetrics1, pipelineMetrics3 ) ).thenReturn( true );

        context.updateRegionMetrics( regionExecutionPlan, singletonList( pipelineMetrics3 ), loadChangePredicate );

        assertThat( context.getBlacklist( pipelineId0 ), hasSize( 0 ) );
    }

}
