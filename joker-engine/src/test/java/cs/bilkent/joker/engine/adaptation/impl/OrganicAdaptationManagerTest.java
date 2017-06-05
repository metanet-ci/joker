package cs.bilkent.joker.engine.adaptation.impl;

import java.util.List;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Predicate;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import cs.bilkent.joker.engine.adaptation.AdaptationAction;
import cs.bilkent.joker.engine.adaptation.BottleneckResolver;
import cs.bilkent.joker.engine.adaptation.impl.adaptationaction.MergePipelinesActionTest.StatefulOperatorInput0Output1;
import static cs.bilkent.joker.engine.adaptation.impl.adaptationaction.RegionRebalanceActionTest.getRegion;
import cs.bilkent.joker.engine.config.AdaptationConfig;
import cs.bilkent.joker.engine.config.JokerConfig;
import cs.bilkent.joker.engine.config.PartitionServiceConfig;
import cs.bilkent.joker.engine.flow.RegionDef;
import cs.bilkent.joker.engine.flow.RegionExecutionPlan;
import cs.bilkent.joker.engine.metric.FlowMetrics;
import cs.bilkent.joker.engine.metric.PipelineMetrics;
import cs.bilkent.joker.engine.metric.PipelineMetricsHistorySummarizer;
import cs.bilkent.joker.engine.region.RegionDefFormer;
import cs.bilkent.joker.engine.region.impl.IdGenerator;
import cs.bilkent.joker.engine.region.impl.PipelineTransformerImplTest.StatelessInput1Output1Operator;
import cs.bilkent.joker.engine.region.impl.RegionDefFormerImpl;
import cs.bilkent.joker.flow.FlowDef;
import cs.bilkent.joker.flow.FlowDefBuilder;
import cs.bilkent.joker.operator.OperatorDef;
import cs.bilkent.joker.operator.OperatorDefBuilder;
import static cs.bilkent.joker.operator.spec.OperatorType.STATEFUL;
import static cs.bilkent.joker.operator.spec.OperatorType.STATELESS;
import cs.bilkent.joker.test.AbstractJokerTest;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyListOf;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith( MockitoJUnitRunner.class )
public class OrganicAdaptationManagerTest extends AbstractJokerTest
{

    private final OperatorDef source = OperatorDefBuilder.newInstance( "up", StatefulOperatorInput0Output1.class ).build();

    private final OperatorDef sink = OperatorDefBuilder.newInstance( "down", StatelessInput1Output1Operator.class ).build();

    private RegionDef upstreamRegion;

    private RegionDef downstreamRegion;

    @Mock
    private JokerConfig config;

    @Mock
    private AdaptationConfig adaptationConfig;

    @Mock
    private Function<RegionExecutionPlan, RegionAdaptationContext> regionAdaptationContextFactory;

    @Mock
    private PipelineMetricsHistorySummarizer pipelineMetricsHistorySummarizer;

    @Mock
    private BiPredicate<PipelineMetrics, PipelineMetrics> loadChangePredicate;

    @Mock
    private Predicate<PipelineMetrics> bottleneckPredicate;

    @Mock
    private BiPredicate<PipelineMetrics, PipelineMetrics> adaptationEvaluationPredicate;

    @Mock
    private PipelineMetrics upstreamPipelineMetrics;

    @Mock
    private PipelineMetrics downstreamPipelineMetrics;

    @Mock
    private FlowMetrics flowMetrics;

    @Mock
    private RegionAdaptationContext upstreamContext;

    @Mock
    private RegionAdaptationContext downstreamContext;

    @Mock
    private RegionExecutionPlan upstreamRegionExecutionPlan;

    @Mock
    private RegionExecutionPlan downstreamRegionExecutionPlan;

    private List<RegionExecutionPlan> regionExecutionPlans;

    private OrganicAdaptationManager adaptationManager;

    @Before
    public void init ()
    {
        regionExecutionPlans = asList( upstreamRegionExecutionPlan, downstreamRegionExecutionPlan );

        when( config.getAdaptationConfig() ).thenReturn( adaptationConfig );
        when( adaptationConfig.getPipelineMetricsHistorySummarizer() ).thenReturn( pipelineMetricsHistorySummarizer );
        when( adaptationConfig.getLoadChangePredicate() ).thenReturn( loadChangePredicate );
        when( adaptationConfig.getBottleneckPredicate() ).thenReturn( bottleneckPredicate );
        when( adaptationConfig.getAdaptationEvaluationPredicate() ).thenReturn( adaptationEvaluationPredicate );
        when( adaptationConfig.isAdaptationEnabled() ).thenReturn( true );
        when( adaptationConfig.isPipelineSplitEnabled() ).thenReturn( true );
        when( adaptationConfig.isRegionRebalanceEnabled() ).thenReturn( true );

        final PartitionServiceConfig partitionServiceConfig = mock( PartitionServiceConfig.class );
        when( partitionServiceConfig.getMaxReplicaCount() ).thenReturn( 2 );
        when( config.getPartitionServiceConfig() ).thenReturn( partitionServiceConfig );

        adaptationManager = new OrganicAdaptationManager( config, regionAdaptationContextFactory );

        final FlowDef flow = new FlowDefBuilder().add( source ).add( sink ).connect( source.getId(), sink.getId() ).build();

        final RegionDefFormer regionDefFormer = new RegionDefFormerImpl( new IdGenerator() );
        final List<RegionDef> regions = regionDefFormer.createRegions( flow );

        upstreamRegion = getRegion( regions, STATEFUL );
        downstreamRegion = getRegion( regions, STATELESS );

        when( upstreamContext.getRegionId() ).thenReturn( upstreamRegion.getRegionId() );
        when( downstreamContext.getRegionId() ).thenReturn( downstreamRegion.getRegionId() );

        when( flowMetrics.getRegionMetrics( upstreamRegion.getRegionId(), pipelineMetricsHistorySummarizer ) ).thenReturn( singletonList(
                upstreamPipelineMetrics ) );
        when( flowMetrics.getRegionMetrics( downstreamRegion.getRegionId(), pipelineMetricsHistorySummarizer ) ).thenReturn( singletonList(
                downstreamPipelineMetrics ) );
    }

    @Test
    public void shouldInitRegionAdaptationContexts ()
    {
        when( upstreamRegionExecutionPlan.getRegionDef() ).thenReturn( upstreamRegion );
        when( downstreamRegionExecutionPlan.getRegionDef() ).thenReturn( downstreamRegion );
        when( upstreamRegionExecutionPlan.getRegionId() ).thenReturn( upstreamRegion.getRegionId() );
        when( downstreamRegionExecutionPlan.getRegionId() ).thenReturn( downstreamRegion.getRegionId() );
        when( regionAdaptationContextFactory.apply( upstreamRegionExecutionPlan ) ).thenReturn( upstreamContext );
        when( regionAdaptationContextFactory.apply( downstreamRegionExecutionPlan ) ).thenReturn( downstreamContext );

        adaptationManager.initialize( regionExecutionPlans );

        assertThat( adaptationManager.getRegion( upstreamRegion.getRegionId() ), equalTo( upstreamContext ) );
        assertThat( adaptationManager.getRegion( downstreamRegion.getRegionId() ), equalTo( downstreamContext ) );
    }

    @Test
    public void shouldApplyFlowMetrics ()
    {
        shouldInitRegionAdaptationContexts();

        adaptationManager.apply( regionExecutionPlans, flowMetrics );

        verify( flowMetrics ).getRegionMetrics( upstreamRegion.getRegionId(), pipelineMetricsHistorySummarizer );
        verify( flowMetrics ).getRegionMetrics( downstreamRegion.getRegionId(), pipelineMetricsHistorySummarizer );

        verify( upstreamContext ).updateRegionMetrics( singletonList( upstreamPipelineMetrics ), loadChangePredicate );
        verify( downstreamContext ).updateRegionMetrics( singletonList( downstreamPipelineMetrics ), loadChangePredicate );

        verify( upstreamContext ).resolveIfBottleneck( eq( bottleneckPredicate ), anyListOf( BottleneckResolver.class ) );
        verify( downstreamContext ).resolveIfBottleneck( eq( bottleneckPredicate ), anyListOf( BottleneckResolver.class ) );
    }

    @Test
    public void shouldReturnAdaptationActionForUpstreamRegion ()
    {
        shouldInitRegionAdaptationContexts();

        final AdaptationAction action = mock( AdaptationAction.class );

        when( upstreamContext.resolveIfBottleneck( eq( bottleneckPredicate ), anyListOf( BottleneckResolver.class ) ) ).thenReturn(
                singletonList( action ) );

        final List<AdaptationAction> result = adaptationManager.apply( regionExecutionPlans, flowMetrics );

        assertThat( result, equalTo( singletonList( action ) ) );
        assertThat( adaptationManager.getAdaptingRegion(), equalTo( upstreamContext ) );

        verify( upstreamContext ).updateRegionMetrics( singletonList( upstreamPipelineMetrics ), loadChangePredicate );
        verify( downstreamContext ).updateRegionMetrics( singletonList( downstreamPipelineMetrics ), loadChangePredicate );

        verify( upstreamContext ).resolveIfBottleneck( eq( bottleneckPredicate ), anyListOf( BottleneckResolver.class ) );
        verify( downstreamContext, never() ).resolveIfBottleneck( eq( bottleneckPredicate ), anyListOf( BottleneckResolver.class ) );
    }

    @Test
    public void shouldReturnAdaptationActionForDownstreamRegion ()
    {
        shouldInitRegionAdaptationContexts();

        final AdaptationAction action = mock( AdaptationAction.class );

        when( downstreamContext.resolveIfBottleneck( eq( bottleneckPredicate ), anyListOf( BottleneckResolver.class ) ) ).thenReturn(
                singletonList( action ) );

        final List<AdaptationAction> result = adaptationManager.apply( regionExecutionPlans, flowMetrics );

        assertThat( result, equalTo( singletonList( action ) ) );
        assertThat( adaptationManager.getAdaptingRegion(), equalTo( downstreamContext ) );

        verify( upstreamContext ).updateRegionMetrics( singletonList( upstreamPipelineMetrics ), loadChangePredicate );
        verify( downstreamContext ).updateRegionMetrics( singletonList( downstreamPipelineMetrics ), loadChangePredicate );

        verify( upstreamContext ).resolveIfBottleneck( eq( bottleneckPredicate ), anyListOf( BottleneckResolver.class ) );
        verify( downstreamContext ).resolveIfBottleneck( eq( bottleneckPredicate ), anyListOf( BottleneckResolver.class ) );
    }

    @Test
    public void shouldNotRollbackAdaptation ()
    {
        shouldInitRegionAdaptationContexts();

        final AdaptationAction action = mock( AdaptationAction.class );

        when( upstreamContext.resolveIfBottleneck( eq( bottleneckPredicate ), anyListOf( BottleneckResolver.class ) ) ).thenReturn(
                singletonList( action ) );

        final List<AdaptationAction> result1 = adaptationManager.apply( regionExecutionPlans, flowMetrics );

        assertThat( result1, equalTo( singletonList( action ) ) );
        assertThat( adaptationManager.getAdaptingRegion(), equalTo( upstreamContext ) );

        verify( upstreamContext ).updateRegionMetrics( singletonList( upstreamPipelineMetrics ), loadChangePredicate );
        verify( downstreamContext ).updateRegionMetrics( singletonList( downstreamPipelineMetrics ), loadChangePredicate );

        verify( upstreamContext ).resolveIfBottleneck( eq( bottleneckPredicate ), anyListOf( BottleneckResolver.class ) );
        verify( downstreamContext, never() ).resolveIfBottleneck( eq( bottleneckPredicate ), anyListOf( BottleneckResolver.class ) );

        final PipelineMetrics newUpstreamPipelineMetrics = mock( PipelineMetrics.class );
        when( flowMetrics.getRegionMetrics( upstreamRegion.getRegionId(), pipelineMetricsHistorySummarizer ) ).thenReturn( singletonList(
                newUpstreamPipelineMetrics ) );

        final List<AdaptationAction> result2 = adaptationManager.apply( regionExecutionPlans, flowMetrics );

        assertTrue( result2.isEmpty() );

        verify( upstreamContext ).evaluateAdaptation( singletonList( newUpstreamPipelineMetrics ), adaptationEvaluationPredicate );
    }

    @Test
    public void shouldRollbackCurrentAdaptationAndContinueAdaptationIfNewActionFound ()
    {
        shouldInitRegionAdaptationContexts();

        final AdaptationAction action1 = mock( AdaptationAction.class );
        final AdaptationAction action2 = mock( AdaptationAction.class );

        when( upstreamContext.resolveIfBottleneck( eq( bottleneckPredicate ), anyListOf( BottleneckResolver.class ) ) ).thenReturn(
                singletonList( action1 ),
                singletonList( action2 ) );

        final List<AdaptationAction> result1 = adaptationManager.apply( regionExecutionPlans, flowMetrics );

        assertThat( result1, equalTo( singletonList( action1 ) ) );
        assertThat( adaptationManager.getAdaptingRegion(), equalTo( upstreamContext ) );

        final PipelineMetrics newUpstreamPipelineMetrics = mock( PipelineMetrics.class );
        when( flowMetrics.getRegionMetrics( upstreamRegion.getRegionId(), pipelineMetricsHistorySummarizer ) ).thenReturn( singletonList(
                newUpstreamPipelineMetrics ) );

        final AdaptationAction rollback = mock( AdaptationAction.class );

        when( upstreamContext.evaluateAdaptation( singletonList( newUpstreamPipelineMetrics ), adaptationEvaluationPredicate ) ).thenReturn(
                singletonList( rollback ) );

        final List<AdaptationAction> result2 = adaptationManager.apply( regionExecutionPlans, flowMetrics );

        assertThat( result2, equalTo( asList( rollback, action2 ) ) );
        assertThat( adaptationManager.getAdaptingRegion(), equalTo( upstreamContext ) );

        verify( upstreamContext ).evaluateAdaptation( singletonList( newUpstreamPipelineMetrics ), adaptationEvaluationPredicate );
    }

    @Test
    public void shouldRollbackAndFinishCurrentAdaptationIfNewActionNotFound ()
    {
        shouldInitRegionAdaptationContexts();

        final AdaptationAction action = mock( AdaptationAction.class );

        when( upstreamContext.resolveIfBottleneck( eq( bottleneckPredicate ), anyListOf( BottleneckResolver.class ) ) ).thenReturn(
                singletonList( action ),
                emptyList() );

        final List<AdaptationAction> result = adaptationManager.apply( asList( upstreamRegionExecutionPlan, downstreamRegionExecutionPlan ),
                                                                       flowMetrics );

        assertThat( result, equalTo( singletonList( action ) ) );
        assertThat( adaptationManager.getAdaptingRegion(), equalTo( upstreamContext ) );

        final PipelineMetrics newUpstreamPipelineMetrics = mock( PipelineMetrics.class );
        when( flowMetrics.getRegionMetrics( upstreamRegion.getRegionId(), pipelineMetricsHistorySummarizer ) ).thenReturn( singletonList(
                newUpstreamPipelineMetrics ) );

        final AdaptationAction rollback = mock( AdaptationAction.class );

        when( upstreamContext.evaluateAdaptation( singletonList( newUpstreamPipelineMetrics ), adaptationEvaluationPredicate ) ).thenReturn(
                singletonList( rollback ) );

        final List<AdaptationAction> result2 = adaptationManager.apply( asList( upstreamRegionExecutionPlan,
                                                                                downstreamRegionExecutionPlan ), flowMetrics );

        assertThat( result2, equalTo( singletonList( rollback ) ) );
        assertNull( adaptationManager.getAdaptingRegion() );

        verify( upstreamContext ).evaluateAdaptation( singletonList( newUpstreamPipelineMetrics ), adaptationEvaluationPredicate );
    }

}
