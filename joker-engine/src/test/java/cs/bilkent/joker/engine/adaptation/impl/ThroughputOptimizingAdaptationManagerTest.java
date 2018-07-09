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
import cs.bilkent.joker.engine.config.AdaptationConfig;
import cs.bilkent.joker.engine.config.JokerConfig;
import cs.bilkent.joker.engine.config.PartitionServiceConfig;
import cs.bilkent.joker.engine.flow.RegionDef;
import cs.bilkent.joker.engine.flow.RegionExecPlan;
import cs.bilkent.joker.engine.metric.FlowMetrics;
import cs.bilkent.joker.engine.metric.PipelineMetrics;
import cs.bilkent.joker.engine.metric.PipelineMetricsHistorySummarizer;
import cs.bilkent.joker.engine.region.RegionDefFormer;
import cs.bilkent.joker.engine.region.impl.IdGenerator;
import cs.bilkent.joker.engine.region.impl.PipelineTransformerImplTest.FusibleStatefulInput1Output1Operator;
import cs.bilkent.joker.engine.region.impl.PipelineTransformerImplTest.StatelessInput1Output1Operator;
import cs.bilkent.joker.engine.region.impl.RegionDefFormerImpl;
import cs.bilkent.joker.flow.FlowDef;
import cs.bilkent.joker.flow.FlowDefBuilder;
import cs.bilkent.joker.operator.OperatorDef;
import cs.bilkent.joker.operator.OperatorDefBuilder;
import cs.bilkent.joker.test.AbstractJokerTest;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyListOf;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith( MockitoJUnitRunner.class )
public class ThroughputOptimizingAdaptationManagerTest extends AbstractJokerTest
{

    private final OperatorDef operator1 = OperatorDefBuilder.newInstance( "op1", StatefulOperatorInput0Output1.class ).build();

    private final OperatorDef operator2 = OperatorDefBuilder.newInstance( "op2", StatelessInput1Output1Operator.class ).build();

    private final OperatorDef operator3 = OperatorDefBuilder.newInstance( "op3", FusibleStatefulInput1Output1Operator.class ).build();

    private final OperatorDef operator4 = OperatorDefBuilder.newInstance( "op4", StatelessInput1Output1Operator.class ).build();

    private FlowDef flow;

    private RegionDef region1;

    private RegionDef region2;

    private RegionDef region3;

    private RegionDef region4;

    @Mock
    private JokerConfig config;

    @Mock
    private AdaptationConfig adaptationConfig;

    @Mock
    private Function<RegionExecPlan, RegionAdaptationContext> regionAdaptationContextFactory;

    @Mock
    private PipelineMetricsHistorySummarizer pipelineMetricsHistorySummarizer;

    @Mock
    private BiPredicate<PipelineMetrics, PipelineMetrics> loadChangePredicate;

    @Mock
    private Predicate<PipelineMetrics> bottleneckPredicate;

    @Mock
    private BiPredicate<PipelineMetrics, PipelineMetrics> adaptationEvaluationPredicate;

    @Mock
    private PipelineMetrics region1PipelineMetrics;

    @Mock
    private PipelineMetrics region2PipelineMetrics;

    @Mock
    private PipelineMetrics region3PipelineMetrics;

    @Mock
    private PipelineMetrics region4PipelineMetrics;

    @Mock
    private FlowMetrics metrics;

    @Mock
    private RegionAdaptationContext region1Context;

    @Mock
    private RegionAdaptationContext region2Context;

    @Mock
    private RegionAdaptationContext region3Context;

    @Mock
    private RegionAdaptationContext region4Context;

    @Mock
    private RegionExecPlan region1ExecPlan;

    @Mock
    private RegionExecPlan region2ExecPlan;

    @Mock
    private RegionExecPlan region3ExecPlan;

    @Mock
    private RegionExecPlan region4ExecPlan;

    private List<RegionExecPlan> regionExecPlans;

    private ThroughputOptimizingAdaptationManager adaptationManager;

    @Before
    public void init ()
    {
        regionExecPlans = asList( region1ExecPlan, region2ExecPlan, region3ExecPlan, region4ExecPlan );

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

        adaptationManager = new ThroughputOptimizingAdaptationManager( config, regionAdaptationContextFactory );

        flow = new FlowDefBuilder().add( operator1 )
                                   .add( operator2 )
                                   .add( operator3 )
                                   .add( operator4 )
                                   .connect( operator1.getId(), operator2.getId() )
                                   .connect( operator2.getId(), operator3.getId() )
                                   .connect( operator3.getId(), operator4.getId() )
                                   .build();

        final RegionDefFormer regionDefFormer = new RegionDefFormerImpl( new IdGenerator() );
        final List<RegionDef> regions = regionDefFormer.createRegions( flow );

        region1 = getRegion( regions, operator1 );
        region2 = getRegion( regions, operator2 );
        region3 = getRegion( regions, operator3 );
        region4 = getRegion( regions, operator4 );

        when( region1Context.getRegionDef() ).thenReturn( region1 );
        when( region2Context.getRegionDef() ).thenReturn( region2 );
        when( region3Context.getRegionDef() ).thenReturn( region3 );
        when( region4Context.getRegionDef() ).thenReturn( region4 );
        when( region1Context.getRegionId() ).thenReturn( region1.getRegionId() );
        when( region2Context.getRegionId() ).thenReturn( region2.getRegionId() );
        when( region3Context.getRegionId() ).thenReturn( region3.getRegionId() );
        when( region4Context.getRegionId() ).thenReturn( region4.getRegionId() );

        when( metrics.getRegionMetrics( region1.getRegionId(), pipelineMetricsHistorySummarizer ) ).thenReturn( singletonList(
                region1PipelineMetrics ) );
        when( metrics.getRegionMetrics( region2.getRegionId(), pipelineMetricsHistorySummarizer ) ).thenReturn( singletonList(
                region2PipelineMetrics ) );
        when( metrics.getRegionMetrics( region3.getRegionId(), pipelineMetricsHistorySummarizer ) ).thenReturn( singletonList(
                region3PipelineMetrics ) );
        when( metrics.getRegionMetrics( region4.getRegionId(), pipelineMetricsHistorySummarizer ) ).thenReturn( singletonList(
                region4PipelineMetrics ) );
    }

    @Test
    public void shouldInitRegionAdaptationContexts ()
    {
        when( region1ExecPlan.getRegionDef() ).thenReturn( region1 );
        when( region2ExecPlan.getRegionDef() ).thenReturn( region2 );
        when( region3ExecPlan.getRegionDef() ).thenReturn( region3 );
        when( region4ExecPlan.getRegionDef() ).thenReturn( region4 );
        when( region1ExecPlan.getRegionId() ).thenReturn( region1.getRegionId() );
        when( region2ExecPlan.getRegionId() ).thenReturn( region2.getRegionId() );
        when( region3ExecPlan.getRegionId() ).thenReturn( region3.getRegionId() );
        when( region4ExecPlan.getRegionId() ).thenReturn( region4.getRegionId() );
        when( regionAdaptationContextFactory.apply( region1ExecPlan ) ).thenReturn( region1Context );
        when( regionAdaptationContextFactory.apply( region2ExecPlan ) ).thenReturn( region2Context );
        when( regionAdaptationContextFactory.apply( region3ExecPlan ) ).thenReturn( region3Context );
        when( regionAdaptationContextFactory.apply( region4ExecPlan ) ).thenReturn( region4Context );

        adaptationManager.initialize( flow, regionExecPlans );

        assertThat( adaptationManager.getRegion( region1.getRegionId() ), equalTo( region1Context ) );
        assertThat( adaptationManager.getRegion( region2.getRegionId() ), equalTo( region2Context ) );
        assertThat( adaptationManager.getRegion( region3.getRegionId() ), equalTo( region3Context ) );
        assertThat( adaptationManager.getRegion( region4.getRegionId() ), equalTo( region4Context ) );
    }

    @Test
    public void shouldApplyFlowMetrics ()
    {
        shouldInitRegionAdaptationContexts();

        adaptationManager.adapt( regionExecPlans, metrics );

        verify( metrics ).getRegionMetrics( region1.getRegionId(), pipelineMetricsHistorySummarizer );
        verify( metrics ).getRegionMetrics( region2.getRegionId(), pipelineMetricsHistorySummarizer );
        verify( metrics ).getRegionMetrics( region3.getRegionId(), pipelineMetricsHistorySummarizer );
        verify( metrics ).getRegionMetrics( region4.getRegionId(), pipelineMetricsHistorySummarizer );

        verify( region1Context ).updateRegionMetrics( singletonList( region1PipelineMetrics ), loadChangePredicate );
        verify( region2Context ).updateRegionMetrics( singletonList( region2PipelineMetrics ), loadChangePredicate );
        verify( region3Context ).updateRegionMetrics( singletonList( region3PipelineMetrics ), loadChangePredicate );
        verify( region4Context ).updateRegionMetrics( singletonList( region4PipelineMetrics ), loadChangePredicate );

        verify( region1Context ).resolveIfBottleneck( eq( bottleneckPredicate ), anyListOf( BottleneckResolver.class ) );
        verify( region2Context ).resolveIfBottleneck( eq( bottleneckPredicate ), anyListOf( BottleneckResolver.class ) );
        verify( region3Context ).resolveIfBottleneck( eq( bottleneckPredicate ), anyListOf( BottleneckResolver.class ) );
        verify( region4Context ).resolveIfBottleneck( eq( bottleneckPredicate ), anyListOf( BottleneckResolver.class ) );
    }

    @Test
    public void shouldReturnAdaptationActionForSingleRegion ()
    {
        shouldInitRegionAdaptationContexts();

        final AdaptationAction action = mock( AdaptationAction.class );

        when( region1Context.resolveIfBottleneck( eq( bottleneckPredicate ), anyListOf( BottleneckResolver.class ) ) ).thenReturn(
                singletonList( action ) );
        when( region2Context.resolveIfBottleneck( eq( bottleneckPredicate ),
                                                  anyListOf( BottleneckResolver.class ) ) ).thenReturn( emptyList() );
        when( region3Context.resolveIfBottleneck( eq( bottleneckPredicate ),
                                                  anyListOf( BottleneckResolver.class ) ) ).thenReturn( emptyList() );
        when( region4Context.resolveIfBottleneck( eq( bottleneckPredicate ),
                                                  anyListOf( BottleneckResolver.class ) ) ).thenReturn( emptyList() );

        final List<AdaptationAction> result = adaptationManager.adapt( regionExecPlans, metrics );

        assertThat( result, equalTo( singletonList( action ) ) );

        assertThat( adaptationManager.getAdaptingRegions(), equalTo( singletonList( region1Context ) ) );

        verify( region1Context ).updateRegionMetrics( singletonList( region1PipelineMetrics ), loadChangePredicate );
        verify( region2Context ).updateRegionMetrics( singletonList( region2PipelineMetrics ), loadChangePredicate );
        verify( region3Context ).updateRegionMetrics( singletonList( region3PipelineMetrics ), loadChangePredicate );
        verify( region4Context ).updateRegionMetrics( singletonList( region4PipelineMetrics ), loadChangePredicate );

        verify( region1Context ).resolveIfBottleneck( eq( bottleneckPredicate ), anyListOf( BottleneckResolver.class ) );
        verify( region2Context ).resolveIfBottleneck( eq( bottleneckPredicate ), anyListOf( BottleneckResolver.class ) );
        verify( region3Context ).resolveIfBottleneck( eq( bottleneckPredicate ), anyListOf( BottleneckResolver.class ) );
        verify( region4Context ).resolveIfBottleneck( eq( bottleneckPredicate ), anyListOf( BottleneckResolver.class ) );
    }

    @Test
    public void shouldReturnAdaptationActionForMultipleRegions ()
    {
        shouldInitRegionAdaptationContexts();

        final AdaptationAction action2 = mock( AdaptationAction.class );
        final AdaptationAction action3 = mock( AdaptationAction.class );

        when( region1Context.resolveIfBottleneck( eq( bottleneckPredicate ),
                                                  anyListOf( BottleneckResolver.class ) ) ).thenReturn( emptyList() );
        when( region2Context.resolveIfBottleneck( eq( bottleneckPredicate ), anyListOf( BottleneckResolver.class ) ) ).thenReturn(
                singletonList( action2 ) );
        when( region3Context.resolveIfBottleneck( eq( bottleneckPredicate ), anyListOf( BottleneckResolver.class ) ) ).thenReturn(
                singletonList( action3 ) );
        when( region4Context.resolveIfBottleneck( eq( bottleneckPredicate ),
                                                  anyListOf( BottleneckResolver.class ) ) ).thenReturn( emptyList() );

        final List<AdaptationAction> result = adaptationManager.adapt( regionExecPlans, metrics );

        assertThat( result, equalTo( asList( action2, action3 ) ) );

        assertThat( adaptationManager.getAdaptingRegions(), equalTo( asList( region2Context, region3Context ) ) );

        verify( region1Context ).resolveIfBottleneck( eq( bottleneckPredicate ), anyListOf( BottleneckResolver.class ) );
        verify( region2Context ).resolveIfBottleneck( eq( bottleneckPredicate ), anyListOf( BottleneckResolver.class ) );
        verify( region3Context ).resolveIfBottleneck( eq( bottleneckPredicate ), anyListOf( BottleneckResolver.class ) );
        verify( region4Context ).resolveIfBottleneck( eq( bottleneckPredicate ), anyListOf( BottleneckResolver.class ) );
    }

    @Test
    public void shouldFinalizeAdaptationForSingleRegion ()
    {
        shouldInitRegionAdaptationContexts();

        final AdaptationAction action = mock( AdaptationAction.class );

        when( region1Context.resolveIfBottleneck( eq( bottleneckPredicate ), anyListOf( BottleneckResolver.class ) ) ).thenReturn(
                singletonList( action ) );
        when( region2Context.resolveIfBottleneck( eq( bottleneckPredicate ),
                                                  anyListOf( BottleneckResolver.class ) ) ).thenReturn( emptyList() );
        when( region3Context.resolveIfBottleneck( eq( bottleneckPredicate ),
                                                  anyListOf( BottleneckResolver.class ) ) ).thenReturn( emptyList() );
        when( region4Context.resolveIfBottleneck( eq( bottleneckPredicate ),
                                                  anyListOf( BottleneckResolver.class ) ) ).thenReturn( emptyList() );

        final List<AdaptationAction> result1 = adaptationManager.adapt( regionExecPlans, metrics );

        assertThat( result1, equalTo( singletonList( action ) ) );

        assertThat( adaptationManager.getAdaptingRegions(), equalTo( singletonList( region1Context ) ) );

        final PipelineMetrics newUpstreamPipelineMetrics = mock( PipelineMetrics.class );
        final List<PipelineMetrics> regionMetrics = singletonList( newUpstreamPipelineMetrics );
        when( metrics.getRegionMetrics( region1.getRegionId(), pipelineMetricsHistorySummarizer ) ).thenReturn( regionMetrics );

        when( region1Context.isAdaptationSuccessful( regionMetrics, adaptationEvaluationPredicate ) ).thenReturn( true );

        final List<AdaptationAction> result2 = adaptationManager.adapt( regionExecPlans, metrics );

        assertTrue( result2.isEmpty() );

        verify( region1Context ).isAdaptationSuccessful( regionMetrics, adaptationEvaluationPredicate );
        verify( region1Context ).finalizeAdaptation( regionMetrics );
        verify( region2Context, never() ).isAdaptationSuccessful( anyObject(), anyObject() );
        verify( region4Context, never() ).isAdaptationSuccessful( anyObject(), anyObject() );
    }

    @Test
    public void shouldFinalizeAdaptationForMultipleRegions ()
    {
        shouldInitRegionAdaptationContexts();

        final AdaptationAction action1 = mock( AdaptationAction.class );
        final AdaptationAction action3 = mock( AdaptationAction.class );

        when( region1Context.resolveIfBottleneck( eq( bottleneckPredicate ), anyListOf( BottleneckResolver.class ) ) ).thenReturn(
                singletonList( action1 ) );
        when( region2Context.resolveIfBottleneck( eq( bottleneckPredicate ),
                                                  anyListOf( BottleneckResolver.class ) ) ).thenReturn( emptyList() );
        when( region3Context.resolveIfBottleneck( eq( bottleneckPredicate ), anyListOf( BottleneckResolver.class ) ) ).thenReturn(
                singletonList( action3 ) );
        when( region4Context.resolveIfBottleneck( eq( bottleneckPredicate ),
                                                  anyListOf( BottleneckResolver.class ) ) ).thenReturn( emptyList() );

        final List<AdaptationAction> result1 = adaptationManager.adapt( regionExecPlans, metrics );

        assertThat( result1, equalTo( asList( action1, action3 ) ) );

        assertThat( adaptationManager.getAdaptingRegions(), equalTo( asList( region1Context, region3Context ) ) );

        final PipelineMetrics newUpstreamPipelineMetrics1 = mock( PipelineMetrics.class );
        final PipelineMetrics newUpstreamPipelineMetrics3 = mock( PipelineMetrics.class );
        final List<PipelineMetrics> region1Metrics = singletonList( newUpstreamPipelineMetrics1 );
        final List<PipelineMetrics> region3Metrics = singletonList( newUpstreamPipelineMetrics3 );
        when( metrics.getRegionMetrics( region1.getRegionId(), pipelineMetricsHistorySummarizer ) ).thenReturn( region1Metrics );
        when( metrics.getRegionMetrics( region3.getRegionId(), pipelineMetricsHistorySummarizer ) ).thenReturn( region3Metrics );

        when( region1Context.isAdaptationSuccessful( region1Metrics, adaptationEvaluationPredicate ) ).thenReturn( true );

        final List<AdaptationAction> result2 = adaptationManager.adapt( regionExecPlans, metrics );

        assertTrue( result2.isEmpty() );

        verify( region1Context ).isAdaptationSuccessful( region1Metrics, adaptationEvaluationPredicate );
        verify( region1Context ).finalizeAdaptation( region1Metrics );
        verify( region3Context, never() ).isAdaptationSuccessful( region3Metrics, adaptationEvaluationPredicate );
        verify( region3Context ).finalizeAdaptation( region3Metrics );
        verify( region2Context, never() ).isAdaptationSuccessful( anyObject(), anyObject() );
        verify( region4Context, never() ).isAdaptationSuccessful( anyObject(), anyObject() );
    }

    @Test
    public void shouldRevertCurrentAdaptationAndContinueAdaptationOfSingleRegionWithNewAction ()
    {
        shouldInitRegionAdaptationContexts();

        final AdaptationAction action1 = mock( AdaptationAction.class );
        final AdaptationAction action2 = mock( AdaptationAction.class );

        when( region1Context.resolveIfBottleneck( eq( bottleneckPredicate ), anyListOf( BottleneckResolver.class ) ) ).thenReturn(
                singletonList( action1 ),
                singletonList( action2 ) );
        when( region2Context.resolveIfBottleneck( eq( bottleneckPredicate ),
                                                  anyListOf( BottleneckResolver.class ) ) ).thenReturn( emptyList() );
        when( region3Context.resolveIfBottleneck( eq( bottleneckPredicate ),
                                                  anyListOf( BottleneckResolver.class ) ) ).thenReturn( emptyList() );
        when( region4Context.resolveIfBottleneck( eq( bottleneckPredicate ),
                                                  anyListOf( BottleneckResolver.class ) ) ).thenReturn( emptyList() );

        final List<AdaptationAction> result1 = adaptationManager.adapt( regionExecPlans, metrics );

        assertThat( result1, equalTo( singletonList( action1 ) ) );
        assertThat( adaptationManager.getAdaptingRegions(), equalTo( singletonList( region1Context ) ) );

        final PipelineMetrics newUpstreamPipelineMetrics = mock( PipelineMetrics.class );
        when( metrics.getRegionMetrics( region1.getRegionId(), pipelineMetricsHistorySummarizer ) ).thenReturn( singletonList(
                newUpstreamPipelineMetrics ) );

        final AdaptationAction revert = mock( AdaptationAction.class );

        when( region1Context.revertAdaptation() ).thenReturn( singletonList( revert ) );

        final List<AdaptationAction> result2 = adaptationManager.adapt( regionExecPlans, metrics );

        assertThat( result2, equalTo( asList( revert, action2 ) ) );
        assertThat( adaptationManager.getAdaptingRegions(), equalTo( singletonList( region1Context ) ) );

        verify( region1Context ).isAdaptationSuccessful( singletonList( newUpstreamPipelineMetrics ), adaptationEvaluationPredicate );
    }

    @Test
    public void shouldRevertAndFinishCurrentAdaptationOfSingleRegionWithNoNewAction ()
    {
        shouldInitRegionAdaptationContexts();

        final AdaptationAction action = mock( AdaptationAction.class );

        when( region1Context.resolveIfBottleneck( eq( bottleneckPredicate ), anyListOf( BottleneckResolver.class ) ) ).thenReturn(
                singletonList( action ),
                emptyList() );
        when( region2Context.resolveIfBottleneck( eq( bottleneckPredicate ),
                                                  anyListOf( BottleneckResolver.class ) ) ).thenReturn( emptyList() );
        when( region3Context.resolveIfBottleneck( eq( bottleneckPredicate ),
                                                  anyListOf( BottleneckResolver.class ) ) ).thenReturn( emptyList() );
        when( region4Context.resolveIfBottleneck( eq( bottleneckPredicate ),
                                                  anyListOf( BottleneckResolver.class ) ) ).thenReturn( emptyList() );

        final List<AdaptationAction> result = adaptationManager.adapt( regionExecPlans, metrics );

        assertThat( result, equalTo( singletonList( action ) ) );
        assertThat( adaptationManager.getAdaptingRegions(), equalTo( singletonList( region1Context ) ) );

        final PipelineMetrics newUpstreamPipelineMetrics = mock( PipelineMetrics.class );
        when( metrics.getRegionMetrics( region1.getRegionId(), pipelineMetricsHistorySummarizer ) ).thenReturn( singletonList(
                newUpstreamPipelineMetrics ) );

        final AdaptationAction revert = mock( AdaptationAction.class );

        when( region1Context.revertAdaptation() ).thenReturn( singletonList( revert ) );

        final List<AdaptationAction> result2 = adaptationManager.adapt( regionExecPlans, metrics );

        assertThat( result2, equalTo( singletonList( revert ) ) );
        assertTrue( adaptationManager.getAdaptingRegions().isEmpty() );

        verify( region1Context ).isAdaptationSuccessful( singletonList( newUpstreamPipelineMetrics ), adaptationEvaluationPredicate );
    }

    @Test
    public void shouldRevertAndFinishCurrentAdaptationOfMultipleRegionsWithNoNewAction ()
    {
        shouldInitRegionAdaptationContexts();

        final AdaptationAction action1 = mock( AdaptationAction.class );
        final AdaptationAction action2 = mock( AdaptationAction.class );
        final AdaptationAction action3 = mock( AdaptationAction.class );
        final AdaptationAction action4 = mock( AdaptationAction.class );

        when( region1Context.resolveIfBottleneck( eq( bottleneckPredicate ), anyListOf( BottleneckResolver.class ) ) ).thenReturn(
                singletonList( action1 ),
                emptyList() );
        when( region2Context.resolveIfBottleneck( eq( bottleneckPredicate ), anyListOf( BottleneckResolver.class ) ) ).thenReturn(
                singletonList( action2 ),
                emptyList() );
        when( region3Context.resolveIfBottleneck( eq( bottleneckPredicate ), anyListOf( BottleneckResolver.class ) ) ).thenReturn(
                singletonList( action3 ) );
        when( region4Context.resolveIfBottleneck( eq( bottleneckPredicate ), anyListOf( BottleneckResolver.class ) ) ).thenReturn(
                singletonList( action4 ) );

        final List<AdaptationAction> result = adaptationManager.adapt( regionExecPlans, metrics );

        assertThat( result, equalTo( asList( action1, action2, action3, action4 ) ) );
        assertThat( adaptationManager.getAdaptingRegions(),
                    equalTo( asList( region1Context, region2Context, region3Context, region4Context ) ) );

        final PipelineMetrics newUpstreamPipelineMetrics1 = mock( PipelineMetrics.class );
        final PipelineMetrics newUpstreamPipelineMetrics2 = mock( PipelineMetrics.class );
        when( metrics.getRegionMetrics( region1.getRegionId(), pipelineMetricsHistorySummarizer ) ).thenReturn( singletonList(
                newUpstreamPipelineMetrics1 ) );
        when( metrics.getRegionMetrics( region2.getRegionId(), pipelineMetricsHistorySummarizer ) ).thenReturn( singletonList(
                newUpstreamPipelineMetrics2 ) );

        final AdaptationAction revert11 = mock( AdaptationAction.class );
        final AdaptationAction revert12 = mock( AdaptationAction.class );
        final AdaptationAction revert21 = mock( AdaptationAction.class );
        final AdaptationAction revert22 = mock( AdaptationAction.class );
        final AdaptationAction revert31 = mock( AdaptationAction.class );
        final AdaptationAction revert32 = mock( AdaptationAction.class );
        final AdaptationAction revert41 = mock( AdaptationAction.class );
        final AdaptationAction revert42 = mock( AdaptationAction.class );

        when( region1Context.revertAdaptation() ).thenReturn( asList( revert11, revert12 ) );
        when( region2Context.revertAdaptation() ).thenReturn( asList( revert21, revert22 ) );
        when( region3Context.revertAdaptation() ).thenReturn( asList( revert31, revert32 ) );
        when( region4Context.revertAdaptation() ).thenReturn( asList( revert41, revert42 ) );

        final List<AdaptationAction> result2 = adaptationManager.adapt( regionExecPlans, metrics );

        assertThat( result2, equalTo( asList( revert41, revert42, revert31, revert32, revert21, revert22, revert11, revert12 ) ) );
        assertTrue( adaptationManager.getAdaptingRegions().isEmpty() );

        verify( region1Context ).isAdaptationSuccessful( singletonList( newUpstreamPipelineMetrics1 ), adaptationEvaluationPredicate );
        verify( region2Context, never() ).isAdaptationSuccessful( anyObject(), anyObject() );
        verify( region3Context, never() ).isAdaptationSuccessful( anyObject(), anyObject() );
        verify( region4Context, never() ).isAdaptationSuccessful( anyObject(), anyObject() );
    }

    @Test
    public void shouldRevertCurrentAdaptationAndContinueAdaptationOfMultipleRegionsWithNewAction ()
    {
        shouldInitRegionAdaptationContexts();

        final AdaptationAction action11 = mock( AdaptationAction.class );
        final AdaptationAction action12 = mock( AdaptationAction.class );
        final AdaptationAction action2 = mock( AdaptationAction.class );
        final AdaptationAction action3 = mock( AdaptationAction.class );
        final AdaptationAction action4 = mock( AdaptationAction.class );

        when( region1Context.resolveIfBottleneck( eq( bottleneckPredicate ), anyListOf( BottleneckResolver.class ) ) ).thenReturn(
                singletonList( action11 ),
                singletonList( action12 ) );
        when( region2Context.resolveIfBottleneck( eq( bottleneckPredicate ), anyListOf( BottleneckResolver.class ) ) ).thenReturn(
                singletonList( action2 ) );
        when( region3Context.resolveIfBottleneck( eq( bottleneckPredicate ), anyListOf( BottleneckResolver.class ) ) ).thenReturn(
                singletonList( action3 ) );
        when( region4Context.resolveIfBottleneck( eq( bottleneckPredicate ), anyListOf( BottleneckResolver.class ) ) ).thenReturn(
                singletonList( action4 ) );

        final List<AdaptationAction> result = adaptationManager.adapt( regionExecPlans, metrics );

        assertThat( result, equalTo( asList( action11, action2, action3, action4 ) ) );
        assertThat( adaptationManager.getAdaptingRegions(),
                    equalTo( asList( region1Context, region2Context, region3Context, region4Context ) ) );

        final PipelineMetrics newUpstreamPipelineMetrics1 = mock( PipelineMetrics.class );
        when( metrics.getRegionMetrics( region1.getRegionId(), pipelineMetricsHistorySummarizer ) ).thenReturn( singletonList(
                newUpstreamPipelineMetrics1 ) );

        final AdaptationAction revert = mock( AdaptationAction.class );

        when( region1Context.revertAdaptation() ).thenReturn( singletonList( revert ) );

        final List<AdaptationAction> result2 = adaptationManager.adapt( regionExecPlans, metrics );

        assertThat( result2, equalTo( asList( revert, action12 ) ) );
        assertThat( adaptationManager.getAdaptingRegions(),
                    equalTo( asList( region1Context, region2Context, region3Context, region4Context ) ) );

        verify( region1Context ).isAdaptationSuccessful( singletonList( newUpstreamPipelineMetrics1 ), adaptationEvaluationPredicate );
        verify( region2Context, never() ).isAdaptationSuccessful( anyObject(), anyObject() );
        verify( region3Context, never() ).isAdaptationSuccessful( anyObject(), anyObject() );
        verify( region4Context, never() ).isAdaptationSuccessful( anyObject(), anyObject() );
        verify( region1Context, never() ).finalizeAdaptation( anyObject() );
        verify( region2Context, never() ).finalizeAdaptation( anyObject() );
        verify( region3Context, never() ).finalizeAdaptation( anyObject() );
        verify( region4Context, never() ).finalizeAdaptation( anyObject() );
        verify( region1Context ).revertAdaptation();
        verify( region2Context, never() ).revertAdaptation();
        verify( region3Context, never() ).revertAdaptation();
        verify( region4Context, never() ).revertAdaptation();
    }

    public static RegionDef getRegion ( final List<RegionDef> regions, final OperatorDef operatorDef )
    {
        return regions.stream().filter( r -> r.getOperators().contains( operatorDef ) ).findFirst().orElse( null );
    }

}
