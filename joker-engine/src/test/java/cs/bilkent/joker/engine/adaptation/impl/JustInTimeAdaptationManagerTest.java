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
import cs.bilkent.joker.engine.adaptation.PipelineMetricsHistorySummarizer;
import cs.bilkent.joker.engine.adaptation.impl.adaptationaction.MergePipelinesActionTest.StatefulOperatorInput0Output1;
import static cs.bilkent.joker.engine.adaptation.impl.adaptationaction.RegionRebalanceActionTest.getRegion;
import cs.bilkent.joker.engine.config.AdaptationConfig;
import cs.bilkent.joker.engine.config.JokerConfig;
import cs.bilkent.joker.engine.config.PartitionServiceConfig;
import cs.bilkent.joker.engine.flow.PipelineId;
import cs.bilkent.joker.engine.flow.RegionDef;
import cs.bilkent.joker.engine.flow.RegionExecutionPlan;
import cs.bilkent.joker.engine.metric.FlowMetrics;
import cs.bilkent.joker.engine.metric.PipelineMetrics;
import cs.bilkent.joker.engine.metric.PipelineMetricsHistory;
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
import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertNull;
import static org.mockito.Matchers.anyListOf;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith( MockitoJUnitRunner.class )
public class JustInTimeAdaptationManagerTest extends AbstractJokerTest
{

    private final OperatorDef source = OperatorDefBuilder.newInstance( "source", StatefulOperatorInput0Output1.class ).build();

    private final OperatorDef sink = OperatorDefBuilder.newInstance( "sink", StatelessInput1Output1Operator.class ).build();

    private RegionDef sourceRegion;

    private RegionDef sinkRegion;

    private FlowDef flow;

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
    private PipelineMetricsHistory sourcePipelineMetricsHistory;

    @Mock
    private PipelineMetricsHistory sinkPipelineMetricsHistory;

    @Mock
    private PipelineMetrics sourcePipelineMetrics;

    @Mock
    private PipelineMetrics sinkPipelineMetrics;

    @Mock
    private FlowMetrics flowMetrics;

    @Mock
    private RegionAdaptationContext sourceContext;

    @Mock
    private RegionAdaptationContext sinkContext;

    @Mock
    private RegionExecutionPlan sourceRegionExecutionPlan;

    @Mock
    private RegionExecutionPlan sinkRegionExecutionPlan;

    private JustInTimeAdaptationManager adaptationManager;

    @Before
    public void init ()
    {
        when( config.getAdaptationConfig() ).thenReturn( adaptationConfig );
        when( adaptationConfig.getPipelineMetricsHistorySummarizer() ).thenReturn( pipelineMetricsHistorySummarizer );
        when( adaptationConfig.getLoadChangePredicate() ).thenReturn( loadChangePredicate );
        when( adaptationConfig.getBottleneckPredicate() ).thenReturn( bottleneckPredicate );
        when( adaptationConfig.getAdaptationEvaluationPredicate() ).thenReturn( adaptationEvaluationPredicate );

        final PartitionServiceConfig partitionServiceConfig = mock( PartitionServiceConfig.class );
        when( partitionServiceConfig.getMaxReplicaCount() ).thenReturn( 2 );
        when( config.getPartitionServiceConfig() ).thenReturn( partitionServiceConfig );

        adaptationManager = new JustInTimeAdaptationManager( config, regionAdaptationContextFactory );

        flow = new FlowDefBuilder().add( source ).add( sink ).connect( source.getId(), sink.getId() ).build();

        final RegionDefFormer regionDefFormer = new RegionDefFormerImpl( new IdGenerator() );
        final List<RegionDef> regions = regionDefFormer.createRegions( flow );

        sourceRegion = getRegion( regions, STATEFUL );
        sinkRegion = getRegion( regions, STATELESS );

        when( sourceContext.getRegionId() ).thenReturn( sourceRegion.getRegionId() );
        when( sinkContext.getRegionId() ).thenReturn( sinkRegion.getRegionId() );

        when( flowMetrics.getRegionMetrics( sourceRegion.getRegionId() ) ).thenReturn( singletonList( sourcePipelineMetricsHistory ) );
        when( flowMetrics.getRegionMetrics( sinkRegion.getRegionId() ) ).thenReturn( singletonList( sinkPipelineMetricsHistory ) );

        when( pipelineMetricsHistorySummarizer.summarize( sourcePipelineMetricsHistory ) ).thenReturn( sourcePipelineMetrics );
        when( pipelineMetricsHistorySummarizer.summarize( sinkPipelineMetricsHistory ) ).thenReturn( sinkPipelineMetrics );
    }

    @Test
    public void shouldInitRegionAdaptationContexts ()
    {
        when( sourceRegionExecutionPlan.getRegionDef() ).thenReturn( sourceRegion );
        when( sinkRegionExecutionPlan.getRegionDef() ).thenReturn( sinkRegion );
        when( sourceRegionExecutionPlan.getRegionId() ).thenReturn( sourceRegion.getRegionId() );
        when( sinkRegionExecutionPlan.getRegionId() ).thenReturn( sinkRegion.getRegionId() );
        when( regionAdaptationContextFactory.apply( sourceRegionExecutionPlan ) ).thenReturn( sourceContext );
        when( regionAdaptationContextFactory.apply( sinkRegionExecutionPlan ) ).thenReturn( sinkContext );

        adaptationManager.initialize( asList( sourceRegionExecutionPlan, sinkRegionExecutionPlan ) );

        assertThat( adaptationManager.getRegion( sourceRegion.getRegionId() ), equalTo( sourceContext ) );
        assertThat( adaptationManager.getRegion( sinkRegion.getRegionId() ), equalTo( sinkContext ) );
    }

    @Test
    public void shouldApplyFlowMetrics ()
    {
        shouldInitRegionAdaptationContexts();

        adaptationManager.apply( asList( sourceRegionExecutionPlan, sinkRegionExecutionPlan ), flowMetrics );

        verify( sourceContext ).updateRegionMetrics( sourceRegionExecutionPlan,
                                                     singletonList( sourcePipelineMetrics ),
                                                     loadChangePredicate );
        verify( sinkContext ).updateRegionMetrics( sinkRegionExecutionPlan, singletonList( sinkPipelineMetrics ), loadChangePredicate );

        verify( sourceContext ).resolveIfBottleneck( eq( bottleneckPredicate ), anyListOf( BottleneckResolver.class ) );
        verify( sinkContext ).resolveIfBottleneck( eq( bottleneckPredicate ), anyListOf( BottleneckResolver.class ) );
    }

    @Test
    public void shouldReturnAdaptationActionForSourceRegion ()
    {
        shouldInitRegionAdaptationContexts();

        final AdaptationAction action = mock( AdaptationAction.class );

        when( sourceContext.resolveIfBottleneck( eq( bottleneckPredicate ), anyListOf( BottleneckResolver.class ) ) ).thenReturn( action );

        final List<AdaptationAction> result = adaptationManager.apply( asList( sourceRegionExecutionPlan, sinkRegionExecutionPlan ),
                                                                       flowMetrics );

        assertThat( result, equalTo( singletonList( action ) ) );
        assertThat( adaptationManager.getAdaptingRegion(), equalTo( sourceContext ) );

        verify( sourceContext ).updateRegionMetrics( sourceRegionExecutionPlan,
                                                     singletonList( sourcePipelineMetrics ),
                                                     loadChangePredicate );
        verify( sinkContext ).updateRegionMetrics( sinkRegionExecutionPlan, singletonList( sinkPipelineMetrics ), loadChangePredicate );

        verify( sourceContext ).resolveIfBottleneck( eq( bottleneckPredicate ), anyListOf( BottleneckResolver.class ) );
        verify( sinkContext, never() ).resolveIfBottleneck( eq( bottleneckPredicate ), anyListOf( BottleneckResolver.class ) );
    }

    @Test
    public void shouldReturnAdaptationActionForSinkRegion ()
    {
        shouldInitRegionAdaptationContexts();

        final AdaptationAction action = mock( AdaptationAction.class );

        when( sinkContext.resolveIfBottleneck( eq( bottleneckPredicate ), anyListOf( BottleneckResolver.class ) ) ).thenReturn( action );

        final List<AdaptationAction> result = adaptationManager.apply( asList( sourceRegionExecutionPlan, sinkRegionExecutionPlan ),
                                                                       flowMetrics );

        assertThat( result, equalTo( singletonList( action ) ) );
        assertThat( adaptationManager.getAdaptingRegion(), equalTo( sinkContext ) );

        verify( sourceContext ).updateRegionMetrics( sourceRegionExecutionPlan,
                                                     singletonList( sourcePipelineMetrics ),
                                                     loadChangePredicate );
        verify( sinkContext ).updateRegionMetrics( sinkRegionExecutionPlan, singletonList( sinkPipelineMetrics ), loadChangePredicate );

        verify( sourceContext ).resolveIfBottleneck( eq( bottleneckPredicate ), anyListOf( BottleneckResolver.class ) );
        verify( sinkContext ).resolveIfBottleneck( eq( bottleneckPredicate ), anyListOf( BottleneckResolver.class ) );
    }

    @Test
    public void shouldNotRollbackAdaptation ()
    {
        shouldInitRegionAdaptationContexts();

        final AdaptationAction action = mock( AdaptationAction.class );

        when( sourceContext.resolveIfBottleneck( eq( bottleneckPredicate ), anyListOf( BottleneckResolver.class ) ) ).thenReturn( action );

        final List<AdaptationAction> result1 = adaptationManager.apply( asList( sourceRegionExecutionPlan, sinkRegionExecutionPlan ),
                                                                        flowMetrics );

        assertThat( result1, equalTo( singletonList( action ) ) );
        assertThat( adaptationManager.getAdaptingRegion(), equalTo( sourceContext ) );

        verify( sourceContext ).updateRegionMetrics( sourceRegionExecutionPlan,
                                                     singletonList( sourcePipelineMetrics ),
                                                     loadChangePredicate );
        verify( sinkContext ).updateRegionMetrics( sinkRegionExecutionPlan, singletonList( sinkPipelineMetrics ), loadChangePredicate );

        verify( sourceContext ).resolveIfBottleneck( eq( bottleneckPredicate ), anyListOf( BottleneckResolver.class ) );
        verify( sinkContext, never() ).resolveIfBottleneck( eq( bottleneckPredicate ), anyListOf( BottleneckResolver.class ) );

        final PipelineId sourcePipelineId = new PipelineId( sourceRegion.getRegionId(), 0 );
        when( sourceContext.getAdaptingPipelineId() ).thenReturn( sourcePipelineId );
        when( flowMetrics.getPipelineMetricsHistory( sourcePipelineId ) ).thenReturn( sourcePipelineMetricsHistory );

        final List<AdaptationAction> result2 = adaptationManager.apply( asList( sourceRegionExecutionPlan, sinkRegionExecutionPlan ),
                                                                        flowMetrics );

        assertThat( result2, hasSize( 0 ) );

        verify( sourceContext ).evaluateAdaptation( sourcePipelineMetrics, adaptationEvaluationPredicate );
    }

    @Test
    public void shouldRollbackAndContinueAdaptationIfNewActionFound ()
    {
        shouldInitRegionAdaptationContexts();

        final AdaptationAction action1 = mock( AdaptationAction.class );
        final AdaptationAction action2 = mock( AdaptationAction.class );

        when( sourceContext.resolveIfBottleneck( eq( bottleneckPredicate ), anyListOf( BottleneckResolver.class ) ) ).thenReturn( action1,
                                                                                                                                  action2 );

        final List<AdaptationAction> result1 = adaptationManager.apply( asList( sourceRegionExecutionPlan, sinkRegionExecutionPlan ),
                                                                        flowMetrics );

        assertThat( result1, equalTo( singletonList( action1 ) ) );
        assertThat( adaptationManager.getAdaptingRegion(), equalTo( sourceContext ) );

        final PipelineId sourcePipelineId = new PipelineId( sourceRegion.getRegionId(), 0 );
        when( sourceContext.getAdaptingPipelineId() ).thenReturn( sourcePipelineId );
        when( flowMetrics.getPipelineMetricsHistory( sourcePipelineId ) ).thenReturn( sourcePipelineMetricsHistory );

        final AdaptationAction rollback = mock( AdaptationAction.class );

        when( sourceContext.evaluateAdaptation( sourcePipelineMetrics, adaptationEvaluationPredicate ) ).thenReturn( rollback );

        final List<AdaptationAction> result2 = adaptationManager.apply( asList( sourceRegionExecutionPlan, sinkRegionExecutionPlan ),
                                                                        flowMetrics );

        assertThat( result2, equalTo( asList( rollback, action2 ) ) );
        assertThat( adaptationManager.getAdaptingRegion(), equalTo( sourceContext ) );

        verify( sourceContext ).evaluateAdaptation( sourcePipelineMetrics, adaptationEvaluationPredicate );
    }

    @Test
    public void shouldRollbackAndFinishAdaptationIfNewActionNotFound ()
    {
        shouldInitRegionAdaptationContexts();

        final AdaptationAction action = mock( AdaptationAction.class );

        when( sourceContext.resolveIfBottleneck( eq( bottleneckPredicate ), anyListOf( BottleneckResolver.class ) ) ).thenReturn( action,
                                                                                                                                  new AdaptationAction[] {
                                                                                                                                          null } );

        final List<AdaptationAction> result = adaptationManager.apply( asList( sourceRegionExecutionPlan, sinkRegionExecutionPlan ),
                                                                       flowMetrics );

        assertThat( result, equalTo( singletonList( action ) ) );
        assertThat( adaptationManager.getAdaptingRegion(), equalTo( sourceContext ) );

        final PipelineId sourcePipelineId = new PipelineId( sourceRegion.getRegionId(), 0 );
        when( sourceContext.getAdaptingPipelineId() ).thenReturn( sourcePipelineId );

        when( flowMetrics.getPipelineMetricsHistory( sourcePipelineId ) ).thenReturn( sourcePipelineMetricsHistory );

        final AdaptationAction rollback = mock( AdaptationAction.class );

        when( sourceContext.evaluateAdaptation( sourcePipelineMetrics, adaptationEvaluationPredicate ) ).thenReturn( rollback );

        final List<AdaptationAction> result2 = adaptationManager.apply( asList( sourceRegionExecutionPlan, sinkRegionExecutionPlan ),
                                                                        flowMetrics );

        assertThat( result2, equalTo( singletonList( rollback ) ) );
        assertNull( adaptationManager.getAdaptingRegion() );

        verify( sourceContext ).evaluateAdaptation( sourcePipelineMetrics, adaptationEvaluationPredicate );
    }

}
