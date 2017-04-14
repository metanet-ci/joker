package cs.bilkent.joker.engine.adaptation.impl.bottleneckresolver;

import java.util.function.BiFunction;

import org.junit.Before;
import org.junit.Test;

import cs.bilkent.joker.engine.adaptation.AdaptationAction;
import cs.bilkent.joker.engine.adaptation.impl.adaptationaction.SplitPipelineAction;
import cs.bilkent.joker.engine.flow.PipelineId;
import cs.bilkent.joker.engine.flow.RegionExecutionPlan;
import cs.bilkent.joker.engine.metric.PipelineMetricsSnapshot;
import cs.bilkent.joker.test.AbstractJokerTest;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PipelineSplitterTest extends AbstractJokerTest
{

    private final BiFunction<RegionExecutionPlan, PipelineMetricsSnapshot, Integer> pipelineSplitIndexExtractor = mock( BiFunction.class );

    private final RegionExecutionPlan regionExecutionPlan = mock( RegionExecutionPlan.class );

    private final RegionExecutionPlan newRegionExecutionPlan = mock( RegionExecutionPlan.class );

    private final PipelineId pipelineId = new PipelineId( 0, 0 );

    private final PipelineMetricsSnapshot bottleneckPipelineMetrics = mock( PipelineMetricsSnapshot.class );

    private PipelineSplitter pipelineSplitter = new PipelineSplitter( pipelineSplitIndexExtractor );

    @Before
    public void init ()
    {
        when( bottleneckPipelineMetrics.getPipelineId() ).thenReturn( pipelineId );
    }

    @Test
    public void shouldNotSplitSingleOperatorPipeline ()
    {
        when( regionExecutionPlan.getOperatorCountByPipelineStartIndex( pipelineId.getPipelineStartIndex() ) ).thenReturn( 1 );
        when( regionExecutionPlan.withSplitPipeline( singletonList( 0 ) ) ).thenReturn( newRegionExecutionPlan );

        final AdaptationAction action = pipelineSplitter.resolve( regionExecutionPlan, bottleneckPipelineMetrics );

        assertNull( action );
    }

    @Test
    public void shouldNotSplitWhenExtractorReturnsNoSplit ()
    {
        when( regionExecutionPlan.getOperatorCountByPipelineStartIndex( pipelineId.getPipelineStartIndex() ) ).thenReturn( 2 );
        when( pipelineSplitIndexExtractor.apply( regionExecutionPlan, bottleneckPipelineMetrics ) ).thenReturn( 0 );

        final AdaptationAction action = pipelineSplitter.resolve( regionExecutionPlan, bottleneckPipelineMetrics );

        assertNull( action );
    }

    @Test
    public void shouldSplitWhenExtractorReturnsSplitIndex ()
    {
        final int pipelineSplitIndex = 1;
        when( regionExecutionPlan.getOperatorCountByPipelineStartIndex( pipelineId.getPipelineStartIndex() ) ).thenReturn( 2 );
        when( pipelineSplitIndexExtractor.apply( regionExecutionPlan, bottleneckPipelineMetrics ) ).thenReturn( pipelineSplitIndex );
        when( regionExecutionPlan.withSplitPipeline( asList( 0, pipelineSplitIndex ) ) ).thenReturn( newRegionExecutionPlan );

        final AdaptationAction action = pipelineSplitter.resolve( regionExecutionPlan, bottleneckPipelineMetrics );

        assertTrue( action instanceof SplitPipelineAction );
        final SplitPipelineAction splitPipelineAction = (SplitPipelineAction) action;
        assertThat( splitPipelineAction.getCurrentRegionExecutionPlan(), equalTo( regionExecutionPlan ) );
        assertThat( splitPipelineAction.getNewRegionExecutionPlan(), equalTo( newRegionExecutionPlan ) );
    }

}
