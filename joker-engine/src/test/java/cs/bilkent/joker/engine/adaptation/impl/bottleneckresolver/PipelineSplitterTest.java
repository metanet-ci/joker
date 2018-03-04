package cs.bilkent.joker.engine.adaptation.impl.bottleneckresolver;

import java.util.function.BiFunction;

import org.junit.Before;
import org.junit.Test;

import cs.bilkent.joker.engine.adaptation.AdaptationAction;
import cs.bilkent.joker.engine.adaptation.impl.adaptationaction.SplitPipelineAction;
import cs.bilkent.joker.engine.flow.PipelineId;
import cs.bilkent.joker.engine.flow.RegionExecPlan;
import cs.bilkent.joker.engine.metric.PipelineMetrics;
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

    private final BiFunction<RegionExecPlan, PipelineMetrics, Integer> pipelineSplitIndexExtractor = mock( BiFunction.class );

    private final RegionExecPlan regionExecPlan = mock( RegionExecPlan.class );

    private final RegionExecPlan newRegionExecPlan = mock( RegionExecPlan.class );

    private final PipelineId pipelineId = new PipelineId( 0, 0 );

    private final PipelineMetrics bottleneckPipelineMetrics = mock( PipelineMetrics.class );

    private PipelineSplitter pipelineSplitter = new PipelineSplitter( pipelineSplitIndexExtractor );

    @Before
    public void init ()
    {
        when( bottleneckPipelineMetrics.getPipelineId() ).thenReturn( pipelineId );
    }

    @Test
    public void shouldNotSplitSingleOperatorPipeline ()
    {
        when( regionExecPlan.getOperatorCountByPipelineStartIndex( pipelineId.getPipelineStartIndex() ) ).thenReturn( 1 );
        when( regionExecPlan.withSplitPipeline( singletonList( 0 ) ) ).thenReturn( newRegionExecPlan );

        final AdaptationAction action = pipelineSplitter.resolve( regionExecPlan, bottleneckPipelineMetrics );

        assertNull( action );
    }

    @Test
    public void shouldNotSplitWhenExtractorReturnsNoSplit ()
    {
        when( regionExecPlan.getOperatorCountByPipelineStartIndex( pipelineId.getPipelineStartIndex() ) ).thenReturn( 2 );
        when( pipelineSplitIndexExtractor.apply( regionExecPlan, bottleneckPipelineMetrics ) ).thenReturn( 0 );

        final AdaptationAction action = pipelineSplitter.resolve( regionExecPlan, bottleneckPipelineMetrics );

        assertNull( action );
    }

    @Test
    public void shouldSplitWhenExtractorReturnsSplitIndex ()
    {
        final int pipelineSplitIndex = 1;
        when( regionExecPlan.getOperatorCountByPipelineStartIndex( pipelineId.getPipelineStartIndex() ) ).thenReturn( 2 );
        when( pipelineSplitIndexExtractor.apply( regionExecPlan, bottleneckPipelineMetrics ) ).thenReturn( pipelineSplitIndex );
        when( regionExecPlan.withSplitPipeline( asList( 0, pipelineSplitIndex ) ) ).thenReturn( newRegionExecPlan );

        final AdaptationAction action = pipelineSplitter.resolve( regionExecPlan, bottleneckPipelineMetrics );

        assertTrue( action instanceof SplitPipelineAction );
        final SplitPipelineAction splitPipelineAction = (SplitPipelineAction) action;
        assertThat( splitPipelineAction.getCurrentExecPlan(), equalTo( regionExecPlan ) );
        assertThat( splitPipelineAction.getNewExecPlan(), equalTo( newRegionExecPlan ) );
    }

}
