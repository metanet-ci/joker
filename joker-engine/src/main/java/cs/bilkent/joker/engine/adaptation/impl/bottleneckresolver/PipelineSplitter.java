package cs.bilkent.joker.engine.adaptation.impl.bottleneckresolver;

import java.util.function.BiFunction;

import cs.bilkent.joker.engine.adaptation.AdaptationAction;
import cs.bilkent.joker.engine.adaptation.BottleneckResolver;
import cs.bilkent.joker.engine.adaptation.impl.adaptationaction.SplitPipelineAction;
import cs.bilkent.joker.engine.flow.PipelineId;
import cs.bilkent.joker.engine.flow.RegionExecutionPlan;
import cs.bilkent.joker.engine.metric.PipelineMetrics;

public class PipelineSplitter implements BottleneckResolver
{

    private final BiFunction<RegionExecutionPlan, PipelineMetrics, Integer> pipelineSplitIndexExtractor;

    public PipelineSplitter ( final BiFunction<RegionExecutionPlan, PipelineMetrics, Integer> pipelineSplitIndexExtractor )
    {
        this.pipelineSplitIndexExtractor = pipelineSplitIndexExtractor;
    }

    @Override
    public AdaptationAction resolve ( final RegionExecutionPlan regionExecutionPlan, final PipelineMetrics bottleneckPipelineMetrics )
    {
        final PipelineId pipelineId = bottleneckPipelineMetrics.getPipelineId();

        final int operatorCount = regionExecutionPlan.getOperatorCountByPipelineStartIndex( pipelineId.getPipelineStartIndex() );
        if ( operatorCount < 2 )
        {
            return null;
        }

        final int splitIndex = pipelineSplitIndexExtractor.apply( regionExecutionPlan, bottleneckPipelineMetrics );

        return splitIndex > 0 ? new SplitPipelineAction( regionExecutionPlan, pipelineId, splitIndex ) : null;
    }

}
