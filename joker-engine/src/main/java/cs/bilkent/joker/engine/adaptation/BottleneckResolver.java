package cs.bilkent.joker.engine.adaptation;

import cs.bilkent.joker.engine.flow.RegionExecutionPlan;
import cs.bilkent.joker.engine.metric.PipelineMetrics;

@FunctionalInterface
public interface BottleneckResolver
{

    AdaptationAction resolve ( RegionExecutionPlan regionExecutionPlan, PipelineMetrics bottleneckPipelineMetrics );

}
