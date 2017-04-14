package cs.bilkent.joker.engine.adaptation;

import cs.bilkent.joker.engine.flow.RegionExecutionPlan;
import cs.bilkent.joker.engine.metric.PipelineMetricsSnapshot;

@FunctionalInterface
public interface BottleneckResolver
{

    AdaptationAction resolve ( RegionExecutionPlan regionExecutionPlan, PipelineMetricsSnapshot bottleneckPipelineMetrics );

}
