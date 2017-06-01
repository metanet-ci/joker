package cs.bilkent.joker.engine.adaptation;

import java.util.List;

import cs.bilkent.joker.engine.flow.PipelineId;
import cs.bilkent.joker.engine.flow.RegionExecutionPlan;
import cs.bilkent.joker.engine.metric.PipelineMetrics;
import cs.bilkent.joker.utils.Pair;

@FunctionalInterface
public interface BottleneckResolver
{

    List<Pair<AdaptationAction, List<PipelineId>>> resolve ( RegionExecutionPlan regionExecutionPlan, List<PipelineMetrics>
                                                                                                              bottleneckPipelinesMetrics );

}
