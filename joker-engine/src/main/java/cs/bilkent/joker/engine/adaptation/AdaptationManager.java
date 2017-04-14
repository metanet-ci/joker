package cs.bilkent.joker.engine.adaptation;

import java.util.List;

import cs.bilkent.joker.engine.flow.RegionExecutionPlan;
import cs.bilkent.joker.engine.metric.FlowMetricsSnapshot;

public interface AdaptationManager
{

    void initialize ( List<RegionExecutionPlan> regionExecutionPlans );

    List<AdaptationAction> apply ( List<RegionExecutionPlan> regionExecutionPlans, FlowMetricsSnapshot flowMetrics );

}
