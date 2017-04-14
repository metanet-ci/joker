package cs.bilkent.joker.engine.adaptation;

import java.util.List;

import cs.bilkent.joker.engine.flow.RegionExecutionPlan;
import cs.bilkent.joker.engine.metric.FlowMetricsSnapshot;
import cs.bilkent.joker.flow.FlowDef;

public interface AdaptationManager
{

    void initialize ( FlowDef flow, List<RegionExecutionPlan> regionExecutionPlans );

    List<AdaptationAction> apply ( List<RegionExecutionPlan> regionExecutionPlans, FlowMetricsSnapshot flowMetrics );

}
