package cs.bilkent.joker.engine.adaptation;

import java.util.List;

import cs.bilkent.joker.engine.flow.RegionExecutionPlan;
import cs.bilkent.joker.engine.metric.FlowMetrics;
import cs.bilkent.joker.flow.FlowDef;

public interface AdaptationManager
{

    void initialize ( FlowDef flowDef, List<RegionExecutionPlan> regionExecutionPlans );

    void disableAdaptation ();

    List<AdaptationAction> adapt ( List<RegionExecutionPlan> regionExecutionPlans, FlowMetrics flowMetrics );

}
