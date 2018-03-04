package cs.bilkent.joker.engine.adaptation;

import java.util.List;

import cs.bilkent.joker.engine.flow.RegionExecPlan;
import cs.bilkent.joker.engine.metric.FlowMetrics;
import cs.bilkent.joker.flow.FlowDef;

public interface AdaptationManager
{

    void initialize ( FlowDef flowDef, List<RegionExecPlan> execPlans );

    void disableAdaptation ();

    List<AdaptationAction> adapt ( List<RegionExecPlan> execPlans, FlowMetrics metrics );

}
