package cs.bilkent.joker.engine.adaptation;

import cs.bilkent.joker.engine.flow.RegionExecutionPlan;

public interface AdaptationAction
{

    void apply ( AdaptationPerformer performer );

    RegionExecutionPlan getCurrentRegionExecutionPlan ();

    RegionExecutionPlan getNewRegionExecutionPlan ();

    AdaptationAction revert ();

}
