package cs.bilkent.joker.engine.adaptation;

import cs.bilkent.joker.engine.flow.RegionExecPlan;

public interface AdaptationAction
{

    void apply ( AdaptationPerformer performer );

    RegionExecPlan getCurrentExecPlan ();

    RegionExecPlan getNewExecPlan ();

    AdaptationAction revert ();

}
