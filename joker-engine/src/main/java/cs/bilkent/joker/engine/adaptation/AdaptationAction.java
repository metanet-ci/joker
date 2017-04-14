package cs.bilkent.joker.engine.adaptation;

import java.util.List;

import cs.bilkent.joker.engine.flow.PipelineId;
import cs.bilkent.joker.engine.flow.RegionExecutionPlan;
import cs.bilkent.joker.utils.Pair;

public interface AdaptationAction
{

    Pair<List<PipelineId>, List<PipelineId>> apply ( AdaptationPerformer performer );

    RegionExecutionPlan getCurrentRegionExecutionPlan ();

    RegionExecutionPlan getNewRegionExecutionPlan ();

    AdaptationAction rollback ();

}
