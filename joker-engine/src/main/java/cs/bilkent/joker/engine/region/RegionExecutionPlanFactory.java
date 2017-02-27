package cs.bilkent.joker.engine.region;

import java.util.List;

import cs.bilkent.joker.engine.flow.RegionDef;
import cs.bilkent.joker.engine.flow.RegionExecutionPlan;

public interface RegionExecutionPlanFactory
{

    default void init ()
    {

    }

    default void destroy ()
    {

    }

    List<RegionExecutionPlan> createRegionExecutionPlans ( List<RegionDef> regionDefs );

}
