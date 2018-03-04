package cs.bilkent.joker.engine.region;

import java.util.List;

import cs.bilkent.joker.engine.flow.RegionDef;
import cs.bilkent.joker.engine.flow.RegionExecPlan;

public interface RegionExecPlanFactory
{

    default void init ()
    {

    }

    default void destroy ()
    {

    }

    List<RegionExecPlan> createRegionExecPlans ( List<RegionDef> regionDefs );

}
