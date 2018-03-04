package cs.bilkent.joker.engine.region.impl;

import javax.inject.Inject;
import javax.inject.Singleton;

import cs.bilkent.joker.engine.config.JokerConfig;
import cs.bilkent.joker.engine.flow.RegionDef;
import cs.bilkent.joker.engine.flow.RegionExecPlan;
import static java.util.Collections.singletonList;

@Singleton
public class DefaultRegionExecPlanFactory extends AbstractRegionExecPlanFactory
{

    @Inject
    public DefaultRegionExecPlanFactory ( final JokerConfig jokerConfig )
    {
        super( jokerConfig );
    }

    @Override
    protected RegionExecPlan createRegionExecPlan ( final RegionDef regionDef )
    {
        return new RegionExecPlan( regionDef, singletonList( 0 ), 1 );
    }

}
