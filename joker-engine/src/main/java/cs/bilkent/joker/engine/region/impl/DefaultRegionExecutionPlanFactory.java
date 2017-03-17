package cs.bilkent.joker.engine.region.impl;

import javax.inject.Inject;
import javax.inject.Singleton;

import cs.bilkent.joker.engine.config.JokerConfig;
import cs.bilkent.joker.engine.flow.RegionDef;
import cs.bilkent.joker.engine.flow.RegionExecutionPlan;
import static java.util.Collections.singletonList;

@Singleton
public class DefaultRegionExecutionPlanFactory extends AbstractRegionExecutionPlanFactory
{

    @Inject
    public DefaultRegionExecutionPlanFactory ( final JokerConfig jokerConfig )
    {
        super( jokerConfig );
    }

    @Override
    protected RegionExecutionPlan createRegionExecutionPlan ( final RegionDef regionDef )
    {
        return new RegionExecutionPlan( regionDef, singletonList( 0 ), 1 );
    }

}
