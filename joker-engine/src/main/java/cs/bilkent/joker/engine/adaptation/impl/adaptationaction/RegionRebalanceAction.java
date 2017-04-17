package cs.bilkent.joker.engine.adaptation.impl.adaptationaction;

import cs.bilkent.joker.engine.adaptation.AdaptationAction;
import cs.bilkent.joker.engine.adaptation.AdaptationPerformer;
import cs.bilkent.joker.engine.flow.RegionExecutionPlan;
import static cs.bilkent.joker.impl.com.google.common.base.Preconditions.checkArgument;
import static cs.bilkent.joker.operator.spec.OperatorType.PARTITIONED_STATEFUL;

public class RegionRebalanceAction implements AdaptationAction
{

    private final RegionExecutionPlan currentRegionExecutionPlan, newRegionExecutionPlan;

    public RegionRebalanceAction ( final RegionExecutionPlan regionExecutionPlan, final int newReplicaCount )
    {
        checkArgument( regionExecutionPlan != null );
        checkArgument( newReplicaCount > 0 );
        checkArgument( regionExecutionPlan.getReplicaCount() != newReplicaCount );
        checkArgument( regionExecutionPlan.getRegionType() == PARTITIONED_STATEFUL );
        this.currentRegionExecutionPlan = regionExecutionPlan;
        this.newRegionExecutionPlan = regionExecutionPlan.withNewReplicaCount( newReplicaCount );
    }

    @Override
    public void apply ( final AdaptationPerformer performer )
    {
        performer.rebalanceRegion( newRegionExecutionPlan.getRegionId(), newRegionExecutionPlan.getReplicaCount() );
    }

    @Override
    public RegionExecutionPlan getCurrentRegionExecutionPlan ()
    {
        return currentRegionExecutionPlan;
    }

    @Override
    public RegionExecutionPlan getNewRegionExecutionPlan ()
    {
        return newRegionExecutionPlan;
    }

    @Override
    public AdaptationAction rollback ()
    {
        return new RegionRebalanceAction( newRegionExecutionPlan, currentRegionExecutionPlan.getReplicaCount() );
    }

    @Override
    public String toString ()
    {
        return "RegionRebalanceAction{" + "currentRegionExecutionPlan=" + currentRegionExecutionPlan + ", newRegionExecutionPlan="
               + newRegionExecutionPlan + '}';
    }

}