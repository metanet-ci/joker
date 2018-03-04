package cs.bilkent.joker.engine.adaptation.impl.adaptationaction;

import cs.bilkent.joker.engine.adaptation.AdaptationAction;
import cs.bilkent.joker.engine.adaptation.AdaptationPerformer;
import cs.bilkent.joker.engine.flow.RegionExecPlan;
import static cs.bilkent.joker.impl.com.google.common.base.Preconditions.checkArgument;
import static cs.bilkent.joker.operator.spec.OperatorType.PARTITIONED_STATEFUL;

public class RegionRebalanceAction implements AdaptationAction
{

    private final RegionExecPlan currentExecPlan, newExecPlan;

    private final int newReplicaCount;

    public RegionRebalanceAction ( final RegionExecPlan execPlan, final int newReplicaCount )
    {
        checkArgument( execPlan != null );
        checkArgument( newReplicaCount > 0 );
        checkArgument( execPlan.getReplicaCount() != newReplicaCount );
        checkArgument( execPlan.getRegionType() == PARTITIONED_STATEFUL );
        this.currentExecPlan = execPlan;
        this.newReplicaCount = newReplicaCount;
        this.newExecPlan = execPlan.withNewReplicaCount( newReplicaCount );
    }

    @Override
    public void apply ( final AdaptationPerformer performer )
    {
        performer.rebalanceRegion( newExecPlan.getRegionId(), newExecPlan.getReplicaCount() );
    }

    @Override
    public RegionExecPlan getCurrentExecPlan ()
    {
        return currentExecPlan;
    }

    @Override
    public RegionExecPlan getNewExecPlan ()
    {
        return newExecPlan;
    }

    @Override
    public AdaptationAction revert ()
    {
        return new RegionRebalanceAction( newExecPlan, currentExecPlan.getReplicaCount() );
    }

    @Override
    public String toString ()
    {
        return "RegionRebalanceAction{" + "currentExecPlan=" + currentExecPlan + ", newExecPlan=" + newExecPlan + '}';
    }

    @Override
    public boolean equals ( final Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }

        final RegionRebalanceAction that = (RegionRebalanceAction) o;

        if ( newReplicaCount != that.newReplicaCount )
        {
            return false;
        }
        return currentExecPlan.equals( that.currentExecPlan );
    }

    @Override
    public int hashCode ()
    {
        int result = currentExecPlan.hashCode();
        result = 31 * result + newReplicaCount;
        return result;
    }

}
