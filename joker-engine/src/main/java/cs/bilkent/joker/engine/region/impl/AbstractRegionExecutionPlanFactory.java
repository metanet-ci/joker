package cs.bilkent.joker.engine.region.impl;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import cs.bilkent.joker.engine.config.JokerConfig;
import cs.bilkent.joker.engine.flow.RegionDef;
import cs.bilkent.joker.engine.flow.RegionExecutionPlan;
import cs.bilkent.joker.engine.region.RegionExecutionPlanFactory;
import static cs.bilkent.joker.engine.util.ExceptionUtils.checkInterruption;
import cs.bilkent.joker.operator.OperatorDef;
import static cs.bilkent.joker.operator.spec.OperatorType.STATEFUL;

public abstract class AbstractRegionExecutionPlanFactory implements RegionExecutionPlanFactory
{

    final int maxReplicaCount;

    protected AbstractRegionExecutionPlanFactory ( JokerConfig jokerConfig )
    {
        this.maxReplicaCount = jokerConfig.getFlowDefOptimizerConfig().getMaxReplicaCount();
    }

    @Override
    public List<RegionExecutionPlan> createRegionExecutionPlans ( final List<RegionDef> regionDefs )
    {
        try
        {
            init();
            final List<RegionExecutionPlan> regionExecutionPlans = new ArrayList<>();
            for ( RegionDef regionDef : regionDefs )
            {
                final RegionExecutionPlan regionExecutionPlan = createRegionExecutionPlan( regionDef );

                final int replicaCount = regionExecutionPlan.getReplicaCount();
                validateReplicaCount( regionDef, replicaCount );
                validatePipelineStartIndices( regionDef.getOperators(), regionExecutionPlan.getPipelineStartIndices() );

                regionExecutionPlans.add( regionExecutionPlan );
            }

            return regionExecutionPlans;
        }
        finally
        {
            try
            {
                destroy();
            }
            catch ( Exception e )
            {
                checkInterruption( e );
            }
        }

    }

    protected abstract RegionExecutionPlan createRegionExecutionPlan ( RegionDef regionDef );

    final void validatePipelineStartIndices ( final List<OperatorDef> operators, final List<Integer> pipelineStartIndices )
    {
        if ( pipelineStartIndices.get( 0 ) != 0 )
        {
            pipelineStartIndices.add( 0, 0 );
        }
        checkArgument( pipelineStartIndices.size() <= operators.size() );
        int i = -1;
        for ( int startIndex : pipelineStartIndices )
        {
            checkArgument( startIndex > i, "invalid pipeline start indices: ", pipelineStartIndices );
            i = startIndex;
        }
        checkArgument( i < operators.size() );
    }

    private void validateReplicaCount ( final RegionDef region, final int replicaCount )
    {
        if ( region.getRegionType() != STATEFUL )
        {
            checkArgument( replicaCount > 0 && replicaCount <= maxReplicaCount, "replica count must be between 0 and %s", maxReplicaCount );
        }
        else
        {
            checkArgument( replicaCount == 1, "Replica count cannot be %s for %s region", replicaCount, region.getRegionType() );
        }
    }

}
