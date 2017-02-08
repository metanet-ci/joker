package cs.bilkent.joker.engine.flow;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import cs.bilkent.joker.engine.pipeline.PipelineId;
import cs.bilkent.joker.flow.FlowDef;
import cs.bilkent.joker.operator.OperatorDef;
import static java.util.Collections.unmodifiableList;

public class FlowExecutionPlan
{

    private final int version;

    private final FlowDef flow;

    private final List<RegionExecutionPlan> regionExecutionPlans;

    public FlowExecutionPlan ( final int version, final FlowDef flow, final Collection<RegionExecutionPlan> regionExecutionPlans )
    {
        this.version = version;
        this.flow = flow;
        this.regionExecutionPlans = unmodifiableList( new ArrayList<>( regionExecutionPlans ) );
    }

    public int getVersion ()
    {
        return version;
    }

    public FlowDef getFlow ()
    {
        return flow;
    }

    public List<RegionExecutionPlan> getRegionExecutionPlans ()
    {
        return regionExecutionPlans;
    }

    public RegionExecutionPlan getRegionExecutionPlan ( final int regionId )
    {
        for ( RegionExecutionPlan regionExecutionPlan : regionExecutionPlans )
        {
            if ( regionExecutionPlan.getRegionId() == regionId )
            {
                return regionExecutionPlan;
            }
        }

        return null;
    }

    public RegionExecutionPlan getOperatorRegionExecutionPlan ( final String operatorId )
    {
        for ( RegionExecutionPlan regionExecutionPlan : regionExecutionPlans )
        {
            for ( OperatorDef operatorDef : regionExecutionPlan.getRegionDef().getOperators() )
            {
                if ( operatorDef.getId().equals( operatorId ) )
                {
                    return regionExecutionPlan;
                }
            }
        }

        return null;
    }

    public PipelineId getOperatorPipeline ( final String operatorId )
    {
        for ( RegionExecutionPlan regionExecutionPlan : regionExecutionPlans )
        {
            for ( PipelineId pipelineId : regionExecutionPlan.getPipelineIds() )
            {
                for ( OperatorDef operatorDef : regionExecutionPlan.getOperatorDefsByPipelineIndex( pipelineId.getPipelineStartIndex() ) )
                {
                    if ( operatorDef.getId().equals( operatorId ) )
                    {
                        return pipelineId;
                    }
                }
            }
        }

        return null;
    }

}
