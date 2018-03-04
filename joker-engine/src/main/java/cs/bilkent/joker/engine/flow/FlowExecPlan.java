package cs.bilkent.joker.engine.flow;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import cs.bilkent.joker.flow.FlowDef;
import cs.bilkent.joker.operator.OperatorDef;
import static java.util.Collections.unmodifiableList;
import static java.util.stream.Collectors.joining;

/**
 * Represents an execution plan as a list of {@link RegionExecPlan} objects, which are determined for a given {@link FlowDef} object.
 * Each flow execution plan contains a version number. It is incremented once a new execution plan is created from the current one.
 * <p>
 * A {@link FlowDef} object is divided into regions, based on types, connections and port schemas of the operators in it. Each region is
 * monitored and scaled independently by the runtime.
 *
 * @see RegionExecPlan
 */
public class FlowExecPlan
{

    private final int version;

    private final FlowDef flow;

    private final List<RegionExecPlan> regionExecPlans;

    public FlowExecPlan ( final int version, final FlowDef flow, final Collection<RegionExecPlan> regionExecPlans )
    {
        this.version = version;
        this.flow = flow;
        this.regionExecPlans = unmodifiableList( new ArrayList<>( regionExecPlans ) );
    }

    /**
     * Returns version of the current execution model deployed by the runtime
     *
     * @return version of the current execution model deployed by the runtime
     */
    public int getVersion ()
    {
        return version;
    }

    /**
     * Returns the flow object which is being executed with the current {@link FlowExecPlan} object.
     *
     * @return the flow object which is being executed with the current {@link FlowExecPlan} object.
     */
    public FlowDef getFlow ()
    {
        return flow;
    }

    /**
     * Returns execution plans of the regions determined from the given {@link FlowDef} object.
     *
     * @return execution plans of the regions determined from the given {@link FlowDef} object.
     */
    public List<RegionExecPlan> getRegionExecPlans ()
    {
        return regionExecPlans;
    }

    /**
     * Returns the execution plan for the region specified with {@code regionId} parameter.
     *
     * @param regionId
     *         region id to get the execution plan
     *
     * @return the execution plan for the region specified with {@code regionId} parameter.
     */
    public RegionExecPlan getRegionExecPlan ( final int regionId )
    {
        for ( RegionExecPlan regionExecPlan : regionExecPlans )
        {
            if ( regionExecPlan.getRegionId() == regionId )
            {
                return regionExecPlan;
            }
        }

        return null;
    }

    /**
     * Returns the execution plan for the region which contains the operator given with {@code operatorId} parameter.
     *
     * @param operatorId
     *         operator id to find the region
     *
     * @return the execution plan for the region which contains the operator given with {@code operatorId} parameter.
     */
    public RegionExecPlan getRegionExecPlan ( final String operatorId )
    {
        for ( RegionExecPlan regionExecPlan : regionExecPlans )
        {
            for ( OperatorDef operatorDef : regionExecPlan.getRegionDef().getOperators() )
            {
                if ( operatorDef.getId().equals( operatorId ) )
                {
                    return regionExecPlan;
                }
            }
        }

        return null;
    }

    /**
     * Returns id of the pipeline which contains the operator given with {@code operatorId} parameter.
     *
     * @param operatorId
     *         operator id to find the pipeline
     *
     * @return id of the pipeline which contains the operator given with {@code operatorId} parameter.
     */
    public PipelineId getOperatorPipeline ( final String operatorId )
    {
        for ( RegionExecPlan regionExecPlan : regionExecPlans )
        {
            for ( PipelineId pipelineId : regionExecPlan.getPipelineIds() )
            {
                for ( OperatorDef operatorDef : regionExecPlan.getOperatorDefsByPipelineIndex( pipelineId.getPipelineStartIndex() ) )
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

    public int getRegionCount ()
    {
        return regionExecPlans.size();
    }

    public String toSummaryString ()
    {
        return regionExecPlans.stream().map( RegionExecPlan::toPlanSummaryString ).collect( joining( ",", "{", "}" ) );
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

        final FlowExecPlan that = (FlowExecPlan) o;

        return version == that.version && flow.equals( that.flow ) && regionExecPlans.equals( that.regionExecPlans );
    }

    @Override
    public int hashCode ()
    {
        int result = version;
        result = 31 * result + flow.hashCode();
        result = 31 * result + regionExecPlans.hashCode();
        return result;
    }

}
