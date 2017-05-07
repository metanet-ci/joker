package cs.bilkent.joker.engine.flow;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import cs.bilkent.joker.flow.FlowDef;
import cs.bilkent.joker.operator.OperatorDef;
import static java.util.Collections.unmodifiableList;
import static java.util.stream.Collectors.joining;

/**
 * Represents an execution plan as a list of {@link RegionExecutionPlan} objects, which are determined for a given {@link FlowDef} object.
 * Each flow execution plan contains a version number. It is incremented once a new execution plan is created from the current one.
 * <p>
 * A {@link FlowDef} object is divided into regions, based on types, connections and port schemas of the operators in it. Each region is
 * monitored and scaled independently by the runtime.
 *
 * @see RegionExecutionPlan
 */
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
     * Returns the flow object which is being executed with the current {@link FlowExecutionPlan} object.
     *
     * @return the flow object which is being executed with the current {@link FlowExecutionPlan} object.
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
    public List<RegionExecutionPlan> getRegionExecutionPlans ()
    {
        return regionExecutionPlans;
    }

    /**
     * Returns the execution plan for the region specified with {@code regionId} parameter.
     *
     * @param regionId
     *         region id to get the execution plan
     *
     * @return the execution plan for the region specified with {@code regionId} parameter.
     */
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

    /**
     * Returns the execution plan for the region which contains the operator given with {@code operatorId} parameter.
     *
     * @param operatorId
     *         operator id to find the region
     *
     * @return the execution plan for the region which contains the operator given with {@code operatorId} parameter.
     */
    public RegionExecutionPlan getRegionExecutionPlan ( final String operatorId )
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

    public String toPlanSummaryString ()
    {
        return regionExecutionPlans.stream().map( RegionExecutionPlan::toPlanSummaryString ).collect( joining( ",", "{", "}" ) );
    }

}
