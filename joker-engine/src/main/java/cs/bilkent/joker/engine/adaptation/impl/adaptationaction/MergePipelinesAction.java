package cs.bilkent.joker.engine.adaptation.impl.adaptationaction;

import java.util.ArrayList;
import java.util.List;

import cs.bilkent.joker.engine.adaptation.AdaptationAction;
import cs.bilkent.joker.engine.adaptation.AdaptationPerformer;
import cs.bilkent.joker.engine.flow.PipelineId;
import cs.bilkent.joker.engine.flow.RegionExecPlan;
import static cs.bilkent.joker.engine.region.impl.RegionExecPlanUtil.getMergeablePipelineStartIndices;
import static cs.bilkent.joker.impl.com.google.common.base.Preconditions.checkArgument;
import static java.util.stream.Collectors.toList;

public class MergePipelinesAction implements AdaptationAction
{

    private final RegionExecPlan currentExecPlan, newExecPlan;

    private final List<PipelineId> pipelineIds = new ArrayList<>();

    public MergePipelinesAction ( final RegionExecPlan execPlan, final List<PipelineId> pipelineIds )
    {
        checkArgument( execPlan != null );
        checkArgument( pipelineIds != null && pipelineIds.size() > 0 );
        this.currentExecPlan = execPlan;
        final List<Integer> startIndicesToMerge = getMergeablePipelineStartIndices( execPlan, pipelineIds );
        this.newExecPlan = execPlan.withMergedPipelines( startIndicesToMerge );
        int pipelineStartIndex = -1;
        for ( PipelineId pipelineId : pipelineIds )
        {
            checkArgument( execPlan.getRegionId() == pipelineId.getRegionId() );
            checkArgument( pipelineId.getPipelineStartIndex() > pipelineStartIndex );
            pipelineStartIndex = pipelineId.getPipelineStartIndex();
        }

        this.pipelineIds.addAll( pipelineIds );
    }

    @Override
    public void apply ( final AdaptationPerformer performer )
    {
        performer.mergePipelines( pipelineIds );
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
        final List<Integer> pipelineOperatorIndices = getSplitIndices();
        return new SplitPipelineAction( newExecPlan, pipelineIds.get( 0 ), pipelineOperatorIndices );
    }

    private List<Integer> getSplitIndices ()
    {
        // op0, op1, op2, op3, op4, op5
        // 0  , 1  , 2  , 3  , 4  , 5
        // merge: (2, 4, 5)
        // split: (2, 3)
        final int base = pipelineIds.get( 0 ).getPipelineStartIndex();

        return pipelineIds.stream().map( p -> ( p.getPipelineStartIndex() - base ) ).filter( i -> i > 0 ).collect( toList() );
    }

    @Override
    public String toString ()
    {
        return "MergePipelineAction{" + "currentExecPlan=" + currentExecPlan + ", newExecPlan=" + newExecPlan + ", pipelineIds="
               + pipelineIds + '}';
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

        final MergePipelinesAction action = (MergePipelinesAction) o;

        if ( !currentExecPlan.equals( action.currentExecPlan ) )
        {
            return false;
        }
        return pipelineIds.equals( action.pipelineIds );
    }

    @Override
    public int hashCode ()
    {
        int result = currentExecPlan.hashCode();
        result = 31 * result + pipelineIds.hashCode();
        return result;
    }

}
