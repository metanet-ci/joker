package cs.bilkent.joker.engine.adaptation.impl.adaptationaction;

import java.util.ArrayList;
import java.util.List;

import cs.bilkent.joker.engine.adaptation.AdaptationAction;
import cs.bilkent.joker.engine.adaptation.AdaptationPerformer;
import cs.bilkent.joker.engine.flow.PipelineId;
import cs.bilkent.joker.engine.flow.RegionExecPlan;
import static cs.bilkent.joker.engine.region.impl.RegionExecPlanUtil.getPipelineStartIndicesToSplit;
import static cs.bilkent.joker.impl.com.google.common.base.Preconditions.checkArgument;
import static cs.bilkent.joker.impl.com.google.common.base.Preconditions.checkState;
import static java.util.Collections.singletonList;
import static java.util.Collections.unmodifiableList;

public class SplitPipelineAction implements AdaptationAction
{

    private final RegionExecPlan currentExecPlan, newExecPlan;

    private final PipelineId pipelineId;

    private final List<Integer> pipelineOperatorIndices;

    public SplitPipelineAction ( final RegionExecPlan execPlan, final PipelineId pipelineId, final int pipelineOperatorIndex )
    {
        this( execPlan, pipelineId, singletonList( pipelineOperatorIndex ) );
    }

    public SplitPipelineAction ( final RegionExecPlan execPlan, final PipelineId pipelineId, final List<Integer> pipelineOperatorIndices )
    {
        checkArgument( execPlan != null );
        checkArgument( pipelineId != null );
        checkArgument( pipelineOperatorIndices != null && pipelineOperatorIndices.size() > 0 );
        this.currentExecPlan = execPlan;
        final List<Integer> pipelineStartIndicesToSplit = getPipelineStartIndicesToSplit( execPlan, pipelineId, pipelineOperatorIndices );
        this.newExecPlan = execPlan.withSplitPipeline( pipelineStartIndicesToSplit );
        checkState( newExecPlan != null );
        this.pipelineId = pipelineId;
        this.pipelineOperatorIndices = unmodifiableList( new ArrayList<>( pipelineOperatorIndices ) );
    }

    @Override
    public void apply ( final AdaptationPerformer performer )
    {
        performer.splitPipeline( pipelineId, pipelineOperatorIndices );
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
        return new MergePipelinesAction( newExecPlan, getMergePipelineIds() );
    }

    private List<PipelineId> getMergePipelineIds ()
    {
        // op0, op1, op2, op3, op4, op5
        // 0  , 1  , 2  , 3  , 4  , 5
        // merge: (2, 4, 5)
        // split: (2, 3)
        final List<PipelineId> pipelineIds = new ArrayList<>();
        final int regionId = pipelineId.getRegionId();
        final int pipelineStartIndex = pipelineId.getPipelineStartIndex();
        pipelineIds.add( pipelineId );
        pipelineOperatorIndices.stream().map( i -> new PipelineId( regionId, pipelineStartIndex + i ) ).forEach( pipelineIds::add );

        return pipelineIds;
    }

    @Override
    public String toString ()
    {
        return "SplitPipelineAction{" + "currentExecPlan=" + currentExecPlan + ", newExecPlan=" + newExecPlan + ", pipelineId=" + pipelineId
               + ", pipelineOperatorIndices=" + pipelineOperatorIndices + '}';
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

        final SplitPipelineAction that = (SplitPipelineAction) o;

        if ( !currentExecPlan.equals( that.currentExecPlan ) )
        {
            return false;
        }
        if ( !pipelineId.equals( that.pipelineId ) )
        {
            return false;
        }
        return pipelineOperatorIndices.equals( that.pipelineOperatorIndices );
    }

    @Override
    public int hashCode ()
    {
        int result = currentExecPlan.hashCode();
        result = 31 * result + pipelineId.hashCode();
        result = 31 * result + pipelineOperatorIndices.hashCode();
        return result;
    }

}
