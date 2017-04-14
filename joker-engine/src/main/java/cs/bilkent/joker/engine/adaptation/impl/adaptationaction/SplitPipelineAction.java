package cs.bilkent.joker.engine.adaptation.impl.adaptationaction;

import java.util.ArrayList;
import java.util.List;

import cs.bilkent.joker.engine.adaptation.AdaptationAction;
import cs.bilkent.joker.engine.adaptation.AdaptationPerformer;
import cs.bilkent.joker.engine.flow.PipelineId;
import cs.bilkent.joker.engine.flow.RegionExecutionPlan;
import static cs.bilkent.joker.engine.region.impl.RegionExecutionPlanUtil.getPipelineStartIndicesToSplit;
import static cs.bilkent.joker.impl.com.google.common.base.Preconditions.checkArgument;
import static cs.bilkent.joker.impl.com.google.common.base.Preconditions.checkState;
import static java.util.Collections.singletonList;
import static java.util.Collections.unmodifiableList;

public class SplitPipelineAction implements AdaptationAction
{

    private final RegionExecutionPlan currentRegionExecutionPlan, newRegionExecutionPlan;

    private final PipelineId pipelineId;

    private final List<Integer> pipelineOperatorIndices;

    public SplitPipelineAction ( final RegionExecutionPlan regionExecutionPlan,
                                 final PipelineId pipelineId,
                                 final int pipelineOperatorIndex )
    {
        this( regionExecutionPlan, pipelineId, singletonList( pipelineOperatorIndex ) );
    }

    public SplitPipelineAction ( final RegionExecutionPlan regionExecutionPlan,
                                 final PipelineId pipelineId,
                                 final List<Integer> pipelineOperatorIndices )
    {
        checkArgument( regionExecutionPlan != null );
        checkArgument( pipelineId != null );
        checkArgument( pipelineOperatorIndices != null && pipelineOperatorIndices.size() > 0 );
        this.currentRegionExecutionPlan = regionExecutionPlan;
        final List<Integer> pipelineStartIndicesToSplit = getPipelineStartIndicesToSplit( regionExecutionPlan,
                                                                                          pipelineId,
                                                                                          pipelineOperatorIndices );
        this.newRegionExecutionPlan = regionExecutionPlan.withSplitPipeline( pipelineStartIndicesToSplit );
        checkState( newRegionExecutionPlan != null );
        this.pipelineId = pipelineId;
        this.pipelineOperatorIndices = unmodifiableList( new ArrayList<>( pipelineOperatorIndices ) );
    }

    @Override
    public void apply ( final AdaptationPerformer performer )
    {
        performer.splitPipeline( pipelineId, pipelineOperatorIndices );
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
        return new MergePipelinesAction( newRegionExecutionPlan, getMergePipelineIds() );
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
        return "SplitPipelineAction{" + "currentRegionExecutionPlan=" + currentRegionExecutionPlan + ", newRegionExecutionPlan="
               + newRegionExecutionPlan + ", pipelineId=" + pipelineId + ", pipelineOperatorIndices=" + pipelineOperatorIndices + '}';
    }

}
