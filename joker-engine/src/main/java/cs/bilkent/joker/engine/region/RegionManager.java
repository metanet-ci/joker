package cs.bilkent.joker.engine.region;

import java.util.List;

import cs.bilkent.joker.engine.flow.RegionExecutionPlan;
import cs.bilkent.joker.engine.pipeline.PipelineId;
import cs.bilkent.joker.flow.FlowDef;

public interface RegionManager
{

    Region createRegion ( FlowDef flow, RegionExecutionPlan regionExecutionPlan );

    void validatePipelineMergeParameters ( List<PipelineId> pipelineIds );

    Region mergePipelines ( List<PipelineId> pipelineIdsToMerge );

    void validatePipelineSplitParameters ( PipelineId pipelineId, List<Integer> pipelineOperatorIndicesToSplit );

    Region splitPipeline ( PipelineId pipelineId, List<Integer> pipelineOperatorIndicesToSplit );

    Region rebalanceRegion ( FlowDef flow, int regionId, int replicaCount );

    void releaseRegion ( int regionId );

}
