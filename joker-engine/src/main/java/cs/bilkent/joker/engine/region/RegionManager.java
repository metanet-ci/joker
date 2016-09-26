package cs.bilkent.joker.engine.region;

import java.util.List;

import cs.bilkent.joker.engine.pipeline.PipelineId;
import cs.bilkent.joker.flow.FlowDef;

public interface RegionManager
{

    Region createRegion ( FlowDef flow, RegionConfig regionConfig );

    List<PipelineId> getMergeablePipelineIds ( final List<PipelineId> pipelineIds );

    Region mergePipelines ( List<PipelineId> pipelineIdsToMerge );

    void releaseRegion ( int regionId );

}
