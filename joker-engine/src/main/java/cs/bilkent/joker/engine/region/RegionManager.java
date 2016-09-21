package cs.bilkent.joker.engine.region;

import java.util.List;

import cs.bilkent.joker.engine.pipeline.PipelineId;
import cs.bilkent.joker.flow.FlowDef;

public interface RegionManager
{

    Region createRegion ( FlowDef flow, RegionConfig regionConfig );

    Region mergePipelines ( int regionId, List<PipelineId> pipelineIdsToMerge );

    void releaseRegion ( int regionId );

}
