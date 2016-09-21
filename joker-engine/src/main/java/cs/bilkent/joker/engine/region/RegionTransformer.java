package cs.bilkent.joker.engine.region;

import java.util.List;

public interface RegionTransformer
{

    Region mergePipelines ( final Region region, final List<Integer> startIndicesToMerge );

    boolean checkPipelineStartIndicesToMerge ( final RegionConfig regionConfig, final List<Integer> pipelineStartIndicesToMerge );

}
