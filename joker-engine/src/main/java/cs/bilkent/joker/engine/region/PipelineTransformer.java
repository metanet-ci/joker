package cs.bilkent.joker.engine.region;

import java.util.List;

public interface PipelineTransformer
{

    Region mergePipelines ( Region region, List<Integer> startIndicesToMerge );

    boolean checkPipelineStartIndicesToMerge ( RegionConfig regionConfig, List<Integer> pipelineStartIndicesToMerge );

    Region splitPipeline ( Region region, List<Integer> indicesToSplit );

    boolean checkPipelineStartIndicesToSplit ( final RegionConfig regionConfig, final List<Integer> pipelineStartIndicesToSplit );

}
