package cs.bilkent.joker.engine.region;

import java.util.List;

public interface PipelineTransformer
{

    Region mergePipelines ( Region region, List<Integer> startIndicesToMerge );

    Region splitPipeline ( Region region, List<Integer> indicesToSplit );

}
