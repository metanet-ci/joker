package cs.bilkent.joker.engine.region;

import java.util.List;

import cs.bilkent.joker.engine.flow.RegionExecutionPlan;

public interface PipelineTransformer
{

    Region mergePipelines ( Region region, List<Integer> startIndicesToMerge );

    boolean checkPipelineStartIndicesToMerge ( RegionExecutionPlan regionExecutionPlan, List<Integer> pipelineStartIndicesToMerge );

    Region splitPipeline ( Region region, List<Integer> indicesToSplit );

    boolean checkPipelineStartIndicesToSplit ( final RegionExecutionPlan regionExecutionPlan,
                                               final List<Integer> pipelineStartIndicesToSplit );

}
