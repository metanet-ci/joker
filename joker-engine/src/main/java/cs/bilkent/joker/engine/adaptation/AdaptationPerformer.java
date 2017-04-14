package cs.bilkent.joker.engine.adaptation;

import java.util.List;

import cs.bilkent.joker.engine.flow.PipelineId;

public interface AdaptationPerformer
{

    void mergePipelines ( List<PipelineId> pipelineIds );

    void splitPipeline ( PipelineId pipelineId, List<Integer> pipelineOperatorIndices );

    void rebalanceRegion ( int regionId, int newReplicaCount );

}
