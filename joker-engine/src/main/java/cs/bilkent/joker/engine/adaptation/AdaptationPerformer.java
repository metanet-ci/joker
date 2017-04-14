package cs.bilkent.joker.engine.adaptation;

import java.util.List;

import cs.bilkent.joker.engine.flow.PipelineId;
import cs.bilkent.joker.utils.Pair;

public interface AdaptationPerformer
{

    Pair<List<PipelineId>, List<PipelineId>> mergePipelines ( List<PipelineId> pipelineIds );

    Pair<List<PipelineId>, List<PipelineId>> splitPipeline ( PipelineId pipelineId, List<Integer> pipelineOperatorIndices );

    Pair<List<PipelineId>, List<PipelineId>> rebalanceRegion ( int regionId, int newReplicaCount );

}
