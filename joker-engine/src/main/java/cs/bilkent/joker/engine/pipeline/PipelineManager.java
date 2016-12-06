package cs.bilkent.joker.engine.pipeline;

import java.util.List;

import cs.bilkent.joker.engine.FlowStatus;
import cs.bilkent.joker.engine.region.RegionConfig;
import cs.bilkent.joker.engine.region.RegionDef;
import cs.bilkent.joker.engine.supervisor.Supervisor;
import cs.bilkent.joker.flow.FlowDef;

public interface PipelineManager
{

    void start ( Supervisor supervisor, FlowDef flow, List<RegionDef> regionDefs, List<RegionConfig> regionConfigs );

    void triggerShutdown ();

    void mergePipelines ( Supervisor supervisor, List<PipelineId> pipelineIds );

    void splitPipeline ( Supervisor supervisor, PipelineId pipelineId, List<Integer> pipelineOperatorIndices );

    void rebalanceRegion ( Supervisor supervisor, int regionId, int newReplicaCount );

    UpstreamContext getUpstreamContext ( PipelineReplicaId id );

    DownstreamTupleSender getDownstreamTupleSender ( PipelineReplicaId id );

    boolean handlePipelineReplicaCompleted ( PipelineReplicaId id );

    void handlePipelineReplicaFailed ( PipelineReplicaId id, Throwable failure );

    FlowStatus getFlowStatus ();

}
