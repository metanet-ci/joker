package cs.bilkent.joker.engine.pipeline;

import java.util.List;

import cs.bilkent.joker.engine.FlowStatus;
import cs.bilkent.joker.engine.region.FlowDeploymentDef;
import cs.bilkent.joker.engine.region.RegionConfig;
import cs.bilkent.joker.engine.supervisor.Supervisor;

public interface PipelineManager
{

    void start ( Supervisor supervisor, FlowDeploymentDef flowDeployment, List<RegionConfig> regionConfigs );

    void triggerShutdown ();

    void mergePipelines ( Supervisor supervisor, List<PipelineId> pipelineIds );

    UpstreamContext getUpstreamContext ( PipelineReplicaId id );

    boolean handlePipelineReplicaCompleted ( PipelineReplicaId id );

    void handlePipelineReplicaFailed ( PipelineReplicaId id, Throwable failure );

    FlowStatus getFlowStatus ();

}
