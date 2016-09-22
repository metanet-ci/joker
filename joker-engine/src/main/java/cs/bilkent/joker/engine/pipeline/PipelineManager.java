package cs.bilkent.joker.engine.pipeline;

import java.util.List;

import cs.bilkent.joker.engine.FlowStatus;
import cs.bilkent.joker.engine.region.FlowDeploymentDef;
import cs.bilkent.joker.engine.region.RegionConfig;
import cs.bilkent.joker.engine.supervisor.Supervisor;

public interface PipelineManager
{

    void start ( final Supervisor supervisor, final FlowDeploymentDef flowDeployment, final List<RegionConfig> regionConfigs );

    void triggerShutdown ();

    UpstreamContext getUpstreamContext ( final PipelineReplicaId id );

    boolean notifyPipelineReplicaCompleted ( final PipelineReplicaId id );

    void notifyPipelineReplicaFailed ( final PipelineReplicaId id, final Throwable failure );

    FlowStatus getFlowStatus ();

}
