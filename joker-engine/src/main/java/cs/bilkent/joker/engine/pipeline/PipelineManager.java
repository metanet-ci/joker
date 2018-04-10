package cs.bilkent.joker.engine.pipeline;

import java.util.List;

import cs.bilkent.joker.engine.FlowStatus;
import cs.bilkent.joker.engine.flow.FlowExecPlan;
import cs.bilkent.joker.engine.flow.PipelineId;
import cs.bilkent.joker.engine.flow.RegionExecPlan;
import cs.bilkent.joker.engine.metric.PipelineMeter;
import cs.bilkent.joker.flow.FlowDef;

public interface PipelineManager
{

    void start ( FlowDef flow, List<RegionExecPlan> regionExecPlans );

    FlowExecPlan getFlowExecPlan ();

    List<PipelineMeter> getAllPipelineMetersOrFail ();

    List<PipelineMeter> getRegionPipelineMetersOrFail ( int regionId );

    PipelineMeter getPipelineMeterOrFail ( PipelineId pipelineId );

    void triggerShutdown ();

    void mergePipelines ( int flowVersion, List<PipelineId> pipelineIds );

    void splitPipeline ( int flowVersion, PipelineId pipelineId, List<Integer> pipelineOperatorIndices );

    void rebalanceRegion ( int flowVersion, int regionId, int newReplicaCount );

    UpstreamCtx getUpstreamCtx ( PipelineReplicaId id );

    DownstreamCollector getDownstreamCollector ( PipelineReplicaId id );

    boolean handlePipelineReplicaCompleted ( PipelineReplicaId id );

    void handlePipelineReplicaFailed ( PipelineReplicaId id, Throwable failure );

    FlowStatus getFlowStatus ();

}
