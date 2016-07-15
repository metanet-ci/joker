package cs.bilkent.zanza.engine.supervisor;

import java.util.List;

import cs.bilkent.zanza.engine.pipeline.PipelineReplicaId;
import cs.bilkent.zanza.engine.pipeline.UpstreamContext;
import cs.bilkent.zanza.engine.region.RegionRuntimeConfig;
import cs.bilkent.zanza.flow.FlowDef;

public interface Supervisor
{

    void deploy ( FlowDef flow, List<RegionRuntimeConfig> regionRuntimeConfigs );

    /**
     * Returns the upstream context for the first operator of the pipeline specified with the given id
     *
     * @param id
     *         id of the pipeline of which the upstream context is requested
     *
     * @return the upstream context for the first operator of the pipeline specified with the given id
     */
    UpstreamContext getUpstreamContext ( PipelineReplicaId id );

    /**
     * Notifies that pipeline instance has completed running all of its operators.
     *
     * @param id
     *         id of the pipeline instance
     */
    void notifyPipelineCompletedRunning ( PipelineReplicaId id );

}
