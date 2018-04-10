package cs.bilkent.joker.engine.supervisor;

import cs.bilkent.joker.engine.pipeline.DownstreamCollector;
import cs.bilkent.joker.engine.pipeline.PipelineReplicaId;
import cs.bilkent.joker.engine.pipeline.UpstreamCtx;

public interface Supervisor
{

    /**
     * Returns the upstream context for the first operator of the pipeline specified with the given id
     *
     * @param id
     *         id of the pipeline of which the upstream context is requested
     *
     * @return the upstream context for the first operator of the pipeline specified with the given id
     */
    UpstreamCtx getUpstreamCtx ( PipelineReplicaId id );

    DownstreamCollector getDownstreamCollector ( PipelineReplicaId id );

    /**
     * Notifies that pipeline instance has completed running all of its operators.
     *
     * @param id
     *         id of the pipeline instance
     */
    void notifyPipelineReplicaCompleted ( PipelineReplicaId id );

    void notifyPipelineReplicaFailed ( PipelineReplicaId id, Throwable failure );

}
