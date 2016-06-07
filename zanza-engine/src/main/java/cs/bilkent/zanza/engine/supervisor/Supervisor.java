package cs.bilkent.zanza.engine.supervisor;

import cs.bilkent.zanza.engine.pipeline.PipelineInstanceId;
import cs.bilkent.zanza.engine.pipeline.UpstreamContext;

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
    UpstreamContext getUpstreamContext ( PipelineInstanceId id );

    /**
     * Notifies that pipeline instance has completed running of its last operator but still has running operators.
     *
     * @param id
     *         id of the pipeline instance
     */
    void notifyPipelineStoppedSendingDownstreamTuples ( PipelineInstanceId id );

    /**
     * Notifies that pipeline instance has completed running all of its operators.
     *
     * @param id
     *         id of the pipeline instance
     */
    void notifyPipelineCompletedRunning ( PipelineInstanceId id );

}
