package cs.bilkent.zanza.engine.coordinator;

import cs.bilkent.zanza.engine.pipeline.PipelineInstanceId;

/**
 * Delivers pipeline instances' notifications to coordinator. Implementations can deliver notifications
 * synchronously or asynchronously.
 */
public interface CoordinatorHandle
{

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
