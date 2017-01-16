package cs.bilkent.joker.engine.metric;

import java.util.List;

import cs.bilkent.joker.engine.pipeline.PipelineId;

public interface MetricManager
{

    void start ( int flowVersion, List<PipelineMeter> pipelineMeters );

    void pause ();

    void resume ( int flowVersion, List<PipelineId> pipelineIdsToRemove, List<PipelineMeter> newPipelineMeters );

    void shutdown ();

}
