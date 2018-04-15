package cs.bilkent.joker.engine.metric;

import java.util.List;

import cs.bilkent.joker.engine.flow.PipelineId;
import cs.bilkent.joker.flow.FlowDef;

public interface MetricManager
{

    void start ( int flowVersion, List<PipelineMeter> pipelineMeters );

    void pause ();

    void update ( int flowVersion, List<PipelineId> pipelineIdsToRemove, List<PipelineMeter> newPipelineMeters );

    void resume ();

    FlowMetrics getMetrics ();

    LatencyMeter createLatencyMeter ( FlowDef flow, String sinkOperatorId, int replicaIndex );

    void shutdown ();

}
