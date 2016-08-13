package cs.bilkent.joker.engine.pipeline;

import java.util.List;

import cs.bilkent.joker.engine.region.RegionConfig;
import cs.bilkent.joker.engine.supervisor.Supervisor;
import cs.bilkent.joker.flow.FlowDef;

public interface PipelineManager
{

    List<Pipeline> createPipelines ( Supervisor supervisor, FlowDef flow, List<RegionConfig> regionConfigs );

    void shutdown ();

}
