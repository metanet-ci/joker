package cs.bilkent.zanza.engine.pipeline;

import java.util.List;

import cs.bilkent.zanza.engine.region.RegionConfig;
import cs.bilkent.zanza.engine.supervisor.Supervisor;
import cs.bilkent.zanza.flow.FlowDef;

public interface PipelineManager
{

    List<Pipeline> createPipelines ( Supervisor supervisor, FlowDef flow, List<RegionConfig> regionConfigs );

    void shutdown ();

}
