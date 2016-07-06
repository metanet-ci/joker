package cs.bilkent.zanza.engine.pipeline;

import java.util.List;

import cs.bilkent.zanza.engine.region.RegionRuntimeConfig;
import cs.bilkent.zanza.engine.supervisor.Supervisor;
import cs.bilkent.zanza.flow.FlowDef;

public interface PipelineRuntimeManager
{

    List<PipelineRuntimeState> createPipelineRuntimeStates ( Supervisor supervisor, FlowDef flow,
                                                             List<RegionRuntimeConfig> regionRuntimeConfigs );

}
