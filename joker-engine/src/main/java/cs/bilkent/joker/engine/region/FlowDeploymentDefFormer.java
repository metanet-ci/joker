package cs.bilkent.joker.engine.region;

import java.util.List;

import cs.bilkent.joker.flow.FlowDef;

public interface FlowDeploymentDefFormer
{

    FlowDeploymentDef createFlowDeploymentDef ( FlowDef flow, List<RegionDef> regions );

}
