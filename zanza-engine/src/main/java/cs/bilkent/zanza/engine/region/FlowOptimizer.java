package cs.bilkent.zanza.engine.region;

import java.util.List;

import cs.bilkent.zanza.flow.FlowDef;

public interface FlowOptimizer
{

    DeploymentDef optimize ( FlowDef flow, List<RegionDef> regions );

}
