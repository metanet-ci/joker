package cs.bilkent.zanza.engine.region;

import java.util.List;

import cs.bilkent.zanza.flow.FlowDef;

public interface RegionConfigFactory
{

    List<RegionConfig> createRegionConfigs ( FlowDef flow, List<RegionDef> regions );

}
