package cs.bilkent.zanza.engine.region;

import cs.bilkent.zanza.flow.FlowDefinition;

public interface RegionManager
{

    RegionInstance createRegion ( final FlowDefinition flow, final RegionRuntimeConfig regionConfig );

}
