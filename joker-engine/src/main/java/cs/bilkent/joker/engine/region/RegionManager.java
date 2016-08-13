package cs.bilkent.joker.engine.region;

import cs.bilkent.joker.flow.FlowDef;

public interface RegionManager
{

    Region createRegion ( final FlowDef flow, final RegionConfig regionConfig );

    void releaseRegion ( int regionId );

}
