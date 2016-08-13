package cs.bilkent.joker.engine.region;

import java.util.List;

import cs.bilkent.joker.flow.FlowDef;

public interface RegionDefFormer
{

    List<RegionDef> createRegions ( FlowDef flowDef );

}
