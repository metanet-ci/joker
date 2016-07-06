package cs.bilkent.zanza.engine.region;

import java.util.List;

import cs.bilkent.zanza.flow.FlowDef;

public interface RegionDefFormer
{

    List<RegionDef> createRegions ( FlowDef flowDef );

}
