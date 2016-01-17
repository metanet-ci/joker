package cs.bilkent.zanza.engine.region;

import java.util.List;

import cs.bilkent.zanza.flow.FlowDefinition;
import cs.bilkent.zanza.region.RegionDefinition;

public interface RegionFormer
{

    List<RegionDefinition> createRegions ( FlowDefinition flowDefinition );

}
