package cs.bilkent.zanza.engine;


import javax.inject.Inject;

import cs.bilkent.zanza.engine.region.RegionDefinitionFormer;
import cs.bilkent.zanza.engine.supervisor.Supervisor;

public class ZanzaEngine
{

    private final RegionDefinitionFormer regionDefinitionFormer;

    private final Supervisor supervisor;

    @Inject
    public ZanzaEngine ( final RegionDefinitionFormer regionDefinitionFormer, final Supervisor supervisor )
    {
        this.regionDefinitionFormer = regionDefinitionFormer;
        this.supervisor = supervisor;
    }

}
