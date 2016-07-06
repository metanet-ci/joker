package cs.bilkent.zanza.engine;


import javax.inject.Inject;

import cs.bilkent.zanza.engine.region.RegionDefFormer;
import cs.bilkent.zanza.engine.supervisor.Supervisor;

public class ZanzaEngine
{

    private final RegionDefFormer regionDefFormer;

    private final Supervisor supervisor;

    @Inject
    public ZanzaEngine ( final RegionDefFormer regionDefFormer, final Supervisor supervisor )
    {
        this.regionDefFormer = regionDefFormer;
        this.supervisor = supervisor;
    }

}
