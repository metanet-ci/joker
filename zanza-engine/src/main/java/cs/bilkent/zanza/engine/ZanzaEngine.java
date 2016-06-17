package cs.bilkent.zanza.engine;


import javax.inject.Inject;

import cs.bilkent.zanza.engine.region.RegionFormer;
import cs.bilkent.zanza.engine.supervisor.Supervisor;

public class ZanzaEngine
{

    private final RegionFormer regionFormer;

    private final Supervisor supervisor;

    @Inject
    public ZanzaEngine ( final RegionFormer regionFormer, final Supervisor supervisor )
    {
        this.regionFormer = regionFormer;
        this.supervisor = supervisor;
    }

}
