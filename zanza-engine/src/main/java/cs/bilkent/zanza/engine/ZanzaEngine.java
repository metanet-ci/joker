package cs.bilkent.zanza.engine;


import java.util.List;
import java.util.concurrent.Future;
import javax.inject.Inject;

import cs.bilkent.zanza.engine.exception.InitializationException;
import cs.bilkent.zanza.engine.region.RegionConfig;
import cs.bilkent.zanza.engine.region.RegionDefFormer;
import cs.bilkent.zanza.engine.supervisor.impl.SupervisorImpl;
import cs.bilkent.zanza.flow.FlowDef;

public class ZanzaEngine
{

    private final RegionDefFormer regionDefFormer;

    private final SupervisorImpl supervisor;

    @Inject
    public ZanzaEngine ( final RegionDefFormer regionDefFormer, final SupervisorImpl supervisor )
    {
        this.regionDefFormer = regionDefFormer;
        this.supervisor = supervisor;
    }

    public void deploy ( final FlowDef flow, final List<RegionConfig> regionConfigs ) throws InitializationException
    {
        supervisor.deploy( flow, regionConfigs );
    }

    public Future<Void> shutdown ()
    {
        return supervisor.shutdown();
    }

}
