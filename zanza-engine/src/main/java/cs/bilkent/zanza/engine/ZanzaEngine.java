package cs.bilkent.zanza.engine;


import java.util.List;
import java.util.concurrent.Future;
import javax.inject.Inject;

import cs.bilkent.zanza.engine.exception.InitializationException;
import cs.bilkent.zanza.engine.region.RegionConfig;
import cs.bilkent.zanza.engine.region.RegionConfigFactory;
import cs.bilkent.zanza.engine.region.RegionDef;
import cs.bilkent.zanza.engine.region.RegionDefFormer;
import cs.bilkent.zanza.engine.supervisor.impl.SupervisorImpl;
import cs.bilkent.zanza.flow.FlowDef;

public class ZanzaEngine
{

    private final RegionDefFormer regionDefFormer;

    private final RegionConfigFactory regionConfigFactory;

    private final SupervisorImpl supervisor;

    @Inject
    public ZanzaEngine ( final RegionDefFormer regionDefFormer,
                         final RegionConfigFactory regionConfigFactory,
                         final SupervisorImpl supervisor )
    {
        this.regionDefFormer = regionDefFormer;
        this.regionConfigFactory = regionConfigFactory;
        this.supervisor = supervisor;
    }

    public void start ( final FlowDef flow ) throws InitializationException
    {
        final List<RegionDef> regions = regionDefFormer.createRegions( flow );
        final List<RegionConfig> regionConfigs = regionConfigFactory.createRegionConfigs( flow, regions );
        supervisor.start( flow, regionConfigs );
    }

    public Future<Void> shutdown ()
    {
        return supervisor.shutdown();
    }

}
