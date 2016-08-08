package cs.bilkent.zanza.engine;


import java.util.List;
import java.util.concurrent.Future;
import javax.inject.Inject;

import cs.bilkent.zanza.engine.exception.InitializationException;
import cs.bilkent.zanza.engine.region.FlowDeploymentDef;
import cs.bilkent.zanza.engine.region.FlowDeploymentDefFormer;
import cs.bilkent.zanza.engine.region.RegionConfig;
import cs.bilkent.zanza.engine.region.RegionConfigFactory;
import cs.bilkent.zanza.engine.region.RegionDef;
import cs.bilkent.zanza.engine.region.RegionDefFormer;
import cs.bilkent.zanza.engine.supervisor.impl.SupervisorImpl;
import cs.bilkent.zanza.flow.FlowDef;

public class ZanzaEngine
{

    private final RegionDefFormer regionDefFormer;

    private final FlowDeploymentDefFormer flowDeploymentDefFormer;

    private final RegionConfigFactory regionConfigFactory;

    private final SupervisorImpl supervisor;

    @Inject
    public ZanzaEngine ( final RegionDefFormer regionDefFormer, final FlowDeploymentDefFormer flowDeploymentDefFormer,
                         final RegionConfigFactory regionConfigFactory,
                         final SupervisorImpl supervisor )
    {
        this.regionDefFormer = regionDefFormer;
        this.flowDeploymentDefFormer = flowDeploymentDefFormer;
        this.regionConfigFactory = regionConfigFactory;
        this.supervisor = supervisor;
    }

    public void start ( final FlowDef flow ) throws InitializationException
    {
        final List<RegionDef> regions = regionDefFormer.createRegions( flow );
        final FlowDeploymentDef flowDeployment = flowDeploymentDefFormer.createFlowDeploymentDef( flow, regions );
        final List<RegionConfig> regionConfigs = regionConfigFactory.createRegionConfigs( flowDeployment );
        supervisor.start( flowDeployment, regionConfigs );
    }

    public Future<Void> shutdown ()
    {
        return supervisor.shutdown();
    }

}
