package cs.bilkent.zanza.engine;


import java.util.List;
import java.util.concurrent.Future;
import javax.inject.Inject;

import static com.google.common.base.Preconditions.checkArgument;
import cs.bilkent.zanza.engine.exception.InitializationException;
import cs.bilkent.zanza.engine.region.RegionConfig;
import cs.bilkent.zanza.engine.region.RegionConfigFactory;
import cs.bilkent.zanza.engine.region.RegionDef;
import cs.bilkent.zanza.engine.region.RegionDefFormer;
import cs.bilkent.zanza.engine.supervisor.impl.SupervisorImpl;
import static cs.bilkent.zanza.engine.util.RegionUtil.sortTopologically;
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
        final List<RegionDef> sorted = sortTopologically( flow.getOperatorsMap(), flow.getAllConnections(), regions );
        final List<RegionConfig> regionConfigs = regionConfigFactory.createRegionConfigs( flow, sorted );
        failIfMissingRegionConfigExists( regions, regionConfigs );
        supervisor.start( flow, regionConfigs );
    }

    public Future<Void> shutdown ()
    {
        return supervisor.shutdown();
    }

    private void failIfMissingRegionConfigExists ( final List<RegionDef> regions, final List<RegionConfig> regionConfigs )
    {
        checkArgument( regions != null );
        checkArgument( regionConfigs != null );
        checkArgument( regions.size() == regionConfigs.size(),
                       "mismatching regions size %s and region configs size %s",
                       regions.size(),
                       regionConfigs.size() );
        for ( final RegionDef region : regions )
        {
            checkArgument( regionConfigs.stream()
                                        .filter( regionConfig -> region.equals( regionConfig.getRegionDef() ) )
                                        .findFirst()
                                        .isPresent(), "no region config found for region: %s", region );
        }
    }

}
