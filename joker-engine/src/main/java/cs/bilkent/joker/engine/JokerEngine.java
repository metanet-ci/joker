package cs.bilkent.joker.engine;


import java.util.List;
import java.util.concurrent.Future;
import javax.inject.Inject;

import cs.bilkent.joker.engine.exception.InitializationException;
import cs.bilkent.joker.engine.pipeline.PipelineId;
import cs.bilkent.joker.engine.region.FlowDeploymentDef;
import cs.bilkent.joker.engine.region.FlowDeploymentDefFormer;
import cs.bilkent.joker.engine.region.RegionConfig;
import cs.bilkent.joker.engine.region.RegionConfigFactory;
import cs.bilkent.joker.engine.region.RegionDef;
import cs.bilkent.joker.engine.region.RegionDefFormer;
import cs.bilkent.joker.engine.supervisor.impl.SupervisorImpl;
import cs.bilkent.joker.flow.FlowDef;

public class JokerEngine
{

    private final RegionDefFormer regionDefFormer;

    private final FlowDeploymentDefFormer flowDeploymentDefFormer;

    private final RegionConfigFactory regionConfigFactory;

    private final SupervisorImpl supervisor;

    @Inject
    public JokerEngine ( final RegionDefFormer regionDefFormer,
                         final FlowDeploymentDefFormer flowDeploymentDefFormer,
                         final RegionConfigFactory regionConfigFactory,
                         final SupervisorImpl supervisor )
    {
        this.regionDefFormer = regionDefFormer;
        this.flowDeploymentDefFormer = flowDeploymentDefFormer;
        this.regionConfigFactory = regionConfigFactory;
        this.supervisor = supervisor;
    }

    public void run ( final FlowDef flow ) throws InitializationException
    {
        final List<RegionDef> regions = regionDefFormer.createRegions( flow );
        final FlowDeploymentDef flowDeployment = flowDeploymentDefFormer.createFlowDeploymentDef( flow, regions );
        final List<RegionConfig> regionConfigs = regionConfigFactory.createRegionConfigs( flowDeployment );
        supervisor.start( flowDeployment, regionConfigs );
    }

    public FlowStatus getStatus ()
    {
        return supervisor.getFlowStatus();
    }

    public Future<Void> shutdown ()
    {
        return supervisor.shutdown();
    }

    public Future<Void> mergePipelines ( final List<PipelineId> pipelineIds )
    {
        return supervisor.mergePipelines( pipelineIds );
    }

    public Future<Void> splitPipeline ( final PipelineId pipelineId, final List<Integer> pipelineOperatorIndices )
    {
        return supervisor.splitPipeline( pipelineId, pipelineOperatorIndices );
    }

    public Future<Void> rebalanceRegion ( final int regionId, final int newReplicaCount )
    {
        return supervisor.rebalanceRegion( regionId, newReplicaCount );
    }

}
