package cs.bilkent.joker.engine;


import java.util.List;
import java.util.concurrent.Future;
import javax.inject.Inject;

import cs.bilkent.joker.engine.exception.InitializationException;
import cs.bilkent.joker.engine.flow.FlowExecutionPlan;
import cs.bilkent.joker.engine.flow.RegionExecutionPlan;
import cs.bilkent.joker.engine.pipeline.PipelineId;
import cs.bilkent.joker.engine.region.FlowDefOptimizer;
import cs.bilkent.joker.engine.region.RegionDef;
import cs.bilkent.joker.engine.region.RegionDefFormer;
import cs.bilkent.joker.engine.region.RegionExecutionPlanFactory;
import cs.bilkent.joker.engine.supervisor.impl.SupervisorImpl;
import cs.bilkent.joker.flow.FlowDef;
import cs.bilkent.joker.utils.Pair;

public class JokerEngine
{

    private final RegionDefFormer regionDefFormer;

    private final FlowDefOptimizer flowDefOptimizer;

    private final RegionExecutionPlanFactory regionExecutionPlanFactory;

    private final SupervisorImpl supervisor;

    @Inject
    public JokerEngine ( final RegionDefFormer regionDefFormer, final FlowDefOptimizer flowDefOptimizer,
                         final RegionExecutionPlanFactory regionExecutionPlanFactory,
                         final SupervisorImpl supervisor )
    {
        this.regionDefFormer = regionDefFormer;
        this.flowDefOptimizer = flowDefOptimizer;
        this.regionExecutionPlanFactory = regionExecutionPlanFactory;
        this.supervisor = supervisor;
    }

    public FlowExecutionPlan run ( final FlowDef flow ) throws InitializationException
    {
        final Pair<FlowDef, List<RegionDef>> result = flowDefOptimizer.optimize( flow, regionDefFormer.createRegions( flow ) );
        final List<RegionExecutionPlan> regionExecutionPlans = regionExecutionPlanFactory.createRegionExecutionPlans( result._2 );
        return supervisor.start( result._1, regionExecutionPlans );
    }

    public FlowStatus getStatus ()
    {
        return supervisor.getFlowStatus();
    }

    public Future<Void> shutdown ()
    {
        return supervisor.shutdown();
    }

    public Future<FlowExecutionPlan> mergePipelines ( final int flowVersion, final List<PipelineId> pipelineIds )
    {
        return supervisor.mergePipelines( flowVersion, pipelineIds );
    }

    public Future<FlowExecutionPlan> splitPipeline ( final int flowVersion,
                                                     final PipelineId pipelineId,
                                                     final List<Integer> pipelineOperatorIndices )
    {
        return supervisor.splitPipeline( flowVersion, pipelineId, pipelineOperatorIndices );
    }

    public Future<FlowExecutionPlan> rebalanceRegion ( final int flowVersion, final int regionId, final int newReplicaCount )
    {
        return supervisor.rebalanceRegion( flowVersion, regionId, newReplicaCount );
    }

}
