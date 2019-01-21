package cs.bilkent.joker.engine;


import java.util.List;
import java.util.concurrent.CompletableFuture;
import javax.inject.Inject;

import cs.bilkent.joker.engine.exception.InitializationException;
import cs.bilkent.joker.engine.flow.FlowExecPlan;
import cs.bilkent.joker.engine.flow.PipelineId;
import cs.bilkent.joker.engine.flow.RegionDef;
import cs.bilkent.joker.engine.flow.RegionExecPlan;
import cs.bilkent.joker.engine.region.FlowDefOptimizer;
import cs.bilkent.joker.engine.region.RegionDefFormer;
import cs.bilkent.joker.engine.region.RegionExecPlanFactory;
import cs.bilkent.joker.engine.supervisor.impl.SupervisorImpl;
import cs.bilkent.joker.flow.FlowDef;
import cs.bilkent.joker.operator.utils.Pair;

public class JokerEngine
{

    private final RegionDefFormer regionDefFormer;

    private final FlowDefOptimizer flowDefOptimizer;

    private final RegionExecPlanFactory regionExecPlanFactory;

    private final SupervisorImpl supervisor;

    @Inject
    public JokerEngine ( final RegionDefFormer regionDefFormer,
                         final FlowDefOptimizer flowDefOptimizer,
                         final RegionExecPlanFactory regionExecPlanFactory,
                         final SupervisorImpl supervisor )
    {
        this.regionDefFormer = regionDefFormer;
        this.flowDefOptimizer = flowDefOptimizer;
        this.regionExecPlanFactory = regionExecPlanFactory;
        this.supervisor = supervisor;
    }

    public FlowExecPlan run ( final FlowDef flow ) throws InitializationException
    {
        final List<RegionDef> initialRegions = regionDefFormer.createRegions( flow );
        final Pair<FlowDef, List<RegionDef>> result = flowDefOptimizer.optimize( flow, initialRegions );
        final FlowDef optimizedFlow = result._1;
        final List<RegionDef> optimizedRegions = result._2;
        final List<RegionExecPlan> regionExecPlans = regionExecPlanFactory.createRegionExecPlans( optimizedRegions );
        return supervisor.start( optimizedFlow, regionExecPlans );
    }

    public FlowStatus getStatus ()
    {
        return supervisor.getFlowStatus();
    }

    public CompletableFuture<Void> shutdown ()
    {
        return supervisor.shutdown();
    }

    public CompletableFuture<Void> disableAdaptation ()
    {
        return supervisor.disableAdaptation();
    }

    public CompletableFuture<FlowExecPlan> mergePipelines ( final int flowVersion, final List<PipelineId> pipelineIds )
    {
        return supervisor.mergePipelines( flowVersion, pipelineIds );
    }

    public CompletableFuture<FlowExecPlan> splitPipeline ( final int flowVersion,
                                                           final PipelineId pipelineId,
                                                           final List<Integer> pipelineOperatorIndices )
    {
        return supervisor.splitPipeline( flowVersion, pipelineId, pipelineOperatorIndices );
    }

    public CompletableFuture<FlowExecPlan> rebalanceRegion ( final int flowVersion, final int regionId, final int newReplicaCount )
    {
        return supervisor.rebalanceRegion( flowVersion, regionId, newReplicaCount );
    }

}
