package cs.bilkent.joker.engine.supervisor.impl;

import java.util.List;

import cs.bilkent.joker.engine.adaptation.AdaptationPerformer;
import cs.bilkent.joker.engine.flow.FlowExecutionPlan;
import cs.bilkent.joker.engine.flow.PipelineId;
import cs.bilkent.joker.engine.flow.RegionExecutionPlan;
import cs.bilkent.joker.engine.pipeline.PipelineManager;
import cs.bilkent.joker.utils.Pair;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;

public class DefaultAdaptationPerformer implements AdaptationPerformer
{

    private final PipelineManager pipelineManager;

    private final int flowVersion;

    public DefaultAdaptationPerformer ( final PipelineManager pipelineManager, final int flowVersion )
    {
        this.pipelineManager = pipelineManager;
        this.flowVersion = flowVersion;
    }

    @Override
    public Pair<List<PipelineId>, List<PipelineId>> mergePipelines ( final List<PipelineId> pipelineIds )
    {
        pipelineManager.mergePipelines( flowVersion, pipelineIds );

        return Pair.of( pipelineIds, singletonList( pipelineIds.get( 0 ) ) );
    }

    @Override
    public Pair<List<PipelineId>, List<PipelineId>> splitPipeline ( final PipelineId pipelineId,
                                                                    final List<Integer> pipelineOperatorIndices )
    {
        final List<PipelineId> existingPipelineIds = pipelineManager.getFlowExecutionPlan()
                                                                    .getRegionExecutionPlan( pipelineId.getRegionId() )
                                                                    .getPipelineIds();

        pipelineManager.splitPipeline( flowVersion, pipelineId, pipelineOperatorIndices );

        final List<PipelineId> newPipelineIds = pipelineManager.getFlowExecutionPlan()
                                                               .getRegionExecutionPlan( pipelineId.getRegionId() )
                                                               .getPipelineIds()
                                                               .stream()
                                                               .filter( p -> !existingPipelineIds.contains( p ) )
                                                               .collect( toList() );

        return Pair.of( singletonList( pipelineId ), newPipelineIds );
    }

    @Override
    public Pair<List<PipelineId>, List<PipelineId>> rebalanceRegion ( final int regionId, final int newReplicaCount )
    {
        pipelineManager.rebalanceRegion( flowVersion, regionId, newReplicaCount );

        final FlowExecutionPlan flowExecutionPlan = pipelineManager.getFlowExecutionPlan();
        final RegionExecutionPlan regionExecutionPlan = flowExecutionPlan.getRegionExecutionPlan( regionId );
        final List<PipelineId> pipelineIds = regionExecutionPlan.getPipelineIds();

        return Pair.of( pipelineIds, pipelineIds );
    }

}
