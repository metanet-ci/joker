package cs.bilkent.joker.engine.supervisor.impl;

import java.util.List;

import cs.bilkent.joker.engine.adaptation.AdaptationPerformer;
import cs.bilkent.joker.engine.flow.FlowExecPlan;
import cs.bilkent.joker.engine.flow.PipelineId;
import cs.bilkent.joker.engine.flow.RegionExecPlan;
import cs.bilkent.joker.engine.pipeline.PipelineManager;
import static cs.bilkent.joker.impl.com.google.common.base.Preconditions.checkState;
import cs.bilkent.joker.utils.Pair;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;

public class DefaultAdaptationPerformer implements AdaptationPerformer
{

    private final PipelineManager pipelineManager;

    private final int flowVersion;

    private Pair<List<PipelineId>, List<PipelineId>> pipelineIdChanges;

    DefaultAdaptationPerformer ( final PipelineManager pipelineManager )
    {
        this.pipelineManager = pipelineManager;
        this.flowVersion = pipelineManager.getFlowExecPlan().getVersion();
    }

    @Override
    public void mergePipelines ( final List<PipelineId> pipelineIds )
    {
        pipelineManager.mergePipelines( flowVersion, pipelineIds );

        pipelineIdChanges = Pair.of( pipelineIds, singletonList( pipelineIds.get( 0 ) ) );
    }

    @Override
    public void splitPipeline ( final PipelineId pipelineId, final List<Integer> pipelineOperatorIndices )
    {
        final List<PipelineId> existingPipelineIds = pipelineManager.getFlowExecPlan().getRegionExecPlan( pipelineId.getRegionId() )
                                                                    .getPipelineIds();
        existingPipelineIds.remove( pipelineId );

        pipelineManager.splitPipeline( flowVersion, pipelineId, pipelineOperatorIndices );

        final List<PipelineId> newPipelineIds = pipelineManager.getFlowExecPlan().getRegionExecPlan( pipelineId.getRegionId() )
                                                               .getPipelineIds()
                                                               .stream()
                                                               .filter( p -> !existingPipelineIds.contains( p ) )
                                                               .collect( toList() );

        pipelineIdChanges = Pair.of( singletonList( pipelineId ), newPipelineIds );
    }

    @Override
    public void rebalanceRegion ( final int regionId, final int newReplicaCount )
    {
        pipelineManager.rebalanceRegion( flowVersion, regionId, newReplicaCount );

        final FlowExecPlan flowExecPlan = pipelineManager.getFlowExecPlan();
        final RegionExecPlan regionExecPlan = flowExecPlan.getRegionExecPlan( regionId );
        final List<PipelineId> pipelineIds = regionExecPlan.getPipelineIds();

        pipelineIdChanges = Pair.of( pipelineIds, pipelineIds );
    }

    Pair<List<PipelineId>, List<PipelineId>> getPipelineIdChanges ()
    {
        checkState( pipelineIdChanges != null );
        return pipelineIdChanges;
    }

}
