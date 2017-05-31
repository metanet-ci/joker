package cs.bilkent.joker.engine.adaptation.impl.bottleneckresolver;

import java.util.List;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cs.bilkent.joker.engine.adaptation.AdaptationAction;
import cs.bilkent.joker.engine.adaptation.BottleneckResolver;
import cs.bilkent.joker.engine.adaptation.impl.adaptationaction.RegionRebalanceAction;
import cs.bilkent.joker.engine.flow.PipelineId;
import cs.bilkent.joker.engine.flow.RegionExecutionPlan;
import cs.bilkent.joker.engine.metric.PipelineMetrics;
import static cs.bilkent.joker.impl.com.google.common.base.Preconditions.checkArgument;
import static cs.bilkent.joker.operator.spec.OperatorType.PARTITIONED_STATEFUL;
import cs.bilkent.joker.utils.Pair;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

public class RegionExtender implements BottleneckResolver
{

    private static final Logger LOGGER = LoggerFactory.getLogger( RegionExtender.class );


    private final int maxReplicaCount;

    public RegionExtender ( final int maxReplicaCount )
    {
        this.maxReplicaCount = maxReplicaCount;
    }

    RegionRebalanceAction resolve ( final RegionExecutionPlan regionExecutionPlan, final PipelineMetrics bottleneckPipelineMetrics )
    {
        checkArgument( regionExecutionPlan != null );
        checkArgument( bottleneckPipelineMetrics != null );

        if ( regionExecutionPlan.getRegionType() != PARTITIONED_STATEFUL )
        {
            return null;
        }

        if ( regionExecutionPlan.getReplicaCount() >= maxReplicaCount )
        {
            LOGGER.warn( "Cannot create a new replica since region: {} has {} replicas",
                         regionExecutionPlan.getRegionId(),
                         regionExecutionPlan.getReplicaCount() );

            return null;
        }

        final int replicaCount = regionExecutionPlan.getReplicaCount();
        return new RegionRebalanceAction( regionExecutionPlan, replicaCount + 1 );
    }

    @Override
    public List<Pair<AdaptationAction, List<PipelineId>>> resolve ( final RegionExecutionPlan regionExecutionPlan,
                                                                    final List<PipelineMetrics> bottleneckPipelinesMetrics )
    {
        final AdaptationAction adaptationAction = resolve( regionExecutionPlan, bottleneckPipelinesMetrics.get( 0 ) );
        if ( adaptationAction != null )
        {
            final List<PipelineId> pipelineIds = bottleneckPipelinesMetrics.stream()
                                                                           .map( PipelineMetrics::getPipelineId )
                                                                           .collect( Collectors.toList() );

            LOGGER.info( "Region: {} bottleneck pipelines: {} can be resolved with {}",
                         regionExecutionPlan.getRegionId(),
                         pipelineIds,
                         adaptationAction );

            return singletonList( Pair.of( adaptationAction, pipelineIds ) );
        }

        return emptyList();
    }

}
