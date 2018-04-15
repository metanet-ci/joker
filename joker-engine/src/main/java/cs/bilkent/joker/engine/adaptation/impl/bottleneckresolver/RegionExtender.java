package cs.bilkent.joker.engine.adaptation.impl.bottleneckresolver;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cs.bilkent.joker.engine.adaptation.AdaptationAction;
import cs.bilkent.joker.engine.adaptation.BottleneckResolver;
import cs.bilkent.joker.engine.adaptation.impl.adaptationaction.RegionRebalanceAction;
import cs.bilkent.joker.engine.flow.PipelineId;
import cs.bilkent.joker.engine.flow.RegionExecPlan;
import cs.bilkent.joker.engine.metric.PipelineMetrics;
import static cs.bilkent.joker.impl.com.google.common.base.Preconditions.checkArgument;
import static cs.bilkent.joker.operator.spec.OperatorType.PARTITIONED_STATEFUL;
import cs.bilkent.joker.operator.utils.Pair;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;

public class RegionExtender implements BottleneckResolver
{

    private static final Logger LOGGER = LoggerFactory.getLogger( RegionExtender.class );


    private final int maxReplicaCount;

    public RegionExtender ( final int maxReplicaCount )
    {
        this.maxReplicaCount = maxReplicaCount;
    }

    RegionRebalanceAction resolve ( final RegionExecPlan execPlan, final PipelineMetrics bottleneckPipelineMetrics )
    {
        checkArgument( execPlan != null );
        checkArgument( bottleneckPipelineMetrics != null );

        if ( execPlan.getRegionType() != PARTITIONED_STATEFUL )
        {
            return null;
        }

        if ( execPlan.getReplicaCount() >= maxReplicaCount )
        {
            LOGGER.warn( "Cannot create a new replica since region: {} has {} replicas",
                         execPlan.getRegionId(),
                         execPlan.getReplicaCount() );

            return null;
        }

        final int replicaCount = execPlan.getReplicaCount();
        return new RegionRebalanceAction( execPlan, replicaCount + 1 );
    }

    @Override
    public List<Pair<AdaptationAction, List<PipelineId>>> resolve ( final RegionExecPlan execPlan, final List<PipelineMetrics> metrics )
    {
        final AdaptationAction action = resolve( execPlan, metrics.get( 0 ) );
        if ( action != null )
        {
            final List<PipelineId> pipelineIds = metrics.stream().map( PipelineMetrics::getPipelineId ).collect( toList() );

            LOGGER.info( "Region: {} bottleneck pipelines: {} can be resolved with {}", execPlan.getRegionId(), pipelineIds, action );

            return singletonList( Pair.of( action, pipelineIds ) );
        }

        return emptyList();
    }

}
