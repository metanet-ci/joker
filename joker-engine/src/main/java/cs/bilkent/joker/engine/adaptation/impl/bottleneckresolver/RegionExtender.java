package cs.bilkent.joker.engine.adaptation.impl.bottleneckresolver;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cs.bilkent.joker.engine.adaptation.AdaptationAction;
import cs.bilkent.joker.engine.adaptation.BottleneckResolver;
import cs.bilkent.joker.engine.adaptation.impl.adaptationaction.RegionRebalanceAction;
import cs.bilkent.joker.engine.flow.RegionExecutionPlan;
import cs.bilkent.joker.engine.metric.PipelineMetricsSnapshot;
import static cs.bilkent.joker.operator.spec.OperatorType.PARTITIONED_STATEFUL;

public class RegionExtender implements BottleneckResolver
{

    private static final Logger LOGGER = LoggerFactory.getLogger( RegionExtender.class );


    private final int maxReplicaCount;

    public RegionExtender ( final int maxReplicaCount )
    {
        this.maxReplicaCount = maxReplicaCount;
    }

    @Override
    public AdaptationAction resolve ( final RegionExecutionPlan regionExecutionPlan,
                                      final PipelineMetricsSnapshot bottleneckPipelineMetrics )
    {
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

}
