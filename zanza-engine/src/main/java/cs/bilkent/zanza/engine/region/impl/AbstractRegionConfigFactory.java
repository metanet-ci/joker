package cs.bilkent.zanza.engine.region.impl;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import cs.bilkent.zanza.engine.config.ZanzaConfig;
import cs.bilkent.zanza.engine.region.FlowDeploymentDef;
import cs.bilkent.zanza.engine.region.FlowDeploymentDef.RegionGroup;
import cs.bilkent.zanza.engine.region.RegionConfig;
import cs.bilkent.zanza.engine.region.RegionConfigFactory;
import cs.bilkent.zanza.engine.region.RegionDef;
import cs.bilkent.zanza.flow.OperatorDef;
import static cs.bilkent.zanza.operator.spec.OperatorType.STATEFUL;

public abstract class AbstractRegionConfigFactory implements RegionConfigFactory
{

    protected final int maxReplicaCount;

    protected AbstractRegionConfigFactory ( ZanzaConfig zanzaConfig )
    {
        this.maxReplicaCount = zanzaConfig.getFlowDeploymentConfig().getMaxReplicaCount();
    }

    @Override
    public List<RegionConfig> createRegionConfigs ( final FlowDeploymentDef flowDeployment )
    {
        try
        {
            init();
            final List<RegionConfig> regionConfigs = new ArrayList<>();
            for ( RegionGroup regionGroup : flowDeployment.getRegionGroups() )
            {
                final List<RegionConfig> r = createRegionConfigs( regionGroup );
                final List<RegionDef> regions = regionGroup.getRegions();

                final int replicaCount = r.get( 0 ).getReplicaCount();
                validateReplicaCount( regions.get( 0 ), replicaCount );

                for ( int i = 0; i < regions.size(); i++ )
                {
                    final RegionDef region = regions.get( i );
                    final RegionConfig regionConfig = r.get( i );
                    checkArgument( region.equals( regionConfig.getRegionDef() ) );
                    validatePipelineStartIndices( region.getOperators(), regionConfig.getPipelineStartIndices() );
                    checkArgument( regionConfig.getReplicaCount() == replicaCount );
                }

                regionConfigs.addAll( r );
            }

            failIfMissingRegionConfigExists( flowDeployment.getRegions(), regionConfigs );

            return regionConfigs;
        }
        finally
        {
            try
            {
                destroy();
            }
            catch ( Exception e )
            {
                if ( e instanceof InterruptedException )
                {
                    Thread.currentThread().interrupt();
                }
            }
        }

    }

    protected abstract List<RegionConfig> createRegionConfigs ( RegionGroup regionGroup );

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

    protected final void validatePipelineStartIndices ( final List<OperatorDef> operators, final List<Integer> pipelineStartIndices )
    {
        if ( pipelineStartIndices.get( 0 ) != 0 )
        {
            pipelineStartIndices.add( 0, 0 );
        }
        checkArgument( pipelineStartIndices.size() <= operators.size() );
        int i = -1;
        for ( int startIndex : pipelineStartIndices )
        {
            checkArgument( startIndex > i, "invalid pipeline start indices: ", pipelineStartIndices );
            i = startIndex;
        }
        checkArgument( i < operators.size() );
    }

    protected final void validateReplicaCount ( final RegionDef region, final int replicaCount )
    {
        if ( region.getRegionType() != STATEFUL )
        {
            checkArgument( replicaCount > 0 && replicaCount <= maxReplicaCount, "replica count must be between 0 and %s", maxReplicaCount );
        }
        else
        {
            checkArgument( replicaCount == 1, "Replica count cannot be %s for %s region", replicaCount, region.getRegionType() );
        }
    }

}
