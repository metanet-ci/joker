package cs.bilkent.joker.engine.flow;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import cs.bilkent.joker.engine.pipeline.PipelineId;
import cs.bilkent.joker.engine.region.RegionConfig;
import cs.bilkent.joker.flow.FlowDef;
import cs.bilkent.joker.operator.OperatorDef;
import static java.util.Collections.unmodifiableList;

public class FlowDeploymentDef
{

    private final int version;

    private final FlowDef flow;

    private final List<RegionConfig> regionConfigs;

    public FlowDeploymentDef ( final int version, final FlowDef flow, final Collection<RegionConfig> regionConfigs )
    {
        this.version = version;
        this.flow = flow;
        this.regionConfigs = unmodifiableList( new ArrayList<>( regionConfigs ) );
    }

    public int getVersion ()
    {
        return version;
    }

    public FlowDef getFlow ()
    {
        return flow;
    }

    public List<RegionConfig> getRegionConfigs ()
    {
        return regionConfigs;
    }

    public RegionConfig getRegionConfig ( final int regionId )
    {
        for ( RegionConfig regionConfig : regionConfigs )
        {
            if ( regionConfig.getRegionId() == regionId )
            {
                return regionConfig;
            }
        }

        return null;
    }

    public RegionConfig getOperatorRegion ( final String operatorId )
    {
        for ( RegionConfig regionConfig : regionConfigs )
        {
            for ( OperatorDef operatorDef : regionConfig.getRegionDef().getOperators() )
            {
                if ( operatorDef.id().equals( operatorId ) )
                {
                    return regionConfig;
                }
            }
        }

        return null;
    }

    public PipelineId getOperatorPipeline ( final String operatorId )
    {
        for ( RegionConfig regionConfig : regionConfigs )
        {
            for ( PipelineId pipelineId : regionConfig.getPipelineIds() )
            {
                for ( OperatorDef operatorDef : regionConfig.getOperatorDefsByPipelineIndex( pipelineId.getPipelineStartIndex() ) )
                {
                    if ( operatorDef.id().equals( operatorId ) )
                    {
                        return pipelineId;
                    }
                }
            }
        }

        return null;
    }

}
