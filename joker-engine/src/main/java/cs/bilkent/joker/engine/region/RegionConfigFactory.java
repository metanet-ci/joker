package cs.bilkent.joker.engine.region;

import java.util.List;

public interface RegionConfigFactory
{

    default void init ()
    {

    }

    default void destroy ()
    {

    }

    List<RegionConfig> createRegionConfigs ( FlowDeploymentDef flowDeployment );

}
