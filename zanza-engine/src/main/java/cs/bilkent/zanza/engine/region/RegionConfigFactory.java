package cs.bilkent.zanza.engine.region;

import java.util.List;

public interface RegionConfigFactory
{

    default void init ()
    {

    }

    default void destroy ()
    {

    }

    List<RegionConfig> createRegionConfigs ( final FlowDeploymentDef flowDeployment );

}
