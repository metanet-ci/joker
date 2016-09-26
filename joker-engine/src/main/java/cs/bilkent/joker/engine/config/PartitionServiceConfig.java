package cs.bilkent.joker.engine.config;

import com.typesafe.config.Config;

public class PartitionServiceConfig
{

    public static final String CONFIG_NAME = "partitionService";

    public static final String PARTITION_COUNT = "partitionCount";


    private final int partitionCount;

    PartitionServiceConfig ( final Config parentConfig )
    {
        final Config config = parentConfig.getConfig( CONFIG_NAME );
        this.partitionCount = config.getInt( PARTITION_COUNT );
    }

    public int getPartitionCount ()
    {
        return partitionCount;
    }

}
