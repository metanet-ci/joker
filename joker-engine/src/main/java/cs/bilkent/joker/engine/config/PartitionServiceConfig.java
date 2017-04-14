package cs.bilkent.joker.engine.config;

import com.typesafe.config.Config;

public class PartitionServiceConfig
{

    public static final String CONFIG_NAME = "partitionService";

    public static final String PARTITION_COUNT = "partitionCount";

    public static final String MAX_REPLICA_COUNT = "maxReplicaCount";


    private final int partitionCount;

    private final int maxReplicaCount;

    PartitionServiceConfig ( final Config parentConfig )
    {
        final Config config = parentConfig.getConfig( CONFIG_NAME );
        this.partitionCount = config.getInt( PARTITION_COUNT );
        this.maxReplicaCount = config.getInt( MAX_REPLICA_COUNT );
    }

    public int getPartitionCount ()
    {
        return partitionCount;
    }

    public int getMaxReplicaCount ()
    {
        return maxReplicaCount;
    }

    @Override
    public String toString ()
    {
        return "PartitionServiceConfig{" + "partitionCount=" + partitionCount + ", maxReplicaCount=" + maxReplicaCount + '}';
    }
    
}
