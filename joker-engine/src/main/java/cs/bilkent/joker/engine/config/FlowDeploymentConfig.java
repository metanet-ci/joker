package cs.bilkent.joker.engine.config;

import com.typesafe.config.Config;

public class FlowDeploymentConfig
{

    static final String CONFIG_NAME = "flowDeployment";

    static final String DUPLICATE_STATELESS_REGIONS = "duplicateStatelessRegions";

    static final String MERGE_STATELESS_REGIONS_WITH_STATEFUL_REGIONS = "mergeStatelessRegionsWithStatefulRegions";

    static final String PAIR_STATELESS_REGIONS_WITH_PARTITIONED_STATEFUL_REGIONS = "pairStatelessRegionsWithPartitionedStatefulRegions";

    static final String MAX_REPLICA_COUNT = "maxReplicaCount";


    private final boolean duplicateStatelessRegionsEnabled;

    private final boolean mergeStatelessRegionsWithStatefulRegionsEnabled;

    private final boolean pairStatelessRegionsWithPartitionedStatefulRegionsEnabled;

    private final int maxReplicaCount;

    FlowDeploymentConfig ( final Config parentConfig )
    {
        final Config config = parentConfig.getConfig( CONFIG_NAME );
        this.duplicateStatelessRegionsEnabled = config.getBoolean( DUPLICATE_STATELESS_REGIONS );
        this.mergeStatelessRegionsWithStatefulRegionsEnabled = config.getBoolean( MERGE_STATELESS_REGIONS_WITH_STATEFUL_REGIONS );
        this.pairStatelessRegionsWithPartitionedStatefulRegionsEnabled = config.getBoolean(
                PAIR_STATELESS_REGIONS_WITH_PARTITIONED_STATEFUL_REGIONS );
        this.maxReplicaCount = config.getInt( MAX_REPLICA_COUNT );
    }

    public boolean isDuplicateStatelessRegionsEnabled ()
    {
        return duplicateStatelessRegionsEnabled;
    }

    public boolean isMergeStatelessRegionsWithStatefulRegionsEnabled ()
    {
        return mergeStatelessRegionsWithStatefulRegionsEnabled;
    }

    public boolean isPairStatelessRegionsWithPartitionedStatefulRegionsEnabled ()
    {
        return pairStatelessRegionsWithPartitionedStatefulRegionsEnabled;
    }

    public int getMaxReplicaCount ()
    {
        return maxReplicaCount;
    }

}
