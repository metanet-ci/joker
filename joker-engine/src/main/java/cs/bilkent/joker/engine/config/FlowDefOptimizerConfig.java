package cs.bilkent.joker.engine.config;

import com.typesafe.config.Config;

public class FlowDefOptimizerConfig
{

    public static final String CONFIG_NAME = "flowDefOptimizer";

    public static final String DUPLICATE_STATELESS_REGIONS = "duplicateStatelessRegions";

    public static final String MERGE_STATELESS_REGIONS_WITH_STATEFUL_REGIONS = "mergeStatelessRegionsWithStatefulRegions";

    public static final String MAX_REPLICA_COUNT = "maxReplicaCount";


    private final boolean duplicateStatelessRegionsEnabled;

    private final boolean mergeStatelessRegionsWithStatefulRegionsEnabled;

    private final int maxReplicaCount;

    FlowDefOptimizerConfig ( final Config parentConfig )
    {
        final Config config = parentConfig.getConfig( CONFIG_NAME );
        this.duplicateStatelessRegionsEnabled = config.getBoolean( DUPLICATE_STATELESS_REGIONS );
        this.mergeStatelessRegionsWithStatefulRegionsEnabled = config.getBoolean( MERGE_STATELESS_REGIONS_WITH_STATEFUL_REGIONS );
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

    public int getMaxReplicaCount ()
    {
        return maxReplicaCount;
    }

}
