package cs.bilkent.joker.engine.config;

import com.typesafe.config.Config;

public class FlowDefOptimizerConfig
{

    public static final String CONFIG_NAME = "flowDefOptimizer";

    public static final String DUPLICATE_STATELESS_REGIONS = "duplicateStatelessRegions";

    public static final String MERGE_REGIONS = "mergeRegions";


    private final boolean duplicateStatelessRegionsEnabled;

    private final boolean mergeRegionsEnabled;

    FlowDefOptimizerConfig ( final Config parentConfig )
    {
        final Config config = parentConfig.getConfig( CONFIG_NAME );
        this.duplicateStatelessRegionsEnabled = config.getBoolean( DUPLICATE_STATELESS_REGIONS );
        this.mergeRegionsEnabled = config.getBoolean( MERGE_REGIONS );
    }

    public boolean isDuplicateStatelessRegionsEnabled ()
    {
        return duplicateStatelessRegionsEnabled;
    }

    public boolean isMergeRegionsEnabled ()
    {
        return mergeRegionsEnabled;
    }

    @Override
    public String toString ()
    {
        return "FlowDefOptimizerConfig{" + "duplicateStatelessRegionsEnabled=" + duplicateStatelessRegionsEnabled + ", mergeRegionsEnabled="
               + mergeRegionsEnabled + '}';
    }

}
