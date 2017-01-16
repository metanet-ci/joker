package cs.bilkent.joker.engine.config;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class JokerConfig
{

    public static final String ENGINE_CONFIG_NAME = "joker.engine";

    public static final String JOKER_THREAD_GROUP_NAME = "jokerThreadGroup";

    public static final String JOKER_ID = "jokerId";


    private final Config config;

    private final TupleQueueManagerConfig tupleQueueManagerConfig;

    private final TupleQueueDrainerConfig tupleQueueDrainerConfig;

    private final PipelineReplicaRunnerConfig pipelineReplicaRunnerConfig;

    private final PartitionServiceConfig partitionServiceConfig;

    private final FlowDefOptimizerConfig flowDefOptimizerConfig;

    private final PipelineManagerConfig pipelineManagerConfig;

    private final RegionManagerConfig regionManagerConfig;

    private final MetricManagerConfig metricManagerConfig;

    public JokerConfig ()
    {
        this( ConfigFactory.load() );
    }

    public JokerConfig ( final Config config )
    {
        this.config = config;
        final Config engineConfig = config.getConfig( ENGINE_CONFIG_NAME );
        this.tupleQueueManagerConfig = new TupleQueueManagerConfig( engineConfig );
        this.tupleQueueDrainerConfig = new TupleQueueDrainerConfig( engineConfig );
        this.pipelineReplicaRunnerConfig = new PipelineReplicaRunnerConfig( engineConfig );
        this.partitionServiceConfig = new PartitionServiceConfig( engineConfig );
        this.flowDefOptimizerConfig = new FlowDefOptimizerConfig( engineConfig );
        this.pipelineManagerConfig = new PipelineManagerConfig( engineConfig );
        this.regionManagerConfig = new RegionManagerConfig( engineConfig );
        this.metricManagerConfig = new MetricManagerConfig( engineConfig );
    }

    public Config getRootConfig ()
    {
        return config;
    }

    public TupleQueueManagerConfig getTupleQueueManagerConfig ()
    {
        return tupleQueueManagerConfig;
    }

    public TupleQueueDrainerConfig getTupleQueueDrainerConfig ()
    {
        return tupleQueueDrainerConfig;
    }

    public PipelineReplicaRunnerConfig getPipelineReplicaRunnerConfig ()
    {
        return pipelineReplicaRunnerConfig;
    }

    public PartitionServiceConfig getPartitionServiceConfig ()
    {
        return partitionServiceConfig;
    }

    public FlowDefOptimizerConfig getFlowDefOptimizerConfig ()
    {
        return flowDefOptimizerConfig;
    }

    public PipelineManagerConfig getPipelineManagerConfig ()
    {
        return pipelineManagerConfig;
    }

    public RegionManagerConfig getRegionManagerConfig ()
    {
        return regionManagerConfig;
    }

    public MetricManagerConfig getMetricManagerConfig ()
    {
        return metricManagerConfig;
    }

}
