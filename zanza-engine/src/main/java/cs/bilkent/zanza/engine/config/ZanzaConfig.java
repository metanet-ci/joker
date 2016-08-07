package cs.bilkent.zanza.engine.config;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class ZanzaConfig
{

    public static final String ENGINE_CONFIG_NAME = "zanza.engine";

    private final Config config;

    private final TupleQueueManagerConfig tupleQueueManagerConfig;

    private final TupleQueueDrainerConfig tupleQueueDrainerConfig;

    private final PipelineReplicaRunnerConfig pipelineReplicaRunnerConfig;

    private final PartitionServiceConfig partitionServiceConfig;

    private final FlowDeploymentConfig flowDeploymentConfig;

    public ZanzaConfig ()
    {
        this( ConfigFactory.load() );
    }

    public ZanzaConfig ( final Config config )
    {
        this.config = config;
        final Config engineConfig = config.getConfig( ENGINE_CONFIG_NAME );
        this.tupleQueueManagerConfig = new TupleQueueManagerConfig( engineConfig );
        this.tupleQueueDrainerConfig = new TupleQueueDrainerConfig( engineConfig );
        this.pipelineReplicaRunnerConfig = new PipelineReplicaRunnerConfig( engineConfig );
        this.partitionServiceConfig = new PartitionServiceConfig( engineConfig );
        this.flowDeploymentConfig = new FlowDeploymentConfig( engineConfig );
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

    public FlowDeploymentConfig getFlowDeploymentConfig ()
    {
        return flowDeploymentConfig;
    }

}
