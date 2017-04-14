package cs.bilkent.joker.engine.config;

import com.typesafe.config.Config;

public class TupleQueueDrainerConfig
{

    public static final String CONFIG_NAME = "tupleQueueDrainer";

    public static final String MAX_BATCH_SIZE = "maxBatchSize";

    public static final String PARTITIONED_STATEFUL_PIPELINE_DRAINER_MAX_BATCH_SIZE = "partitionedStatefulPipelineDrainerMaxBatchSize";


    private final int maxBatchSize;

    private final int partitionedStatefulPipelineDrainerMaxBatchSize;

    TupleQueueDrainerConfig ( final Config parentConfig )
    {
        final Config config = parentConfig.getConfig( CONFIG_NAME );
        this.maxBatchSize = config.getInt( MAX_BATCH_SIZE );
        this.partitionedStatefulPipelineDrainerMaxBatchSize = config.getInt( PARTITIONED_STATEFUL_PIPELINE_DRAINER_MAX_BATCH_SIZE );
    }

    public int getMaxBatchSize ()
    {
        return maxBatchSize;
    }

    public int getPartitionedStatefulPipelineDrainerMaxBatchSize ()
    {
        return partitionedStatefulPipelineDrainerMaxBatchSize;
    }

    @Override
    public String toString ()
    {
        return "TupleQueueDrainerConfig{" + "maxBatchSize=" + maxBatchSize + ", partitionedStatefulPipelineDrainerMaxBatchSize="
               + partitionedStatefulPipelineDrainerMaxBatchSize + '}';
    }

}
