package cs.bilkent.joker.engine.config;

import java.util.concurrent.TimeUnit;

import com.typesafe.config.Config;

public class TupleQueueDrainerConfig
{

    public static final String CONFIG_NAME = "tupleQueueDrainer";

    public static final String MAX_BATCH_SIZE = "maxBatchSize";

    public static final String PARTITIONED_STATEFUL_PIPELINE_DRAINER_MAX_BATCH_SIZE = "partitionedStatefulPipelineDrainerMaxBatchSize";

    public static final String DRAIN_TIMEOUT = "drainTimeout";

    public static final String DRAIN_TIMEOUT_UNIT = "drainTimeoutUnit";


    private final int maxBatchSize;

    private final int partitionedStatefulPipelineDrainerMaxBatchSize;

    private final long drainTimeout;

    private final TimeUnit drainTimeoutUnit;

    TupleQueueDrainerConfig ( final Config parentConfig )
    {
        final Config config = parentConfig.getConfig( CONFIG_NAME );
        this.maxBatchSize = config.getInt( MAX_BATCH_SIZE );
        this.partitionedStatefulPipelineDrainerMaxBatchSize = config.getInt( PARTITIONED_STATEFUL_PIPELINE_DRAINER_MAX_BATCH_SIZE );
        this.drainTimeout = config.getLong( DRAIN_TIMEOUT );
        this.drainTimeoutUnit = TimeUnit.valueOf( config.getString( DRAIN_TIMEOUT_UNIT ) );
    }

    public int getMaxBatchSize ()
    {
        return maxBatchSize;
    }

    public int getPartitionedStatefulPipelineDrainerMaxBatchSize ()
    {
        return partitionedStatefulPipelineDrainerMaxBatchSize;
    }

    public long getDrainTimeout ()
    {
        return drainTimeout;
    }

    public TimeUnit getDrainTimeoutTimeUnit ()
    {
        return drainTimeoutUnit;
    }

}
