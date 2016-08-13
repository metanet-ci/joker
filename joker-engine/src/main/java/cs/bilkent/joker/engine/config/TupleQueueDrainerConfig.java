package cs.bilkent.joker.engine.config;

import com.typesafe.config.Config;

public class TupleQueueDrainerConfig
{
    static final String CONFIG_NAME = "tupleQueueDrainer";

    static final String MAX_BATCH_SIZE = "maxBatchSize";

    static final String DRAIN_TIMEOUT_IN_MILLIS = "drainTimeoutInMillis";


    private final int maxBatchSize;

    private final long drainTimeoutInMillis;

    TupleQueueDrainerConfig ( final Config parentConfig )
    {
        final Config config = parentConfig.getConfig( CONFIG_NAME );
        this.maxBatchSize = config.getInt( MAX_BATCH_SIZE );
        this.drainTimeoutInMillis = config.getLong( DRAIN_TIMEOUT_IN_MILLIS );
    }

    public int getMaxBatchSize ()
    {
        return maxBatchSize;
    }

    public long getDrainTimeoutInMillis ()
    {
        return drainTimeoutInMillis;
    }

}
