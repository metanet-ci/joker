package cs.bilkent.zanza.engine.config;

import com.typesafe.config.Config;

public class TupleQueueDrainerConfig
{
    public static final String CONFIG_NAME = "tupleQueueDrainer";

    public static final String MAX_BATCH_SIZE = "maxBatchSize";

    public static final String DRAIN_TIMEOUT_IN_MILLIS = "drainTimeoutInMillis";

    private final int maxBatchSize;

    private final long drainTimeoutInMillis;

    public TupleQueueDrainerConfig ( final Config parentConfig )
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
