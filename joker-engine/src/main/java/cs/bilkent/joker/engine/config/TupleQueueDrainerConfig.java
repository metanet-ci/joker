package cs.bilkent.joker.engine.config;

import java.util.concurrent.TimeUnit;

import com.typesafe.config.Config;

public class TupleQueueDrainerConfig
{
    static final String CONFIG_NAME = "tupleQueueDrainer";

    static final String MAX_BATCH_SIZE = "maxBatchSize";

    static final String DRAIN_TIMEOUT = "drainTimeout";

    static final String DRAIN_TIMEOUT_UNIT = "drainTimeoutUnit";


    private final int maxBatchSize;

    private final long drainTimeout;

    private final TimeUnit drainTimeoutUnit;

    TupleQueueDrainerConfig ( final Config parentConfig )
    {
        final Config config = parentConfig.getConfig( CONFIG_NAME );
        this.maxBatchSize = config.getInt( MAX_BATCH_SIZE );
        this.drainTimeout = config.getLong( DRAIN_TIMEOUT );
        this.drainTimeoutUnit = TimeUnit.valueOf( config.getString( DRAIN_TIMEOUT_UNIT ) );
    }

    public int getMaxBatchSize ()
    {
        return maxBatchSize;
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
