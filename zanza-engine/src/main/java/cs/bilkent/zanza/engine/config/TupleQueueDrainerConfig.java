package cs.bilkent.zanza.engine.config;

import com.typesafe.config.Config;

public class TupleQueueDrainerConfig
{
    public static final String CONFIG_NAME = "tupleQueueDrainer";

    public static final String DRAIN_TIMEOUT_IN_MILLIS = "drainTimeoutInMillis";


    private final long drainTimeoutInMillis;

    public TupleQueueDrainerConfig ( final Config parentConfig )
    {
        final Config config = parentConfig.getConfig( CONFIG_NAME );
        this.drainTimeoutInMillis = config.getLong( DRAIN_TIMEOUT_IN_MILLIS );
    }

    public long getDrainTimeoutInMillis ()
    {
        return drainTimeoutInMillis;
    }

}
