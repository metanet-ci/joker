package cs.bilkent.joker.engine.config;

import com.typesafe.config.Config;

public class TupleQueueDrainerConfig
{

    static final String CONFIG_NAME = "tupleQueueDrainer";

    static final String MAX_BATCH_SIZE = "maxBatchSize";


    private final int maxBatchSize;


    TupleQueueDrainerConfig ( final Config parentConfig )
    {
        final Config config = parentConfig.getConfig( CONFIG_NAME );
        this.maxBatchSize = config.getInt( MAX_BATCH_SIZE );
    }

    public int getMaxBatchSize ()
    {
        return maxBatchSize;
    }

    @Override
    public String toString ()
    {
        return "TupleQueueDrainerConfig{" + "maxBatchSize=" + maxBatchSize + '}';
    }

}
