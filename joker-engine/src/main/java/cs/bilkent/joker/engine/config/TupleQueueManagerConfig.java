package cs.bilkent.joker.engine.config;

import com.typesafe.config.Config;

public class TupleQueueManagerConfig
{

    public static final String CONFIG_NAME = "tupleQueueManager";

    public static final String TUPLE_QUEUE_CAPACITY = "tupleQueueCapacity";

    public static final String MAX_DRAINABLE_KEY_COUNT = "maxDrainableKeyCount";


    private final int tupleQueueCapacity;

    private final int maxDrainableKeyCount;

    TupleQueueManagerConfig ( final Config parentConfig )
    {
        final Config config = parentConfig.getConfig( CONFIG_NAME );
        this.tupleQueueCapacity = config.getInt( TUPLE_QUEUE_CAPACITY );
        this.maxDrainableKeyCount = config.getInt( MAX_DRAINABLE_KEY_COUNT );
    }

    public int getTupleQueueCapacity ()
    {
        return tupleQueueCapacity;
    }

    public int getMaxDrainableKeyCount ()
    {
        return maxDrainableKeyCount;
    }

}
