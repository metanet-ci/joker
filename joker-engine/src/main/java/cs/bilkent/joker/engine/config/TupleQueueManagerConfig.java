package cs.bilkent.joker.engine.config;

import com.typesafe.config.Config;

public class TupleQueueManagerConfig
{

    public static final String CONFIG_NAME = "tupleQueueManager";

    public static final String TUPLE_QUEUE_INITIAL_SIZE = "tupleQueueInitialSize";

    public static final String MAX_DRAINABLE_KEY_COUNT = "maxDrainableKeyCount";

    public static final String MAX_SINGLE_THREADED_TUPLE_QUEUE_SIZE = "maxSingleThreadedTupleQueueSize";


    private final int tupleQueueInitialSize;

    private final int maxDrainableKeyCount;

    private final int maxSingleThreadedTupleQueueSize;

    TupleQueueManagerConfig ( final Config parentConfig )
    {
        final Config config = parentConfig.getConfig( CONFIG_NAME );
        this.tupleQueueInitialSize = config.getInt( TUPLE_QUEUE_INITIAL_SIZE );
        this.maxDrainableKeyCount = config.getInt( MAX_DRAINABLE_KEY_COUNT );
        this.maxSingleThreadedTupleQueueSize = config.getInt( MAX_SINGLE_THREADED_TUPLE_QUEUE_SIZE );
    }

    public int getTupleQueueInitialSize ()
    {
        return tupleQueueInitialSize;
    }

    public int getMaxDrainableKeyCount ()
    {
        return maxDrainableKeyCount;
    }

    public int getMaxSingleThreadedTupleQueueSize ()
    {
        return maxSingleThreadedTupleQueueSize;
    }

}
