package cs.bilkent.joker.engine.config;

import com.typesafe.config.Config;

public class TupleQueueManagerConfig
{

    public static final String CONFIG_NAME = "tupleQueueManager";

    public static final String TUPLE_QUEUE_CAPACITY = "tupleQueueCapacity";

    public static final String MAX_DRAINABLE_KEY_COUNT = "maxDrainableKeyCount";

    public static final String PARTITIONED_TUPLE_QUEUE_DRAINER_HINT = "partitionedTupleQueueDrainHint";


    private final int tupleQueueCapacity;

    private final int maxDrainableKeyCount;

    private final int partitionedTupleQueueDrainHint;

    TupleQueueManagerConfig ( final Config parentConfig )
    {
        final Config config = parentConfig.getConfig( CONFIG_NAME );
        this.tupleQueueCapacity = config.getInt( TUPLE_QUEUE_CAPACITY );
        this.maxDrainableKeyCount = config.getInt( MAX_DRAINABLE_KEY_COUNT );
        this.partitionedTupleQueueDrainHint = config.getInt( PARTITIONED_TUPLE_QUEUE_DRAINER_HINT );
    }

    public int getTupleQueueCapacity ()
    {
        return tupleQueueCapacity;
    }

    public int getMaxDrainableKeyCount ()
    {
        return maxDrainableKeyCount;
    }

    public int getPartitionedTupleQueueDrainHint ()
    {
        return partitionedTupleQueueDrainHint;
    }

    @Override
    public String toString ()
    {
        return "TupleQueueManagerConfig{" + "tupleQueueCapacity=" + tupleQueueCapacity + ", maxDrainableKeyCount=" + maxDrainableKeyCount
               + ", partitionedTupleQueueDrainHint=" + partitionedTupleQueueDrainHint + '}';
    }

}
