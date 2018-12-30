package cs.bilkent.joker.engine.config;

import com.typesafe.config.Config;

public class TupleQueueManagerConfig
{

    static final String CONFIG_NAME = "tupleQueueManager";

    static final String TUPLE_QUEUE_CAPACITY = "tupleQueueCapacity";


    private final int tupleQueueCapacity;


    TupleQueueManagerConfig ( final Config parentConfig )
    {
        final Config config = parentConfig.getConfig( CONFIG_NAME );
        this.tupleQueueCapacity = config.getInt( TUPLE_QUEUE_CAPACITY );
    }

    public int getTupleQueueCapacity ()
    {
        return tupleQueueCapacity;
    }

    @Override
    public String toString ()
    {
        return "TupleQueueManagerConfig{" + "tupleQueueCapacity=" + tupleQueueCapacity + '}';
    }
}
