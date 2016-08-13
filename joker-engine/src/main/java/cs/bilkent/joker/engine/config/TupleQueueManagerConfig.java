package cs.bilkent.joker.engine.config;

import com.typesafe.config.Config;

public class TupleQueueManagerConfig
{

    static final String CONFIG_NAME = "tupleQueueManager";

    static final String TUPLE_QUEUE_INITIAL_SIZE = "tupleQueueInitialSize";


    private final int tupleQueueInitialSize;

    TupleQueueManagerConfig ( final Config parentConfig )
    {
        final Config config = parentConfig.getConfig( CONFIG_NAME );
        this.tupleQueueInitialSize = config.getInt( TUPLE_QUEUE_INITIAL_SIZE );
    }

    public int getTupleQueueInitialSize ()
    {
        return tupleQueueInitialSize;
    }

}
