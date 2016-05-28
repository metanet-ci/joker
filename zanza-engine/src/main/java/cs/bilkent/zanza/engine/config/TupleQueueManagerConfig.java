package cs.bilkent.zanza.engine.config;

import com.typesafe.config.Config;

public class TupleQueueManagerConfig
{

    public static final String CONFIG_NAME = "tupleQueueManager";

    public static final String TUPLE_QUEUE_INITIAL_SIZE = "tupleQueueInitialSize";


    private final int tupleQueueInitialSize;

    public TupleQueueManagerConfig ( final Config parentConfig )
    {
        final Config config = parentConfig.getConfig( CONFIG_NAME );
        this.tupleQueueInitialSize = config.getInt( TUPLE_QUEUE_INITIAL_SIZE );
    }

    public int getTupleQueueInitialSize ()
    {
        return tupleQueueInitialSize;
    }

}
