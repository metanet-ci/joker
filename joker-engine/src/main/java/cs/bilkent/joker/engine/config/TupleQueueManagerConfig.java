package cs.bilkent.joker.engine.config;

import com.typesafe.config.Config;

import static cs.bilkent.joker.engine.config.ThreadingPref.MULTI_THREADED;
import static cs.bilkent.joker.engine.config.ThreadingPref.SINGLE_THREADED;
import static cs.bilkent.joker.impl.com.google.common.base.Preconditions.checkArgument;

public class TupleQueueManagerConfig
{

    static final String CONFIG_NAME = "tupleQueueManager";

    static final String TUPLE_QUEUE_CAPACITY = "tupleQueueCapacity";

    static final String MULTI_THREADED_QUEUE_DRAIN_LIMIT = "multiThreadedQueueDrainLimit";


    private final int tupleQueueCapacity;

    private final int multiThreadedQueueDrainLimit;

    TupleQueueManagerConfig ( final Config parentConfig )
    {
        final Config config = parentConfig.getConfig( CONFIG_NAME );
        this.tupleQueueCapacity = config.getInt( TUPLE_QUEUE_CAPACITY );
        this.multiThreadedQueueDrainLimit = config.getInt( MULTI_THREADED_QUEUE_DRAIN_LIMIT );
    }

    public int getTupleQueueCapacity ()
    {
        return tupleQueueCapacity;
    }

    public int getMultiThreadedQueueDrainLimit ()
    {
        return multiThreadedQueueDrainLimit;
    }

    public int getDrainLimit ( final ThreadingPref threadingPref )
    {
        checkArgument( threadingPref == MULTI_THREADED || threadingPref == SINGLE_THREADED );
        return threadingPref == MULTI_THREADED ? multiThreadedQueueDrainLimit : Integer.MAX_VALUE;
    }

    @Override
    public String toString ()
    {
        return "TupleQueueManagerConfig{" + "tupleQueueCapacity=" + tupleQueueCapacity + ", multiThreadedQueueDrainLimit="
               + multiThreadedQueueDrainLimit + '}';
    }

}
