package cs.bilkent.zanza.engine.config;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class ZanzaConfig
{

    public static final String ROOT_PATH_NAME = "zanza.engine";


    public static final class PipelineInstanceRunnerConfig
    {

        public static final String CONFIG_FIELD_NAME = "pipelineInstanceRunner";

        public static final String CONFIG_FIELD_NAME_FULL_PATH = ROOT_PATH_NAME + "." + CONFIG_FIELD_NAME;

        public static final String RUNNER_WAIT_TIME_IN_MILLIS = "runnerWaitTimeInMillis";

        public static final String RUNNER_WAIT_TIME_IN_MILLIS_FULL_PATH =
                ROOT_PATH_NAME + "." + CONFIG_FIELD_NAME + "." + RUNNER_WAIT_TIME_IN_MILLIS;

        private PipelineInstanceRunnerConfig ()
        {
        }

    }


    public static final class TupleQueueManagerConfig
    {

        public static final String CONFIG_FIELD_NAME = "tupleQueueManager";

        public static final String CONFIG_FIELD_NAME_FULL_PATH = ROOT_PATH_NAME + "." + CONFIG_FIELD_NAME;

        public static final String TUPLE_QUEUE_INITIAL_SIZE = "tupleQueueInitialSize";

        public static final String TUPLE_QUEUE_INITIAL_SIZE_FULL_PATH =
                ROOT_PATH_NAME + "." + CONFIG_FIELD_NAME + "." + TUPLE_QUEUE_INITIAL_SIZE;

        private TupleQueueManagerConfig ()
        {
        }

    }


    public static final class TupleQueueDrainerConfig
    {

        public static final String CONFIG_FIELD_NAME = "tupleQueueDrainer";

        public static final String CONFIG_FIELD_NAME_FULL_PATH = ROOT_PATH_NAME + "." + CONFIG_FIELD_NAME;

        public static final String DRAIN_TIMEOUT_IN_MILLIS = "drainTimeoutInMillis";

        public static final String DRAIN_TIMEOUT_IN_MILLIS_FULL_PATH =
                ROOT_PATH_NAME + "." + CONFIG_FIELD_NAME + "." + DRAIN_TIMEOUT_IN_MILLIS;

        private TupleQueueDrainerConfig ()
        {
        }

    }


    private final Config config;

    public ZanzaConfig ()
    {
        this( ConfigFactory.load() );
    }

    public ZanzaConfig ( final Config config )
    {
        this.config = config;
    }

    public Config getConfig ()
    {
        return config;
    }

    public Config getPipelineInstanceRunnerConfig ()
    {
        return config.getConfig( PipelineInstanceRunnerConfig.CONFIG_FIELD_NAME_FULL_PATH );
    }

    public Config getTupleQueueManagerConfig ()
    {
        return config.getConfig( TupleQueueManagerConfig.CONFIG_FIELD_NAME_FULL_PATH );
    }

    public Config getTupleQueueDrainerConfig ()
    {
        return config.getConfig( TupleQueueDrainerConfig.CONFIG_FIELD_NAME_FULL_PATH );
    }

    public int getInt ( final String fullPath )
    {
        return config.getInt( fullPath );
    }

    public long getLong ( final String fullPath )
    {
        return config.getLong( fullPath );
    }

}
