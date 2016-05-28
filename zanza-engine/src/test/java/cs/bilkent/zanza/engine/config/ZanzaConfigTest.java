package cs.bilkent.zanza.engine.config;

import org.junit.Test;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

import cs.bilkent.zanza.engine.config.ZanzaConfig.PipelineInstanceRunnerConfig;
import cs.bilkent.zanza.engine.config.ZanzaConfig.TupleQueueDrainerConfig;
import cs.bilkent.zanza.engine.config.ZanzaConfig.TupleQueueManagerConfig;
import static org.junit.Assert.assertEquals;

public class ZanzaConfigTest
{

    @Test
    public void shouldLoadDefaultConfig ()
    {
        final Config config = ConfigFactory.load();
        final ZanzaConfig zanzaConfig = new ZanzaConfig( config );

        final Config pipelineInstanceRunnerConfig = zanzaConfig.getPipelineInstanceRunnerConfig();
        assertEquals( 100, pipelineInstanceRunnerConfig.getInt( PipelineInstanceRunnerConfig.RUNNER_WAIT_TIME_IN_MILLIS ) );

        assertEquals( 100, zanzaConfig.getTupleQueueManagerConfig().getInt( TupleQueueManagerConfig.TUPLE_QUEUE_INITIAL_SIZE ) );
        assertEquals( 100, zanzaConfig.getTupleQueueDrainerConfig().getInt( TupleQueueDrainerConfig.DRAIN_TIMEOUT_IN_MILLIS ) );

        assertEquals( 100, config.getInt( TupleQueueManagerConfig.TUPLE_QUEUE_INITIAL_SIZE_FULL_PATH ) );
        assertEquals( 100, config.getInt( TupleQueueDrainerConfig.DRAIN_TIMEOUT_IN_MILLIS_FULL_PATH ) );
        assertEquals( 100, config.getInt( PipelineInstanceRunnerConfig.RUNNER_WAIT_TIME_IN_MILLIS_FULL_PATH ) );
    }

    @Test
    public void shouldOverwriteDefaultConfigWithConfFile ()
    {
        final Config overridingConfig = ConfigFactory.empty()
                                                     .withValue( TupleQueueManagerConfig.TUPLE_QUEUE_INITIAL_SIZE_FULL_PATH,
                                                                 ConfigValueFactory.fromAnyRef( 250 ) );

        final Config config = ConfigFactory.load( overridingConfig );
        final ZanzaConfig zanzaConfig = new ZanzaConfig( config );

        final Config tupleQueueConfig = zanzaConfig.getTupleQueueManagerConfig();
        assertEquals( 250, tupleQueueConfig.getInt( TupleQueueManagerConfig.TUPLE_QUEUE_INITIAL_SIZE ) );
    }

}
