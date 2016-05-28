package cs.bilkent.zanza.engine.config;

import org.junit.Test;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

import static org.junit.Assert.assertEquals;

public class ZanzaConfigTest
{

    @Test
    public void shouldLoadDefaultConfig ()
    {
        final Config config = ConfigFactory.load();
        final ZanzaConfig zanzaConfig = new ZanzaConfig( config );

        assertEquals( 100, zanzaConfig.getPipelineInstanceRunnerConfig().getWaitTimeoutInMillis() );
        assertEquals( 100, zanzaConfig.getTupleQueueManagerConfig().getTupleQueueInitialSize() );
        assertEquals( 100, zanzaConfig.getTupleQueueDrainerConfig().getDrainTimeoutInMillis() );
    }

    @Test
    public void shouldOverwriteDefaultConfigWithConfFile ()
    {
        final String configPath = ZanzaConfig.ENGINE_CONFIG_NAME + "." + TupleQueueManagerConfig.CONFIG_NAME + "."
                                  + TupleQueueManagerConfig.TUPLE_QUEUE_INITIAL_SIZE;

        final Config tupleQueueManagerConfig = ConfigFactory.empty().withValue( configPath, ConfigValueFactory.fromAnyRef( 250 ) );

        final Config config = ConfigFactory.load().withoutPath( configPath ).withFallback( tupleQueueManagerConfig );

        final ZanzaConfig zanzaConfig = new ZanzaConfig( config );

        assertEquals( 250, zanzaConfig.getTupleQueueManagerConfig().getTupleQueueInitialSize() );
    }

}
