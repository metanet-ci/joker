package cs.bilkent.zanza.engine.config;

import org.junit.Test;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

import cs.bilkent.zanza.testutils.ZanzaTest;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ZanzaConfigTest extends ZanzaTest
{

    @Test
    public void shouldLoadDefaultConfig ()
    {
        final Config config = ConfigFactory.load();
        final ZanzaConfig zanzaConfig = new ZanzaConfig( config );

        assertTrue( zanzaConfig.getPipelineReplicaRunnerConfig().getWaitTimeoutInMillis() > 0 );
        assertTrue( zanzaConfig.getTupleQueueManagerConfig().getTupleQueueInitialSize() > 0 );
        assertTrue( zanzaConfig.getTupleQueueDrainerConfig().getDrainTimeoutInMillis() > 0 );
    }

    @Test
    public void shouldOverwriteDefaultConfigWithCustomConfigValue ()
    {
        final String configPath = ZanzaConfig.ENGINE_CONFIG_NAME + "." + TupleQueueManagerConfig.CONFIG_NAME + "."
                                  + TupleQueueManagerConfig.TUPLE_QUEUE_INITIAL_SIZE;

        final Config tupleQueueManagerConfig = ConfigFactory.empty().withValue( configPath, ConfigValueFactory.fromAnyRef( 250 ) );

        final Config config = ConfigFactory.load().withoutPath( configPath ).withFallback( tupleQueueManagerConfig );

        final ZanzaConfig zanzaConfig = new ZanzaConfig( config );

        assertEquals( 250, zanzaConfig.getTupleQueueManagerConfig().getTupleQueueInitialSize() );
    }

}
