package cs.bilkent.joker.engine.config;

import org.junit.Test;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

import cs.bilkent.joker.test.AbstractJokerTest;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class JokerConfigTest extends AbstractJokerTest
{

    @Test
    public void shouldLoadDefaultConfig ()
    {
        final Config config = ConfigFactory.load();
        final JokerConfig jokerConfig = new JokerConfig( config );

        assertTrue( jokerConfig.getPipelineReplicaRunnerConfig().getRunnerWaitTimeoutInMillis() > 0 );
        assertTrue( jokerConfig.getTupleQueueManagerConfig().getTupleQueueCapacity() > 0 );
    }

    @Test
    public void shouldOverwriteDefaultConfigWithCustomConfigValue ()
    {
        final String configPath = JokerConfig.ENGINE_CONFIG_NAME + "." + TupleQueueManagerConfig.CONFIG_NAME + "."
                                  + TupleQueueManagerConfig.TUPLE_QUEUE_CAPACITY;

        final Config tupleQueueManagerConfig = ConfigFactory.empty().withValue( configPath, ConfigValueFactory.fromAnyRef( 250 ) );

        final Config config = ConfigFactory.load().withoutPath( configPath ).withFallback( tupleQueueManagerConfig );

        final JokerConfig jokerConfig = new JokerConfig( config );

        assertEquals( 250, jokerConfig.getTupleQueueManagerConfig().getTupleQueueCapacity() );
    }

}
