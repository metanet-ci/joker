package cs.bilkent.zanza.engine.partition;

import java.util.Arrays;

import org.junit.Before;
import org.junit.Test;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

import cs.bilkent.zanza.engine.config.PartitionServiceConfig;
import cs.bilkent.zanza.engine.config.ZanzaConfig;
import cs.bilkent.zanza.testutils.ZanzaTest;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class PartitionServiceImplTest extends ZanzaTest
{

    private final static int PARTITION_COUNT = 15;

    private PartitionServiceImpl partitionService;

    @Before
    public void before ()
    {
        final String configPath =
                ZanzaConfig.ENGINE_CONFIG_NAME + "." + PartitionServiceConfig.CONFIG_NAME + "." + PartitionServiceConfig.PARTITION_COUNT;
        final Config config = ConfigFactory.load()
                                           .withoutPath( configPath )
                                           .withValue( configPath, ConfigValueFactory.fromAnyRef( PARTITION_COUNT ) );

        final ZanzaConfig zanzaConfig = new ZanzaConfig( config );
        partitionService = new PartitionServiceImpl( zanzaConfig );
    }

    @Test
    public void shouldDistributePartitions ()
    {
        final int regionId = 1;
        final int replicaCount = 5;
        final int[] partitions = partitionService.getOrCreatePartitionDistribution( regionId, replicaCount );
        final int[] replicaCounts = new int[ replicaCount ];
        for ( int i = 0; i < PARTITION_COUNT; i++ )
        {
            replicaCounts[ partitions[ i ] ]++;
        }

        final int[] expected = new int[ replicaCount ];
        Arrays.fill( expected, PARTITION_COUNT / replicaCount );

        assertThat( replicaCounts, equalTo( expected ) );
    }

}
