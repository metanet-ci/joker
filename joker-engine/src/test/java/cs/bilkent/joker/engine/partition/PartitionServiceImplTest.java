package cs.bilkent.joker.engine.partition;

import java.util.Arrays;
import java.util.Collection;
import java.util.Random;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import static com.typesafe.config.ConfigValueFactory.fromAnyRef;
import cs.bilkent.joker.engine.config.JokerConfig;
import static cs.bilkent.joker.engine.config.JokerConfig.ENGINE_CONFIG_NAME;
import static cs.bilkent.joker.engine.config.PartitionServiceConfig.CONFIG_NAME;
import static cs.bilkent.joker.engine.config.PartitionServiceConfig.PARTITION_COUNT;
import cs.bilkent.joker.engine.partition.impl.PartitionServiceImpl;
import cs.bilkent.joker.test.AbstractJokerTest;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

@RunWith( Parameterized.class )
public class PartitionServiceImplTest extends AbstractJokerTest
{

    @Parameters( name = "partitionCount={0}, initialReplicaCount={1}, newReplicaCount={2}" )
    public static Collection<Object[]> data ()
    {
        return asList( new Object[][] { { 200, 25, 15 },
                                        { 200, 25, 20 },
                                        { 200, 15, 25 },
                                        { 200, 15, 20 },
                                        { 200, 20, 15 },
                                        { 200, 20, 10 },
                                        { 200, 10, 20 },
                                        { 200, 10, 25 } } );
    }

    private static final Random RANDOM = new Random();

    private final int regionId = 1;

    private PartitionServiceImpl partitionService;


    private final int partitionCount;

    private final int initialReplicaCount;

    private final int newReplicaCount;

    public PartitionServiceImplTest ( final int partitionCount, final int initialReplicaCount, final int newReplicaCount )
    {
        this.partitionCount = partitionCount;
        this.initialReplicaCount = initialReplicaCount;
        this.newReplicaCount = newReplicaCount;
    }

    @Before
    public void init ()
    {
        final String configPath = ENGINE_CONFIG_NAME + "." + CONFIG_NAME + "." + PARTITION_COUNT;
        final Config config = ConfigFactory.load().withoutPath( configPath ).withValue( configPath, fromAnyRef( partitionCount ) );

        final JokerConfig jokerConfig = new JokerConfig( config );
        this.partitionService = new PartitionServiceImpl( jokerConfig );
    }

    @Test
    public void shouldCreatePartitionDistribution ()
    {
        final PartitionDistribution partitionDistribution = partitionService.createPartitionDistribution( regionId, 1 );
        assertNotNull( partitionDistribution );
    }

    @Test( expected = IllegalStateException.class )
    public void shouldCreatePartitionDistributionOnlyOnce ()
    {
        try
        {
            partitionService.createPartitionDistribution( regionId, 1 );
        }
        catch ( Exception e )
        {
            e.printStackTrace();
            fail();
        }

        partitionService.createPartitionDistribution( regionId, 1 );
    }

    @Test
    public void shouldNotGetNonExistingPartitionDistribution ()
    {
        assertNull( partitionService.getPartitionDistribution( regionId ) );
    }

    @Test( expected = IllegalStateException.class )
    public void shouldFailWhenGetNonExistingPartitionDistribution ()
    {
        partitionService.getPartitionDistributionOrFail( regionId );
    }

    @Test
    public void shouldDistributePartitions ()
    {
        final PartitionDistribution distribution = partitionService.createPartitionDistribution( regionId, initialReplicaCount );
        validateDistribution( partitionCount, initialReplicaCount, distribution.getDistribution() );
    }

    @Test
    public void shouldRedistributePartitions ()
    {
        partitionService.createPartitionDistribution( regionId, initialReplicaCount );

        final PartitionDistribution distribution = partitionService.rebalancePartitionDistribution( regionId, newReplicaCount );
        validateDistribution( partitionCount, newReplicaCount, distribution.getDistribution() );
    }

    @Test
    public void shouldRedistributePartitionsMultipleTimes ()
    {
        partitionService.createPartitionDistribution( regionId, initialReplicaCount );

        for ( int i = 0; i < 5000; i++ )
        {
            final int newReplicaCount = 1 + RANDOM.nextInt( 25 );
            final PartitionDistribution distribution = partitionService.rebalancePartitionDistribution( regionId, newReplicaCount );
            validateDistribution( partitionCount, newReplicaCount, distribution.getDistribution() );
        }
    }

    @Test
    public void shouldGetRedistributedPartitions ()
    {
        partitionService.createPartitionDistribution( regionId, initialReplicaCount );

        final PartitionDistribution distribution = partitionService.rebalancePartitionDistribution( regionId, newReplicaCount );
        assertEquals( distribution, partitionService.getPartitionDistribution( regionId ) );
    }

    private void validateDistribution ( final int partitionCount, final int replicaCount, final int[] distribution )
    {
        final int[] ownedPartitionCountsByReplicaIndex = new int[ replicaCount ];
        for ( int i = 0; i < partitionCount; i++ )
        {
            ownedPartitionCountsByReplicaIndex[ distribution[ i ] ]++;
        }

        final int normalCapacity = partitionCount / replicaCount;
        final int overCapacity = normalCapacity + 1;

        int overCapacityCount = partitionCount % replicaCount;
        int normalCapacityCount = replicaCount - overCapacityCount;
        for ( int i = 0; i < ownedPartitionCountsByReplicaIndex.length; i++ )
        {
            if ( ownedPartitionCountsByReplicaIndex[ i ] == normalCapacity )
            {
                normalCapacityCount--;
            }
            else if ( ownedPartitionCountsByReplicaIndex[ i ] == overCapacity )
            {
                overCapacityCount--;
            }
            else
            {
                fail( "invalid distribution: " + Arrays.toString( distribution ) );
            }
        }

        assertEquals( "invalid distribution: " + Arrays.toString( distribution ), 0, normalCapacityCount );
        assertEquals( "invalid distribution: " + Arrays.toString( distribution ), 0, overCapacityCount );
    }

}
