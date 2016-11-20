package cs.bilkent.joker.engine.kvstore.impl;

import java.util.HashSet;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import cs.bilkent.joker.engine.config.JokerConfig;
import cs.bilkent.joker.engine.kvstore.OperatorKVStore;
import cs.bilkent.joker.engine.partition.PartitionDistribution;
import cs.bilkent.joker.engine.partition.PartitionKey;
import cs.bilkent.joker.engine.partition.PartitionService;
import static cs.bilkent.joker.engine.partition.PartitionUtil.getPartitionId;
import cs.bilkent.joker.engine.partition.impl.PartitionKey1;
import cs.bilkent.joker.engine.partition.impl.PartitionServiceImpl;
import cs.bilkent.joker.operator.kvstore.KVStore;
import cs.bilkent.joker.test.AbstractJokerTest;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;


public class OperatorKVStoreManagerImplTest extends AbstractJokerTest
{

    private static final int REGION_ID = 1;

    private static final String OPERATOR_ID = "op1";


    private final Set<Object> keys = new HashSet<>();

    private PartitionService partitionService;

    private OperatorKVStoreManagerImpl kvStoreManager;

    @Before
    public void init ()
    {
        partitionService = new PartitionServiceImpl( new JokerConfig() );
        kvStoreManager = new OperatorKVStoreManagerImpl();
    }

    @Test
    public void shouldCreateDefaultOperatorKVStore ()
    {
        final OperatorKVStore operatorKvStore = kvStoreManager.createDefaultOperatorKVStore( REGION_ID, OPERATOR_ID );
        assertNotNull( operatorKvStore );
        assertEquals( operatorKvStore, kvStoreManager.getDefaultOperatorKVStore( REGION_ID, OPERATOR_ID ) );
    }

    @Test
    public void shouldPartitionedOperatorKVStore ()
    {
        final PartitionDistribution partitionDistribution = partitionService.createPartitionDistribution( REGION_ID, 2 );
        final OperatorKVStore[] operatorKvStores = kvStoreManager.createPartitionedOperatorKVStores( REGION_ID,
                                                                                                     OPERATOR_ID,
                                                                                                     partitionDistribution );
        assertNotNull( operatorKvStores );
        assertThat( operatorKvStores.length, equalTo( 2 ) );
        assertThat( kvStoreManager.getPartitionedOperatorKVStores( REGION_ID, OPERATOR_ID ), equalTo( operatorKvStores ) );
    }

    @Test
    public void shouldReleaseDefaultOperatorKVStore ()
    {
        final OperatorKVStore operatorKvStore = kvStoreManager.createDefaultOperatorKVStore( REGION_ID, OPERATOR_ID );
        final KVStore kvStore = operatorKvStore.getKVStore( null );
        kvStore.set( "key", "value" );
        kvStoreManager.releaseDefaultOperatorKVStore( REGION_ID, OPERATOR_ID );
        assertThat( kvStore.size(), equalTo( 0 ) );
    }

    @Test
    public void shouldReleasePartitionedOperatorKVStore ()
    {
        final PartitionDistribution partitionDistribution = partitionService.createPartitionDistribution( REGION_ID, 1 );
        final OperatorKVStore operatorKvStore = kvStoreManager.createPartitionedOperatorKVStores( REGION_ID,
                                                                                                  OPERATOR_ID,
                                                                                                  partitionDistribution )[ 0 ];
        final KVStore kvStore = operatorKvStore.getKVStore( new PartitionKey1( "key" ) );
        kvStore.set( "key", "value" );
        kvStoreManager.releasePartitionedOperatorKVStores( REGION_ID, OPERATOR_ID );
        assertThat( kvStore.size(), equalTo( 0 ) );
        assertNull( kvStoreManager.getPartitionedOperatorKVStores( REGION_ID, OPERATOR_ID ) );
    }

    @Test
    public void shouldShrinkPartitionedOperatorKVStores ()
    {
        testRebalancePartitionedOperatorKVStores( 4, 2 );
    }

    @Test
    public void shouldExtendPartitionedOperatorKVStores ()
    {
        testRebalancePartitionedOperatorKVStores( 2, 4 );
    }

    private void testRebalancePartitionedOperatorKVStores ( final int initialReplicaCount, final int newReplicaCount )
    {
        final PartitionDistribution partitionDistribution = partitionService.createPartitionDistribution( REGION_ID, initialReplicaCount );
        OperatorKVStore[] operatorKVStores = kvStoreManager.createPartitionedOperatorKVStores( REGION_ID,
                                                                                               OPERATOR_ID,
                                                                                               partitionDistribution );

        final Set<PartitionKey> keys = new HashSet<>();
        for ( int partitionId = 0; partitionId < partitionDistribution.getPartitionCount(); partitionId++ )
        {
            final int replicaIndex = partitionDistribution.getReplicaIndex( partitionId );
            final PartitionKey key = generatePartitionKey( partitionId );
            keys.add( key );
            operatorKVStores[ replicaIndex ].getKVStore( key ).set( "key", "val" );
        }

        final PartitionDistribution newPartitionDistribution = partitionService.rebalancePartitionDistribution( 1, newReplicaCount );
        operatorKVStores = kvStoreManager.rebalancePartitionedOperatorKVStores( REGION_ID,
                                                                                OPERATOR_ID,
                                                                                partitionDistribution,
                                                                                newPartitionDistribution );
        assertNotNull( operatorKVStores );
        assertEquals( newReplicaCount, operatorKVStores.length );

        for ( PartitionKey key : keys )
        {
            final int partitionId = getPartitionId( key.partitionHashCode(), partitionDistribution.getPartitionCount() );
            final int replicaIndex = newPartitionDistribution.getReplicaIndex( partitionId );
            final KVStore kvStore = operatorKVStores[ replicaIndex ].getKVStore( key );
            assertEquals( "val", kvStore.get( "key" ) );
        }
    }

    private PartitionKey generatePartitionKey ( final int partitionId )
    {
        int i = 0;
        PartitionKey1 key;
        while ( true )
        {
            key = new PartitionKey1( i );
            if ( keys.contains( i ) )
            {
                i++;
                continue;
            }

            final int partitionHash = key.partitionHashCode();

            if ( getPartitionId( partitionHash, partitionService.getPartitionCount() ) == partitionId )
            {
                keys.add( i );
                return key;
            }

            i++;
        }
    }

}
