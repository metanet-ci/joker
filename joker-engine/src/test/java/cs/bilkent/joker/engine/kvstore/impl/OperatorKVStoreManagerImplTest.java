package cs.bilkent.joker.engine.kvstore.impl;

import org.junit.Before;
import org.junit.Test;

import cs.bilkent.joker.engine.config.JokerConfig;
import cs.bilkent.joker.engine.kvstore.OperatorKVStore;
import cs.bilkent.joker.engine.partition.PartitionService;
import cs.bilkent.joker.engine.partition.PartitionServiceImpl;
import cs.bilkent.joker.engine.partition.impl.PartitionKey1;
import cs.bilkent.joker.operator.kvstore.KVStore;
import cs.bilkent.joker.testutils.AbstractJokerTest;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;


public class OperatorKVStoreManagerImplTest extends AbstractJokerTest
{

    private static final int REGION_ID = 1;

    private static final String OPERATOR_ID = "op1";

    private PartitionService partitionService;

    private OperatorKVStoreManagerImpl kvStoreManager;

    @Before
    public void init ()
    {
        partitionService = new PartitionServiceImpl( new JokerConfig() );
        kvStoreManager = new OperatorKVStoreManagerImpl( partitionService );
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
        final OperatorKVStore[] operatorKvStores = kvStoreManager.createPartitionedOperatorKVStore( REGION_ID, 2, OPERATOR_ID );
        assertNotNull( operatorKvStores );
        assertThat( operatorKvStores.length, equalTo( 2 ) );
        assertThat( kvStoreManager.getPartitionedOperatorKVStore( REGION_ID, OPERATOR_ID ), equalTo( operatorKvStores ) );
    }

    @Test
    public void shouldReleaseDefaultOperatorKVStore ()
    {
        final OperatorKVStore operatorKvStore = kvStoreManager.createDefaultOperatorKVStore( REGION_ID, OPERATOR_ID );
        final KVStore kvStore = operatorKvStore.getKVStore( null );
        kvStore.set( "key", "value" );
        assertTrue( kvStoreManager.releaseDefaultOperatorKVStore( REGION_ID, OPERATOR_ID ) );
        assertThat( kvStore.size(), equalTo( 0 ) );
    }

    @Test
    public void shouldReleasePartitionedOperatorKVStore ()
    {
        final OperatorKVStore operatorKvStore = kvStoreManager.createPartitionedOperatorKVStore( REGION_ID, 1, OPERATOR_ID )[ 0 ];
        final KVStore kvStore = operatorKvStore.getKVStore( new PartitionKey1( "key" ) );
        kvStore.set( "key", "value" );
        assertTrue( kvStoreManager.releasePartitionedOperatorKVStore( REGION_ID, OPERATOR_ID ) );
        assertThat( kvStore.size(), equalTo( 0 ) );
        assertNull( kvStoreManager.getPartitionedOperatorKVStore( REGION_ID, OPERATOR_ID ) );
    }

}
