package cs.bilkent.joker.engine.kvstore.impl;

import org.junit.Before;
import org.junit.Test;

import cs.bilkent.joker.engine.config.JokerConfig;
import cs.bilkent.joker.engine.kvstore.KVStoreContext;
import cs.bilkent.joker.engine.partition.PartitionService;
import cs.bilkent.joker.engine.partition.PartitionServiceImpl;
import cs.bilkent.joker.operator.kvstore.KVStore;
import cs.bilkent.joker.operator.kvstore.impl.KeyDecoratedKVStore;
import cs.bilkent.joker.testutils.AbstractJokerTest;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;


public class KVStoreContextManagerImplTest extends AbstractJokerTest
{

    private static final int REGION_ID = 1;


    private PartitionService partitionService;

    private KVStoreContextManagerImpl kvStoreManager;

    @Before
    public void init ()
    {
        partitionService = new PartitionServiceImpl( new JokerConfig() );
        kvStoreManager = new KVStoreContextManagerImpl( partitionService );
    }

    @Test
    public void shouldCreateDefaultKVStoreContext ()
    {
        final KVStoreContext kvStoreContext = kvStoreManager.createDefaultKVStoreContext( REGION_ID, "op1" );
        assertNotNull( kvStoreContext );
        assertThat( kvStoreManager.getKVStores( REGION_ID, "op1" ).length, equalTo( 1 ) );
    }

    @Test
    public void shouldPartitionedDefaultKVStoreContext ()
    {
        final KVStoreContext[] kvStoreContexts = kvStoreManager.createPartitionedKVStoreContexts( REGION_ID, 2, "op1" );
        assertNotNull( kvStoreContexts );
        assertThat( kvStoreContexts.length, equalTo( 2 ) );
        assertThat( kvStoreManager.getKVStores( REGION_ID, "op1" ).length, equalTo( partitionService.getPartitionCount() ) );
    }

    @Test
    public void shouldReleaseDefaultKVStoreContext ()
    {
        final KVStoreContext kvStoreContext = kvStoreManager.createDefaultKVStoreContext( REGION_ID, "op1" );
        final KVStore kvStore = kvStoreContext.getKVStore( null );
        kvStore.put( "key", "value" );
        assertTrue( kvStoreManager.releaseDefaultKVStoreContext( REGION_ID, "op1" ) );
        assertThat( kvStore.size(), equalTo( 0 ) );
        assertNull( kvStoreManager.getKVStores( REGION_ID, "op1" ) );
    }

    @Test
    public void shouldReleasePartitionedKVStoreContext ()
    {
        final KVStoreContext kvStoreContext = kvStoreManager.createPartitionedKVStoreContexts( REGION_ID, 1, "op1" )[ 0 ];
        final KeyDecoratedKVStore kvStore = (KeyDecoratedKVStore) kvStoreContext.getKVStore( "key" );
        kvStore.put( "key", "value" );
        assertTrue( kvStoreManager.releasePartitionedKVStoreContext( REGION_ID, "op1" ) );
        assertThat( kvStore.getKvStore().size(), equalTo( 0 ) );
        assertNull( kvStoreManager.getKVStores( REGION_ID, "op1" ) );
    }

}
