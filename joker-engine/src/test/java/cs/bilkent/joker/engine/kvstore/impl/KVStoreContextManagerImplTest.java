package cs.bilkent.joker.engine.kvstore.impl;

import org.junit.Before;
import org.junit.Test;

import cs.bilkent.joker.engine.config.JokerConfig;
import cs.bilkent.joker.engine.kvstore.KVStoreContext;
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


public class KVStoreContextManagerImplTest extends AbstractJokerTest
{

    private static final int REGION_ID = 1;

    private static final String OPERATOR_ID = "op1";

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
        final KVStoreContext kvStoreContext = kvStoreManager.createDefaultKVStoreContext( REGION_ID, OPERATOR_ID );
        assertNotNull( kvStoreContext );
        assertEquals( kvStoreContext, kvStoreManager.getDefaultKVStoreContext( REGION_ID, OPERATOR_ID ) );
    }

    @Test
    public void shouldPartitionedDefaultKVStoreContext ()
    {
        final KVStoreContext[] kvStoreContexts = kvStoreManager.createPartitionedKVStoreContexts( REGION_ID, 2, OPERATOR_ID );
        assertNotNull( kvStoreContexts );
        assertThat( kvStoreContexts.length, equalTo( 2 ) );
        assertThat( kvStoreManager.getPartitionedKVStoreContexts( REGION_ID, OPERATOR_ID ), equalTo( kvStoreContexts ) );
    }

    @Test
    public void shouldReleaseDefaultKVStoreContext ()
    {
        final KVStoreContext kvStoreContext = kvStoreManager.createDefaultKVStoreContext( REGION_ID, OPERATOR_ID );
        final KVStore kvStore = kvStoreContext.getKVStore( null );
        kvStore.set( "key", "value" );
        assertTrue( kvStoreManager.releaseDefaultKVStoreContext( REGION_ID, OPERATOR_ID ) );
        assertThat( kvStore.size(), equalTo( 0 ) );
    }

    @Test
    public void shouldReleasePartitionedKVStoreContext ()
    {
        final KVStoreContext kvStoreContext = kvStoreManager.createPartitionedKVStoreContexts( REGION_ID, 1, OPERATOR_ID )[ 0 ];
        final KVStore kvStore = kvStoreContext.getKVStore( new PartitionKey1( "key" ) );
        kvStore.set( "key", "value" );
        assertTrue( kvStoreManager.releasePartitionedKVStoreContext( REGION_ID, OPERATOR_ID ) );
        assertThat( kvStore.size(), equalTo( 0 ) );
        assertNull( kvStoreManager.getPartitionedKVStoreContexts( REGION_ID, OPERATOR_ID ) );
    }

}
