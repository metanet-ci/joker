package cs.bilkent.zanza.engine.kvstore.impl;

import org.junit.Test;

import cs.bilkent.zanza.engine.kvstore.KVStoreContext;
import cs.bilkent.zanza.kvstore.KVStore;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;


public class KVStoreManagerImplTest
{

    private final KVStoreManagerImpl kvStoreManager = new KVStoreManagerImpl();

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotCreateKVStoreWithoutOperatorId ()
    {
        kvStoreManager.createKVStoreContext( null, 1 );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotCreateKVStoreWithInvalidReplicaCount ()
    {
        kvStoreManager.createKVStoreContext( "op1", 0 );
    }

    @Test
    public void shouldCreateKVStoreContext ()
    {
        final KVStoreContextImpl kvStoreContext = (KVStoreContextImpl) kvStoreManager.createKVStoreContext( "op1", 2 );
        assertNotNull( kvStoreContext );
        assertThat( kvStoreContext.getKVStoreCount(), equalTo( 2 ) );
    }

    @Test
    public void shouldReleaseCleanKVStoreContext ()
    {
        final KVStoreContext kvStoreContext = kvStoreManager.createKVStoreContext( "op1", 1 );
        final KVStore kvStore = kvStoreContext.getKVStore();
        kvStore.put( 1, 1 );
        assertTrue( kvStoreManager.releaseKVStoreContext( "op1" ) );
        assertThat( kvStore.size(), equalTo( 0 ) );
    }

    @Test
    public void shouldReCreateReleasedKVStoreContext ()
    {
        final KVStoreContext kvStoreContext1 = kvStoreManager.createKVStoreContext( "op1", 1 );
        kvStoreManager.releaseKVStoreContext( "op1" );
        final KVStoreContext kvStoreContext2 = kvStoreManager.createKVStoreContext( "op1", 1 );
        assertFalse( kvStoreContext1 == kvStoreContext2 );
    }

    @Test
    public void shouldNotReleaseNonExistingKVStoreContext ()
    {
        assertFalse( kvStoreManager.releaseKVStoreContext( "op1" ) );
    }

}
