package cs.bilkent.zanza.engine.kvstore.impl;

import org.junit.Test;

import cs.bilkent.zanza.engine.kvstore.KVStoreContext;
import cs.bilkent.zanza.kvstore.KVStore;
import cs.bilkent.zanza.operator.PartitionKeyExtractor;
import static cs.bilkent.zanza.operator.spec.OperatorType.PARTITIONED_STATEFUL;
import static cs.bilkent.zanza.operator.spec.OperatorType.STATEFUL;
import static cs.bilkent.zanza.operator.spec.OperatorType.STATELESS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class KVStoreManagerImplTest
{

    private final KVStoreManagerImpl kvStoreManager = new KVStoreManagerImpl();

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotCreateKVStoreWithoutOperatorId ()
    {
        kvStoreManager.createKVStoreContext( null, STATEFUL, null, 1 );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotCreateKVStoreWithoutOperatorType ()
    {
        kvStoreManager.createKVStoreContext( "opId", null, null, 1 );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotCreateKVStoreWithInvalidKVStoreCount ()
    {
        kvStoreManager.createKVStoreContext( "op1", STATEFUL, null, 0 );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotCreatePartitionedStatefulOperatorWithoutPartitionKeyExtractor ()
    {
        kvStoreManager.createKVStoreContext( "op1", PARTITIONED_STATEFUL, null, 1 );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotCreateStatefulOperatorWithWithPartitionKeyExtractor ()
    {
        kvStoreManager.createKVStoreContext( "op1", STATEFUL, mock( PartitionKeyExtractor.class ), 1 );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotCreateStatelessOperatorWithWithPartitionKeyExtractor ()
    {
        kvStoreManager.createKVStoreContext( "op1", STATELESS, mock( PartitionKeyExtractor.class ), 1 );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotCreateMultipleKVStoresForNonStatefulOperator ()
    {
        kvStoreManager.createKVStoreContext( "op1", STATEFUL, null, 2 );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotCreateKVStoreContextForStatelessOperator ()
    {
        kvStoreManager.createKVStoreContext( "op1", STATELESS, null, 1 );
    }

    @Test
    public void shouldCreateSingleKVStoreForStatefulOperator ()
    {
        final KVStoreContextImpl kvStoreContext = (KVStoreContextImpl) kvStoreManager.createKVStoreContext( "op1", STATEFUL, null, 1 );
        assertThat( kvStoreContext.getKVStoresSize(), equalTo( 1 ) );
    }

    @Test
    public void shouldCreateSingleKVStoreForPartitionedStatefulOperator ()
    {
        final KVStoreContextImpl kvStoreContext = (KVStoreContextImpl) kvStoreManager.createKVStoreContext( "op1", STATEFUL, null, 1 );
        assertThat( kvStoreContext.getKVStoresSize(), equalTo( 1 ) );
    }

    @Test
    public void shouldCreateMultipleKVStoresForPartitionedStatefulOperator ()
    {
        final KVStoreContextImpl kvStoreContext = (KVStoreContextImpl) kvStoreManager.createKVStoreContext( "op1",
                                                                                                            PARTITIONED_STATEFUL,
                                                                                                            mock( PartitionKeyExtractor.class ),
                                                                                                            2 );
        assertThat( kvStoreContext.getKVStoresSize(), equalTo( 2 ) );
    }

    @Test
    public void shouldCreateKVStoreContextOnlyOnceForAnOperator ()
    {
        final KVStoreContext kvStoreContext1 = kvStoreManager.createKVStoreContext( "op1",
                                                                                    PARTITIONED_STATEFUL,
                                                                                    mock( PartitionKeyExtractor.class ),
                                                                                    2 );
        final KVStoreContext kvStoreContext2 = kvStoreManager.createKVStoreContext( "op1",
                                                                                    PARTITIONED_STATEFUL,
                                                                                    mock( PartitionKeyExtractor.class ),
                                                                                    2 );
        assertTrue( kvStoreContext1 == kvStoreContext2 );
    }

    @Test
    public void shouldReleaseCleanKVStoreContext ()
    {
        final KVStoreContext kvStoreContext = kvStoreManager.createKVStoreContext( "op1", STATEFUL, null, 1 );
        final KVStore kvStore = kvStoreContext.getKVStore();
        kvStore.put( 1, 1 );
        assertTrue( kvStoreManager.releaseKVStoreContext( "op1" ) );
        assertThat( kvStore.size(), equalTo( 0 ) );
    }

    @Test
    public void shouldReCreateReleasedKVStoreContext ()
    {
        final KVStoreContext kvStoreContext1 = kvStoreManager.createKVStoreContext( "op1", STATEFUL, null, 1 );
        kvStoreManager.releaseKVStoreContext( "op1" );
        final KVStoreContext kvStoreContext2 = kvStoreManager.createKVStoreContext( "op1", STATEFUL, null, 1 );
        assertFalse( kvStoreContext1 == kvStoreContext2 );
    }

    @Test
    public void shouldNotReleaseNonExistingKVStoreContext ()
    {
        assertFalse( kvStoreManager.releaseKVStoreContext( "op1" ) );
    }

}
