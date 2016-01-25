package cs.bilkent.zanza.engine.kvstore.impl;

import org.junit.Test;

import cs.bilkent.zanza.engine.kvstore.KVStoreContext;
import cs.bilkent.zanza.kvstore.KVStore;
import static cs.bilkent.zanza.operator.spec.OperatorType.PARTITIONED_STATEFUL;
import static cs.bilkent.zanza.operator.spec.OperatorType.STATEFUL;
import static cs.bilkent.zanza.operator.spec.OperatorType.STATELESS;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


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
    public void shouldNotCreatePartitionedStatefulOperatorWithNullPartitionFieldNames ()
    {
        kvStoreManager.createKVStoreContext( "op1", PARTITIONED_STATEFUL, null, 1 );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotCreatePartitionedStatefulOperatorWithEmptyPartitionFieldNames ()
    {
        kvStoreManager.createKVStoreContext( "op1", PARTITIONED_STATEFUL, emptyList(), 1 );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotCreateStatefulOperatorWithWithPartitionFieldNames ()
    {
        kvStoreManager.createKVStoreContext( "op1", STATEFUL, singletonList( "field1" ), 1 );
    }

    @Test
    public void shouldCreateStatefulOperatorWithWithEmptyPartitionFieldNames ()
    {
        kvStoreManager.createKVStoreContext( "op1", STATEFUL, emptyList(), 1 );
    }

    @Test
    public void shouldCreateStatefulOperatorWithWithNullPartitionFieldNames ()
    {
        kvStoreManager.createKVStoreContext( "op1", STATEFUL, null, 1 );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotCreateStatelessOperatorWithPartitionFieldNames ()
    {
        kvStoreManager.createKVStoreContext( "op1", STATELESS, singletonList( "field1" ), 1 );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotCreateStatelessOperatorWithEmptyPartitionFieldNames ()
    {
        kvStoreManager.createKVStoreContext( "op1", STATELESS, emptyList(), 1 );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotCreateStatelessOperatorWithNullPartitionFieldNames ()
    {
        kvStoreManager.createKVStoreContext( "op1", STATELESS, null, 1 );
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
                                                                                                            singletonList( "field1" ),
                                                                                                            2 );
        assertThat( kvStoreContext.getKVStoresSize(), equalTo( 2 ) );
    }

    @Test
    public void shouldCreateKVStoreContextOnlyOnceForAnOperator ()
    {
        final KVStoreContext kvStoreContext1 = kvStoreManager.createKVStoreContext( "op1",
                                                                                    PARTITIONED_STATEFUL,
                                                                                    singletonList( "field1" ),
                                                                                    2 );
        final KVStoreContext kvStoreContext2 = kvStoreManager.createKVStoreContext( "op1",
                                                                                    PARTITIONED_STATEFUL,
                                                                                    singletonList( "field1" ),
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
