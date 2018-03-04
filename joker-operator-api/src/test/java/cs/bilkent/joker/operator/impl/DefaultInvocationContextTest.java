package cs.bilkent.joker.operator.impl;

import java.util.function.Function;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import cs.bilkent.joker.operator.Tuple;
import cs.bilkent.joker.operator.kvstore.KVStore;
import cs.bilkent.joker.partition.impl.PartitionKey;
import cs.bilkent.joker.test.AbstractJokerTest;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith( MockitoJUnitRunner.class )
public class DefaultInvocationContextTest extends AbstractJokerTest
{

    @Mock
    private Function<PartitionKey, KVStore> kvStoreSupplier;

    @Test
    public void testSingleInput ()
    {
        final DefaultInvocationContext ctx = new DefaultInvocationContext( 1, kvStoreSupplier, new TuplesImpl( 1 ) );

        final PartitionKey key = mock( PartitionKey.class );
        final KVStore kvStore = mock( KVStore.class );
        when( kvStoreSupplier.apply( key ) ).thenReturn( kvStore );

        final Tuple tuple = new Tuple();
        final TuplesImpl tuples = ctx.createInputTuples( key );
        tuples.add( tuple );

        assertThat( ctx.getInputCount(), equalTo( 1 ) );
        final TuplesImpl input = ctx.getInput();
        assertTrue( input.getTupleOrFail( 0, 0 ) == tuple );
        assertTrue( ctx.getPartitionKey() == key );
        assertTrue( ctx.getKVStore() == kvStore );
        assertFalse( ctx.nextInput() );
    }

    @Test
    public void testMultipleInputs ()
    {
        final DefaultInvocationContext ctx = new DefaultInvocationContext( 1, kvStoreSupplier, new TuplesImpl( 1 ) );

        final PartitionKey key1 = mock( PartitionKey.class );
        final KVStore kvStore1 = mock( KVStore.class );
        when( kvStoreSupplier.apply( key1 ) ).thenReturn( kvStore1 );
        final PartitionKey key2 = mock( PartitionKey.class );
        final KVStore kvStore2 = mock( KVStore.class );
        when( kvStoreSupplier.apply( key2 ) ).thenReturn( kvStore2 );
        final PartitionKey key3 = mock( PartitionKey.class );
        final KVStore kvStore3 = mock( KVStore.class );
        when( kvStoreSupplier.apply( key3 ) ).thenReturn( kvStore3 );

        final Tuple tuple1 = new Tuple();
        final TuplesImpl tuples1 = ctx.createInputTuples( key1 );
        tuples1.add( tuple1 );
        final Tuple tuple2 = new Tuple();
        final TuplesImpl tuples2 = ctx.createInputTuples( key2 );
        tuples2.add( tuple2 );
        final Tuple tuple3 = new Tuple();
        final TuplesImpl tuples3 = ctx.createInputTuples( key3 );
        tuples3.add( tuple3 );

        assertThat( ctx.getInputCount(), equalTo( 3 ) );
        final TuplesImpl input1 = ctx.getInput();
        assertTrue( input1.getTupleOrFail( 0, 0 ) == tuple1 );
        assertTrue( ctx.getPartitionKey() == key1 );
        assertTrue( ctx.getKVStore() == kvStore1 );
        assertTrue( ctx.nextInput() );
        final TuplesImpl input2 = ctx.getInput();
        assertTrue( input2.getTupleOrFail( 0, 0 ) == tuple2 );
        assertTrue( ctx.getPartitionKey() == key2 );
        assertTrue( ctx.getKVStore() == kvStore2 );
        assertTrue( ctx.nextInput() );
        final TuplesImpl input3 = ctx.getInput();
        assertTrue( input3.getTupleOrFail( 0, 0 ) == tuple3 );
        assertTrue( ctx.getPartitionKey() == key3 );
        assertTrue( ctx.getKVStore() == kvStore3 );
        assertFalse( ctx.nextInput() );
    }

}
