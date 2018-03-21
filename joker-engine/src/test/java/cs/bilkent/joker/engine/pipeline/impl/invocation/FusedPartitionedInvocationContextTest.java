package cs.bilkent.joker.engine.pipeline.impl.invocation;

import java.util.function.Function;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import cs.bilkent.joker.engine.partition.PartitionKeyExtractor;
import cs.bilkent.joker.operator.Tuple;
import cs.bilkent.joker.operator.impl.OutputCollector;
import cs.bilkent.joker.operator.kvstore.KVStore;
import cs.bilkent.joker.partition.impl.PartitionKey;
import cs.bilkent.joker.partition.impl.PartitionKey1;
import cs.bilkent.joker.test.AbstractJokerTest;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith( MockitoJUnitRunner.class )
public class FusedPartitionedInvocationContextTest extends AbstractJokerTest
{

    @Mock
    private Function<PartitionKey, KVStore> kvStoreSupplier;

    @Mock
    private PartitionKeyExtractor partitionKeyExtractor;

    @Mock
    private OutputCollector outputCollector;

    private FusedPartitionedInvocationContext invocationContext;

    @Before
    public void init ()
    {
        invocationContext = new FusedPartitionedInvocationContext( 1, kvStoreSupplier, partitionKeyExtractor, outputCollector );
    }

    @Test
    public void when_noOutputIsAdded_then_noInputIsPresent ()
    {
        assertThat( invocationContext.getInputCount(), equalTo( 0 ) );
    }

    @Test
    public void when_singleKeyIsAdded_then_singleInputIsPresent ()
    {
        final Tuple tuple = new Tuple();
        final PartitionKey key = new PartitionKey1( "val" );
        when( partitionKeyExtractor.getPartitionKey( tuple ) ).thenReturn( key );

        invocationContext.add( tuple );

        assertThat( invocationContext.getInputCount(), equalTo( 1 ) );
    }

    @Test
    public void when_singleKeyIsAddedMultipleTimes_then_singleInputIsPresent ()
    {
        final Tuple tuple1 = new Tuple();
        final Tuple tuple2 = new Tuple();
        final PartitionKey key = new PartitionKey1( "val" );
        when( partitionKeyExtractor.getPartitionKey( tuple1 ) ).thenReturn( key );
        when( partitionKeyExtractor.getPartitionKey( tuple2 ) ).thenReturn( key );

        invocationContext.add( tuple1 );
        invocationContext.add( tuple2 );

        assertThat( invocationContext.getInputCount(), equalTo( 1 ) );
    }

    @Test
    public void when_singleMultipleTuplesAreAdded_then_multipleInputsArePresent ()
    {
        final Tuple tuple1 = new Tuple();
        tuple1.set( "field", "val1" );
        final Tuple tuple2 = new Tuple();
        tuple2.set( "field", "val2" );
        final PartitionKey key1 = new PartitionKey1( "val1" );
        final PartitionKey key2 = new PartitionKey1( "val2" );
        when( partitionKeyExtractor.getPartitionKey( tuple1 ) ).thenReturn( key1 );
        when( partitionKeyExtractor.getPartitionKey( tuple2 ) ).thenReturn( key2 );

        invocationContext.add( tuple1 );
        invocationContext.add( tuple2 );
        invocationContext.add( tuple1 );

        assertThat( invocationContext.getInputCount(), equalTo( 2 ) );
    }

    @Test
    public void when_tuplesAreAdded_then_theyAreReturnedAsInput ()
    {
        final Tuple tuple1 = new Tuple();
        final Tuple tuple2 = new Tuple();
        final PartitionKey key = new PartitionKey1( "val" );
        when( partitionKeyExtractor.getPartitionKey( tuple1 ) ).thenReturn( key );
        when( partitionKeyExtractor.getPartitionKey( tuple2 ) ).thenReturn( key );

        invocationContext.add( tuple1 );
        invocationContext.add( tuple2 );

        assertThat( invocationContext.getInputTuples( 0 ), equalTo( asList( tuple1, tuple2 ) ) );
        assertThat( invocationContext.getPartitionKey(), equalTo( key ) );
        assertFalse( invocationContext.nextInput() );
    }

    @Test
    public void when_tuplesAreAddedForMultipleKeys_then_theyAreReturnedAsInput ()
    {
        final Tuple tuple1 = new Tuple();
        tuple1.set( "field", "val1" );
        final Tuple tuple2 = new Tuple();
        tuple2.set( "field", "val2" );
        final PartitionKey key1 = new PartitionKey1( "val1" );
        final PartitionKey key2 = new PartitionKey1( "val2" );
        when( partitionKeyExtractor.getPartitionKey( tuple1 ) ).thenReturn( key1 );
        when( partitionKeyExtractor.getPartitionKey( tuple2 ) ).thenReturn( key2 );
        final KVStore kvStore1 = mock( KVStore.class );
        final KVStore kvStore2 = mock( KVStore.class );
        when( kvStoreSupplier.apply( key1 ) ).thenReturn( kvStore1 );
        when( kvStoreSupplier.apply( key2 ) ).thenReturn( kvStore2 );

        invocationContext.add( tuple1 );
        invocationContext.add( tuple2 );
        invocationContext.add( tuple1 );

        assertThat( invocationContext.getInputTuples( 0 ), equalTo( asList( tuple1, tuple1 ) ) );
        assertThat( invocationContext.getPartitionKey(), equalTo( key1 ) );
        assertThat( invocationContext.getKVStore(), equalTo( kvStore1 ) );
        assertTrue( invocationContext.nextInput() );

        assertThat( invocationContext.getInputTuples( 0 ), equalTo( singletonList( tuple2 ) ) );
        assertThat( invocationContext.getPartitionKey(), equalTo( key2 ) );
        assertThat( invocationContext.getKVStore(), equalTo( kvStore2 ) );
        assertFalse( invocationContext.nextInput() );
    }

    @Test
    public void when_invocationContextIsReset_then_tuplesAreAddedCorrectlyAfterwards ()
    {
        final Tuple tuple1 = new Tuple();
        tuple1.set( "field", "val1" );
        final Tuple tuple2 = new Tuple();
        tuple2.set( "field", "val2" );
        final PartitionKey key1 = new PartitionKey1( "val1" );
        final PartitionKey key2 = new PartitionKey1( "val2" );
        when( partitionKeyExtractor.getPartitionKey( tuple1 ) ).thenReturn( key1 );
        when( partitionKeyExtractor.getPartitionKey( tuple2 ) ).thenReturn( key2 );
        final KVStore kvStore1 = mock( KVStore.class );
        final KVStore kvStore2 = mock( KVStore.class );
        when( kvStoreSupplier.apply( key1 ) ).thenReturn( kvStore1 );
        when( kvStoreSupplier.apply( key2 ) ).thenReturn( kvStore2 );

        invocationContext.add( tuple1 );
        invocationContext.add( tuple2 );
        invocationContext.add( tuple1 );

        invocationContext.reset();

        invocationContext.add( tuple1 );
        invocationContext.add( tuple2 );
        invocationContext.add( tuple1 );

        assertThat( invocationContext.getInputCount(), equalTo( 2 ) );
    }

}
