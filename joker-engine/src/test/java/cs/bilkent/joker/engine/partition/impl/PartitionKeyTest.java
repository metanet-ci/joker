package cs.bilkent.joker.engine.partition.impl;

import org.junit.Test;

import cs.bilkent.joker.engine.partition.PartitionKeyExtractor;
import cs.bilkent.joker.operator.Tuple;
import cs.bilkent.joker.partition.impl.PartitionKey;
import cs.bilkent.joker.partition.impl.PartitionKey1;
import cs.bilkent.joker.partition.impl.PartitionKey2;
import cs.bilkent.joker.partition.impl.PartitionKey3;
import cs.bilkent.joker.partition.impl.PartitionKeyN;
import cs.bilkent.joker.test.AbstractJokerTest;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;

public class PartitionKeyTest extends AbstractJokerTest
{

    private final Object val0 = "v0";

    private final Object val1 = "v1";

    private final Object val2 = "v2";

    private final Object val3 = "v3";

    private final Object val4 = "v4";

    private final Tuple tuple = new Tuple();

    @Test
    public void shouldHash1Field ()
    {
        tuple.set( "val0", val0 );

        final PartitionKey key = new PartitionKey1( val0 );
        final PartitionKeyExtractor extractor = new PartitionKeyExtractor1( singletonList( "val0" ) );

        assertEquals( key, new PartitionKey1( val0 ) );
        assertEquals( key, extractor.getPartitionKey( tuple ) );
        assertEquals( key, singletonList( val0 ) );

        final int expectedHash = singletonList( val0 ).hashCode();
        assertEquals( extractor.getPartitionHash( tuple ), expectedHash );
        assertEquals( key.partitionHashCode(), expectedHash );
        assertEquals( key.partitionHashCode(), new PartitionKey2Fwd1( val0, "val1" ).partitionHashCode() );
        assertEquals( key.partitionHashCode(), new PartitionKey3Fwd1( val0, "val1", "val2" ).partitionHashCode() );
        assertEquals( key.partitionHashCode(), new PartitionKeyNFwd1( tuple, singletonList( "val0" ) ).partitionHashCode() );
    }

    @Test
    public void shouldHash2Fields ()
    {
        tuple.set( "val0", val0 ).set( "val1", val1 );

        final PartitionKey key = new PartitionKey2( val0, val1 );
        final PartitionKeyExtractor extractor = new PartitionKeyExtractor2( asList( "val0", "val1" ) );

        assertEquals( key, extractor.getPartitionKey( tuple ) );
        assertEquals( key, asList( val0, val1 ) );

        final int expectedHash = asList( val0, val1 ).hashCode();
        assertEquals( key.hashCode(), expectedHash );
        assertEquals( extractor.getPartitionHash( tuple ), expectedHash );
        assertEquals( key.partitionHashCode(), expectedHash );
        assertEquals( key.partitionHashCode(), new PartitionKey3Fwd2( val0, val1, "val2" ).partitionHashCode() );
        assertEquals( key.partitionHashCode(), new PartitionKeyNFwd2( tuple, asList( "val0", "val1" ) ).partitionHashCode() );
    }

    @Test
    public void shouldHash2FieldsWith1Forward ()
    {
        tuple.set( "val0", val0 ).set( "val1", val1 );

        final PartitionKey key = new PartitionKey2Fwd1( val0, val1 );
        final PartitionKeyExtractor extractor = new PartitionKeyExtractor2Fwd1( asList( "val0", "val1" ) );

        assertEquals( key, new PartitionKey2Fwd1( val0, val1 ) );
        assertEquals( key, extractor.getPartitionKey( tuple ) );
        assertEquals( key, asList( val0, val1 ) );

        assertEquals( key.hashCode(), asList( val0, val1 ).hashCode() );
        final int expectedPartitionHash = singletonList( val0 ).hashCode();
        assertEquals( extractor.getPartitionHash( tuple ), expectedPartitionHash );
        assertEquals( key.partitionHashCode(), expectedPartitionHash );
        assertEquals( key.partitionHashCode(), new PartitionKey1( val0 ).partitionHashCode() );
        assertEquals( key.partitionHashCode(), new PartitionKey3Fwd1( val0, "val1", "val2" ).partitionHashCode() );
        assertEquals( key.partitionHashCode(), new PartitionKeyNFwd1( tuple, singletonList( "val0" ) ).partitionHashCode() );
    }

    @Test
    public void shouldHash3Fields ()
    {
        tuple.set( "val0", val0 ).set( "val1", val1 ).set( "val2", val2 );

        final PartitionKey key = new PartitionKey3( val0, val1, val2 );
        final PartitionKeyExtractor extractor = new PartitionKeyExtractor3( asList( "val0", "val1", "val2" ) );

        assertEquals( key, new PartitionKey3( val0, val1, val2 ) );
        assertEquals( key, extractor.getPartitionKey( tuple ) );
        assertEquals( key, asList( val0, val1, val2 ) );

        final int expectedHash = asList( val0, val1, val2 ).hashCode();
        assertEquals( key.hashCode(), expectedHash );
        assertEquals( extractor.getPartitionHash( tuple ), expectedHash );
        assertEquals( key.partitionHashCode(), expectedHash );
    }

    @Test
    public void shouldHash3FieldsWith1Forward ()
    {
        tuple.set( "val0", val0 ).set( "val1", val1 ).set( "val2", val2 );

        final PartitionKey key = new PartitionKey3Fwd1( val0, val1, val2 );
        final PartitionKeyExtractor extractor = new PartitionKeyExtractor3Fwd1( asList( "val0", "val1", "val2" ) );

        assertEquals( key, new PartitionKey3Fwd1( val0, val1, val2 ) );
        assertEquals( key, extractor.getPartitionKey( tuple ) );
        assertEquals( key, asList( val0, val1, val2 ) );

        assertEquals( key.hashCode(), asList( val0, val1, val2 ).hashCode() );
        final int expectedPartitionHash = singletonList( val0 ).hashCode();
        assertEquals( extractor.getPartitionHash( tuple ), expectedPartitionHash );
        assertEquals( key.partitionHashCode(), expectedPartitionHash );
        assertEquals( key.partitionHashCode(), new PartitionKey1( val0 ).partitionHashCode() );
        assertEquals( key.partitionHashCode(), new PartitionKey2Fwd1( val0, "val1" ).partitionHashCode() );
        assertEquals( key.partitionHashCode(), new PartitionKeyNFwd1( tuple, singletonList( "val0" ) ).partitionHashCode() );
    }

    @Test
    public void shouldHash3FieldsWith2Forwards ()
    {
        tuple.set( "val0", val0 ).set( "val1", val1 ).set( "val2", val2 );

        final PartitionKey key = new PartitionKey3Fwd2( val0, val1, val2 );
        final PartitionKeyExtractor extractor = new PartitionKeyExtractor3Fwd2( asList( "val0", "val1", "val2" ) );

        assertEquals( key, new PartitionKey3Fwd2( val0, val1, val2 ) );
        assertEquals( key, extractor.getPartitionKey( tuple ) );
        assertEquals( key, asList( val0, val1, val2 ) );

        assertEquals( key.hashCode(), asList( val0, val1, val2 ).hashCode() );
        final int expectedPartitionHash = asList( val0, val1 ).hashCode();
        assertEquals( extractor.getPartitionHash( tuple ), expectedPartitionHash );
        assertEquals( key.partitionHashCode(), expectedPartitionHash );
        assertEquals( key.partitionHashCode(), new PartitionKey2( val0, val1 ).partitionHashCode() );
        assertEquals( key.partitionHashCode(), new PartitionKeyNFwd2( tuple, asList( "val0", "val1" ) ).partitionHashCode() );
    }

    @Test
    public void shouldHashN1Field ()
    {
        final Tuple tuple = Tuple.of( "val0", val0 );

        final PartitionKey key = new PartitionKeyN( tuple, singletonList( "val0" ) );
        final PartitionKeyExtractor extractor = new PartitionKeyExtractorN( singletonList( "val0" ) );

        assertEquals( key, new PartitionKeyN( tuple, singletonList( "val0" ) ) );
        assertEquals( key, extractor.getPartitionKey( tuple ) );

        assertEquals( key, singletonList( val0 ) );
        final int expectedPartitionHash = singletonList( val0 ).hashCode();
        assertEquals( key.hashCode(), expectedPartitionHash );
        assertEquals( key.partitionHashCode(), expectedPartitionHash );
        assertEquals( extractor.getPartitionHash( tuple ), expectedPartitionHash );
        assertEquals( key.partitionHashCode(), new PartitionKey1( val0 ).partitionHashCode() );
        assertEquals( key.partitionHashCode(), new PartitionKey2Fwd1( val0, "val1" ).partitionHashCode() );
        assertEquals( key.partitionHashCode(), new PartitionKey3Fwd1( val0, "val1", "val2" ).partitionHashCode() );
        assertEquals( key.partitionHashCode(), new PartitionKeyNFwd1( tuple, singletonList( "val0" ) ).partitionHashCode() );
    }

    @Test
    public void shouldHashN5Fields ()
    {
        final Tuple tuple = Tuple.of( "val0", val0, "val1", val1, "val2", val2, "val3", val3, "val4", val4 );

        final PartitionKey key = new PartitionKeyN( tuple, asList( "val0", "val1", "val2", "val3", "val4" ) );
        final PartitionKeyExtractor extractor = new PartitionKeyExtractorN( asList( "val0", "val1", "val2", "val3", "val4" ) );

        assertEquals( key, new PartitionKeyN( tuple, asList( "val0", "val1", "val2", "val3", "val4" ) ) );
        assertEquals( key, extractor.getPartitionKey( tuple ) );
        assertEquals( key, asList( val0, val1, val2, val3, val4 ) );

        final int expectedHash = asList( val0, val1, val2, val3, val4 ).hashCode();
        assertEquals( key.hashCode(), expectedHash );
        assertEquals( key.partitionHashCode(), expectedHash );
        assertEquals( extractor.getPartitionHash( tuple ), expectedHash );
    }

    @Test
    public void shouldHashN5Fields1Forward ()
    {
        final Tuple tuple = Tuple.of( "val0", val0, "val1", val1, "val2", val2, "val3", val3, "val4", val4 );

        final PartitionKey key = new PartitionKeyNFwd1( tuple, asList( "val0", "val1", "val2", "val3", "val4" ) );
        final PartitionKeyExtractor extractor = new PartitionKeyExtractorNFwd1( asList( "val0", "val1", "val2", "val3", "val4" ) );

        assertEquals( key, new PartitionKeyNFwdM( tuple, asList( "val0", "val1", "val2", "val3", "val4" ), 1 ) );
        assertEquals( key, extractor.getPartitionKey( tuple ) );
        assertEquals( key, asList( val0, val1, val2, val3, val4 ) );

        assertEquals( key.hashCode(), asList( val0, val1, val2, val3, val4 ).hashCode() );
        final int expectedPartitionHash = singletonList( val0 ).hashCode();
        assertEquals( key.partitionHashCode(), expectedPartitionHash );
        assertEquals( extractor.getPartitionHash( tuple ), expectedPartitionHash );
        assertEquals( key.partitionHashCode(), new PartitionKey1( val0 ).partitionHashCode() );
        assertEquals( key.partitionHashCode(), new PartitionKey2Fwd1( val0, "val1" ).partitionHashCode() );
        assertEquals( key.partitionHashCode(), new PartitionKey3Fwd1( val0, "val1", "val2" ).partitionHashCode() );
        assertEquals( key.partitionHashCode(), new PartitionKeyNFwd1( tuple, singletonList( "val0" ) ).partitionHashCode() );
    }

    @Test
    public void shouldHashN5Fields2Forward ()
    {
        final Tuple tuple = Tuple.of( "val0", val0, "val1", val1, "val2", val2, "val3", val3, "val4", val4 );

        final PartitionKey key = new PartitionKeyNFwd2( tuple, asList( "val0", "val1", "val2", "val3", "val4" ) );
        final PartitionKeyExtractor extractor = new PartitionKeyExtractorNFwd2( asList( "val0", "val1", "val2", "val3", "val4" ) );

        assertEquals( key, new PartitionKeyNFwd2( tuple, asList( "val0", "val1", "val2", "val3", "val4" ) ) );
        assertEquals( key, extractor.getPartitionKey( tuple ) );
        assertEquals( key, asList( val0, val1, val2, val3, val4 ) );

        assertEquals( key.hashCode(), asList( val0, val1, val2, val3, val4 ).hashCode() );
        final int expectedPartitionHash = asList( val0, val1 ).hashCode();
        assertEquals( key.partitionHashCode(), expectedPartitionHash );
        assertEquals( extractor.getPartitionHash( tuple ), expectedPartitionHash );
        assertEquals( key.partitionHashCode(), new PartitionKey2( val0, val1 ).partitionHashCode() );
        assertEquals( key.partitionHashCode(), new PartitionKey3Fwd2( val0, val1, val2 ).partitionHashCode() );
        assertEquals( key.partitionHashCode(), new PartitionKeyNFwd2( tuple, asList( "val0", "val1" ) ).partitionHashCode() );
    }

    @Test
    public void shouldHashN5Fields3Forward ()
    {
        final Tuple tuple = Tuple.of( "val0", val0, "val1", val1, "val2", val2, "val3", val3, "val4", val4 );

        final PartitionKey key = new PartitionKeyNFwd3( tuple, asList( "val0", "val1", "val2", "val3", "val4" ) );
        final PartitionKeyExtractor extractor = new PartitionKeyExtractorNFwd3( asList( "val0", "val1", "val2", "val3", "val4" ) );

        assertEquals( key, new PartitionKeyNFwdM( tuple, asList( "val0", "val1", "val2", "val3", "val4" ), 2 ) );
        assertEquals( key, extractor.getPartitionKey( tuple ) );
        assertEquals( key, asList( val0, val1, val2, val3, val4 ) );

        assertEquals( key.hashCode(), asList( val0, val1, val2, val3, val4 ).hashCode() );
        final int expectedPartitionHash = asList( val0, val1, val2 ).hashCode();
        assertEquals( extractor.getPartitionHash( tuple ), expectedPartitionHash );
        assertEquals( key.partitionHashCode(), expectedPartitionHash );
        assertEquals( key.partitionHashCode(), new PartitionKey3( val0, val1, val2 ).partitionHashCode() );
    }

    @Test
    public void shouldHashN5FieldsM4Forward ()
    {
        final Tuple tuple = Tuple.of( "val0", val0, "val1", val1, "val2", val2, "val3", val3, "val4", val4 );

        final PartitionKey key = new PartitionKeyNFwdM( tuple, asList( "val0", "val1", "val2", "val3", "val4" ), 4 );
        final PartitionKeyExtractor extractor = new PartitionKeyExtractorNFwdM( asList( "val0", "val1", "val2", "val3", "val4" ), 4 );

        assertEquals( key, new PartitionKeyNFwdM( tuple, asList( "val0", "val1", "val2", "val3", "val4" ), 4 ) );
        assertEquals( key, extractor.getPartitionKey( tuple ) );
        assertEquals( key, asList( val0, val1, val2, val3, val4 ) );

        assertEquals( key.hashCode(), asList( val0, val1, val2, val3, val4 ).hashCode() );
        final int expectedPartitionHash = asList( val0, val1, val2, val3 ).hashCode();
        assertEquals( key.partitionHashCode(), expectedPartitionHash );
        assertEquals( extractor.getPartitionHash( tuple ), expectedPartitionHash );
    }

}
