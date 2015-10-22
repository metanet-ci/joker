package cs.bilkent.zanza.operator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class TupleTest
{
    private Tuple tuple = new Tuple();

    @Test
    public void shouldSet ()
    {
        tuple.set( "field", "value" );
        assertThat( tuple.getObject( "field" ), equalTo( "value" ) );
    }

    @Test
    public void shouldPut ()
    {
        tuple.set( "field", "value" );
        assertThat( tuple.put( "field", "value2" ), equalTo( "value" ) );
    }

    @Test
    public void shouldRemove ()
    {
        tuple.set( "field", "value" );
        assertNotNull( tuple.remove( "field" ) );
        assertNull( tuple.getObject( "field" ) );
    }

    @Test
    public void shouldNotRemoveNonExistingField ()
    {
        assertNull( tuple.remove( "field" ) );
    }

    @Test
    public void shouldDelete ()
    {
        tuple.set( "field", "value" );
        assertTrue( tuple.delete( "field" ) );
        assertNull( tuple.getObject( "field" ) );
    }

    @Test
    public void shouldNotDeleteNonExistingField ()
    {
        assertFalse( tuple.delete( "field" ) );
    }

    @Test
    public void shouldGetExisting ()
    {
        tuple.set( "field", "value" );
        assertThat( tuple.<String>get( "field" ), equalTo( "value" ) );
    }

    @Test
    public void shouldGetExistingOrDefault ()
    {
        tuple.set( "field", "value" );
        assertThat( tuple.getOrDefault( "field", "value2" ), equalTo( "value" ) );
    }

    @Test
    public void shouldGetNonExistingOrDefault ()
    {
        assertThat( tuple.getOrDefault( "field", "value" ), equalTo( "value" ) );
    }

    @Test
    public void shouldContainExistingField ()
    {
        tuple.put( "field", "value" );
        assertTrue( tuple.contains( "field" ) );
    }

    @Test
    public void shouldNotContainNonExistingField ()
    {
        assertFalse( tuple.contains( "field" ) );
    }

    @Test
    public void shouldGetExistingObject ()
    {
        tuple.put( "field", "value" );
        assertThat( tuple.getObject( "field" ), equalTo( "value" ) );
    }

    @Test
    public void shouldNotGetNonExistingObject ()
    {
        assertNull( tuple.getObject( "field" ) );
    }

    @Test
    public void shouldGetExistingObjectOrDefault ()
    {
        tuple.put( "field", "value" );
        assertThat( tuple.getObjectOrDefault( "field", "value2" ), equalTo( "value" ) );
    }

    @Test
    public void shouldGetNonExistingObjectOrDefault ()
    {
        assertThat( tuple.getObjectOrDefault( "field", "value2" ), equalTo( "value2" ) );
    }

    @Test
    public void shouldGetExistingString ()
    {
        tuple.put( "field", "value" );
        assertThat( tuple.getString( "field" ), equalTo( "value" ) );
    }

    @Test
    public void shouldNotGetExistingString ()
    {
        assertNull( tuple.getString( "field" ) );
    }

    @Test
    public void shouldGetExistingStringOrDefault ()
    {
        tuple.put( "field", "value" );
        assertThat( tuple.getStringOrDefault( "field", "value2" ), equalTo( "value" ) );
    }

    @Test
    public void shouldGetNonExistingStringOrDefault ()
    {
        assertThat( tuple.getStringOrDefault( "field", "value2" ), equalTo( "value2" ) );
    }

    @Test
    public void shouldGetExistingInteger ()
    {
        tuple.put( "field", 1 );
        assertThat( tuple.getInteger( "field" ), equalTo( 1 ) );
    }

    @Test
    public void shouldNotGetNotExistingInteger ()
    {
        assertNull( tuple.getInteger( "field" ) );
    }

    @Test
    public void shouldGetExistingIntegerOrDefault ()
    {
        tuple.put( "field", 1 );
        assertThat( tuple.getIntegerOrDefault( "field", 2 ), equalTo( 1 ) );
    }

    @Test
    public void shouldGetNonExistingIntegerOrDefault ()
    {
        assertThat( tuple.getIntegerOrDefault( "field", 2 ), equalTo( 2 ) );
    }

    @Test
    public void shouldGetExistingLong ()
    {
        tuple.put( "field", 1L );
        assertThat( tuple.getLong( "field" ), equalTo( 1L ) );
    }

    @Test
    public void shouldNotGetExistingLong ()
    {
        assertNull( tuple.getLong( "field" ) );
    }

    @Test
    public void shouldGetExistingLongOrDefault ()
    {
        tuple.put( "field", 1L );
        assertThat( tuple.getLongOrDefault( "field", 2L ), equalTo( 1L ) );
    }

    @Test
    public void shouldGetNonExistingLongOrDefault ()
    {
        assertThat( tuple.getLongOrDefault( "field", 2L ), equalTo( 2L ) );
    }

    @Test
    public void shouldGetExistingBoolean ()
    {
        tuple.put( "field", Boolean.TRUE );
        assertTrue( tuple.getBoolean( "field" ) );
    }

    @Test
    public void shouldNotGetNonExistingBoolean ()
    {
        assertNull( tuple.getBoolean( "field" ) );
    }

    @Test
    public void shouldGetExistingBooleanOrDefault ()
    {
        tuple.put( "field", Boolean.TRUE );
        assertTrue( tuple.getBooleanOrDefault( "field", Boolean.FALSE ) );
    }

    @Test
    public void shouldGetNonExistingBooleanOrDefault ()
    {
        assertTrue( tuple.getBooleanOrDefault( "field", Boolean.TRUE ) );
    }

    @Test
    public void shouldGetGetExistingShort ()
    {
        final short val = 1;
        tuple.put( "field", val );
        assertThat( tuple.getShort( "field" ), equalTo( val ) );
    }

    @Test
    public void shouldNotGetNonExistingShort ()
    {
        assertNull( tuple.getShort( "field" ) );
    }

    @Test
    public void shouldGetExistingShortOrDefault ()
    {
        final short val = 1;
        final short other = 2;
        tuple.put( "field", val );
        assertThat( tuple.getShortOrDefault( "field", other ), equalTo( val ) );
    }

    @Test
    public void shouldGetNonExistingShortOrDefault ()
    {
        final short val = 1;
        assertThat( tuple.getShortOrDefault( "field", val ), equalTo( val ) );
    }

    @Test
    public void shouldGetExistingByte ()
    {
        final byte val = 1;
        tuple.put( "field", val );
        assertThat( tuple.getByte( "field" ), equalTo( val ) );
    }

    @Test
    public void shouldNotGetNonExistingByte ()
    {
        assertNull( tuple.getByte( "field" ) );
    }

    @Test
    public void shouldGetExistingByteOrDefault ()
    {
        final byte val = 1;
        final byte other = 1;
        tuple.put( "field", val );
        assertThat( tuple.getByteOrDefault( "field", other ), equalTo( val ) );
    }

    @Test
    public void shouldGetNonExistingByteOrDefault ()
    {
        final byte val = 1;
        assertThat( tuple.getByteOrDefault( "field", val ), equalTo( val ) );
    }

    @Test
    public void shouldGetExistingDouble ()
    {
        tuple.put( "field", 1d );
        assertThat( tuple.getDouble( "field" ), equalTo( 1d ) );
    }

    @Test
    public void shouldNotGetNonExistingDouble ()
    {
        assertNull( tuple.getDouble( "field" ) );
    }

    @Test
    public void shouldGetExistingDoubleOrDefault ()
    {
        tuple.put( "field", 1d );
        assertThat( tuple.getDoubleOrDefault( "field", 2d ), equalTo( 1d ) );
    }

    @Test
    public void shouldGetNonExistingDoubleOrDefault ()
    {
        assertThat( tuple.getDoubleOrDefault( "field", 1d ), equalTo( 1d ) );
    }

    @Test
    public void shouldGetExistingFloat ()
    {
        tuple.put( "field", 1f );
        assertThat( tuple.getFloat( "field" ), equalTo( 1f ) );
    }

    @Test
    public void shouldNotGetNonExistingFloat ()
    {
        assertNull( tuple.getFloat( "field" ) );
    }

    @Test
    public void shouldGetExistingFloatOrDefault ()
    {
        tuple.put( "field", 1f );
        assertThat( tuple.getFloatOrDefault( "field", 2f ), equalTo( 1f ) );
    }

    @Test
    public void shouldGetNonExistingFloatOrDefault ()
    {
        assertThat( tuple.getFloatOrDefault( "field", 1f ), equalTo( 1f ) );
    }

    @Test
    public void shouldGetExistingBinary ()
    {
        final byte[] val = new byte[] { 1 };
        tuple.put( "field", val );
        assertThat( tuple.getBinary( "field" ), equalTo( val ) );
    }

    @Test
    public void shouldGetExistingBinaryOrDefault ()
    {
        final byte[] val = new byte[] { 1 };
        tuple.put( "field", val );
        assertThat( tuple.getBinaryOrDefault( "field", new byte[] {} ), equalTo( val ) );
    }

    @Test
    public void shouldGetNonExistingBinaryOrDefault ()
    {
        final byte[] val = new byte[] { 1 };
        assertThat( tuple.getBinaryOrDefault( "field", val ), equalTo( val ) );
    }

    @SuppressWarnings( "serial" )
    private static List<Integer> makeArrayList ()
    {
        return new ArrayList<Integer>()
        {
            {
                add( 1 );
            }
        };
    }

    @SuppressWarnings( "serial" )
    private static Set<Integer> makeHashSet ()
    {
        return new HashSet<Integer>()
        {
            {
                add( 1 );
            }
        };
    }

    @SuppressWarnings( "serial" )
    private static Map<Integer, Integer> makeHashMap ()
    {
        return new HashMap<Integer, Integer>()
        {
            {
                put( 1, 1 );
            }
        };
    }

    @Test
    public void shouldGetExistingCollection ()
    {
        final Collection<Integer> value = makeArrayList();
        tuple.put( "field", value );
        assertThat( tuple.getCollection( "field" ), equalTo( value ) );
    }

    @Test
    public void shouldNotGetNonExistingCollection ()
    {
        assertNull( tuple.getCollection( "field" ) );
    }

    @Test
    public void shouldGetExistingCollectionOrDefault ()
    {
        final Collection<Integer> value = makeArrayList();
        tuple.put( "field", value );
        assertThat( tuple.getCollectionOrDefault( "field", Collections.emptyList() ), equalTo( value ) );
    }

    @Test
    public void shouldGetNonExistingCollectionOrDefault ()
    {
        final Collection<Integer> value = makeArrayList();
        assertThat( tuple.getCollectionOrDefault( "field", value ), equalTo( value ) );
    }

    @Test
    public void shouldGetExistingCollectionOrEmpty ()
    {
        final Collection<Integer> value = makeArrayList();
        tuple.put( "field", value );
        assertThat( tuple.getCollectionOrEmpty( "field" ), equalTo( value ) );
    }

    @Test
    public void shouldGetNonExistingCollectionOrEmpty ()
    {
        assertThat( tuple.getCollectionOrEmpty( "field" ), empty() );
    }

    @Test
    public void shouldGetExistingList ()
    {
        final List<Integer> value = makeArrayList();
        tuple.put( "field", value );
        assertThat( tuple.getList( "field" ), equalTo( value ) );
    }

    @Test
    public void shouldNotGetNonExistingList ()
    {
        assertNull( tuple.getList( "field" ) );
    }

    @Test
    public void shouldGetExistingListOrDefault ()
    {
        final List<Integer> value = makeArrayList();
        tuple.put( "field", value );
        assertThat( tuple.getListOrDefault( "field", Collections.emptyList() ), equalTo( value ) );
    }

    @Test
    public void shouldGetNonExistingListOrDefault ()
    {
        final List<Integer> value = makeArrayList();
        assertThat( tuple.getListOrDefault( "field", value ), equalTo( value ) );
    }

    @Test
    public void shouldGetExistingListOrEmpty ()
    {
        final List<Integer> value = makeArrayList();
        tuple.put( "field", value );
        assertThat( tuple.getListOrEmpty( "field" ), equalTo( value ) );
    }

    @Test
    public void shouldGetNonExistingListOrEmpty ()
    {
        assertThat( tuple.getListOrEmpty( "field" ), empty() );
    }

    @Test
    public void shouldGetExistingSet ()
    {
        final Set<Integer> value = makeHashSet();
        tuple.put( "field", value );
        assertThat( tuple.getSet( "field" ), equalTo( value ) );
    }

    @Test
    public void shouldNotGetNonExistingSet ()
    {
        assertNull( tuple.getSet( "field" ) );
    }

    @Test
    public void shouldGetExistingSetOrDefault ()
    {
        final Set<Integer> value = makeHashSet();
        tuple.put( "field", value );
        assertThat( tuple.getSetOrDefault( "field", Collections.emptySet() ), equalTo( value ) );
    }

    @Test
    public void shouldGetNonExistingSetOrDefault ()
    {
        final Set<Integer> value = makeHashSet();
        assertThat( tuple.getSetOrDefault( "field", value ), equalTo( value ) );
    }

    @Test
    public void shouldGetExistingSetOrEmpty ()
    {
        final Set<Integer> value = makeHashSet();
        tuple.put( "field", value );
        assertThat( tuple.getSetOrEmpty( "field" ), equalTo( value ) );
    }

    @Test
    public void shouldGetNonExistingSetOrEmpty ()
    {
        assertThat( tuple.getSetOrEmpty( "field" ), empty() );
    }

    @Test
    public void shouldGetExistingMap ()
    {
        final Map<Integer, Integer> value = makeHashMap();
        tuple.put( "field", value );
        assertThat( tuple.getMap( "field" ), equalTo( value ) );
    }

    @Test
    public void shouldNotGetNonExistingMap ()
    {
        assertNull( tuple.getMap( "field" ) );
    }

    @Test
    public void shouldGetExistingMapOrDefault ()
    {
        final Map<Integer, Integer> value = makeHashMap();
        tuple.put( "field", value );
        assertThat( tuple.getMapOrDefault( "field", Collections.emptyMap() ), equalTo( value ) );
    }

    @Test
    public void shouldGetNonExistingMapOrDefault ()
    {
        final Map<Integer, Integer> value = makeHashMap();
        assertThat( tuple.getMapOrDefault( "field", value ), equalTo( value ) );
    }

    @Test
    public void shouldGetExistingMapOrEmpty ()
    {
        final Map<Integer, Integer> value = makeHashMap();
        tuple.put( "field", value );
        assertThat( tuple.getMapOrEmpty( "field" ), equalTo( value ) );
    }

    @Test
    public void shouldGetNonExistingMapOrEmpty ()
    {
        assertThat( tuple.getMapOrEmpty( "field" ).size(), equalTo( 0 ) );
    }
    
	@Test
	public void shouldClear()
	{
		tuple.set("field", "value");
		tuple.clear();
		assertThat(tuple.size(), equalTo(0));
	}

	@Test
	public void shouldGetSize()
	{
		tuple.set("field", "value");
		assertThat(tuple.size(), equalTo(1));
	}

    @Test
    public void testTupleEquality1 ()
    {
        final Tuple tuple1 = new Tuple( true );
        final Tuple tuple2 = new Tuple( true );

        tuple1.set( "k1", "v1" );
        tuple1.set( "k2", "v2" );

        tuple2.set( "k1", "v1" );
        tuple2.set( "k2", "v2" );

        assertThat( tuple1, equalTo( tuple2 ) );
        assertThat( tuple1.hashCode(), equalTo( tuple2.hashCode() ) );
    }

    @Test
    public void testTupleEquality2 ()
    {
        final Tuple tuple1 = new Tuple( true );
        final Tuple tuple2 = new Tuple( true );

        tuple1.set( "k1", "v1" );
        tuple1.set( "k2", "v2" );

        tuple2.set( "k2", "v2" );
        tuple2.set( "k1", "v1" );

        assertThat( tuple1, equalTo( tuple2 ) );
        assertThat( tuple1.hashCode(), equalTo( tuple2.hashCode() ) );
    }

    @Test
    public void testTupleEquality3 ()
    {
        final Tuple tuple1 = new Tuple( false );
        final Tuple tuple2 = new Tuple( false );

        tuple1.set( "k1", "v1" );
        tuple1.set( "k2", "v2" );

        tuple2.set( "k1", "v1" );
        tuple2.set( "k2", "v2" );

        assertThat( tuple1, equalTo( tuple2 ) );
        assertThat( tuple1.hashCode(), equalTo( tuple2.hashCode() ) );
    }

    @Test
    public void testTupleEquality4 ()
    {
        final Tuple tuple1 = new Tuple( false );
        final Tuple tuple2 = new Tuple( false );

        tuple1.set( "k1", "v1" );
        tuple1.set( "k2", "v2" );

        tuple2.set( "k2", "v2" );
        tuple2.set( "k1", "v1" );

        assertThat( tuple1, equalTo( tuple2 ) );
        assertThat( tuple1.hashCode(), equalTo( tuple2.hashCode() ) );
    }

    @Test
    public void testTupleEquality5 ()
    {
        final Tuple tuple1 = new Tuple( true );
        final Tuple tuple2 = new Tuple( false );

        tuple1.set( "k1", "v1" );
        tuple1.set( "k2", "v2" );

        tuple2.set( "k1", "v1" );
        tuple2.set( "k2", "v2" );

        assertThat( tuple1, equalTo( tuple2 ) );
        assertThat( tuple1.hashCode(), equalTo( tuple2.hashCode() ) );
    }

    @Test
    public void testTupleEquality6 ()
    {
        final Tuple tuple1 = new Tuple( true );
        final Tuple tuple2 = new Tuple( false );

        tuple1.set( "k1", "v1" );
        tuple1.set( "k2", "v2" );

        tuple2.set( "k2", "v2" );
        tuple2.set( "k1", "v1" );

        assertThat( tuple1, equalTo( tuple2 ) );
        assertThat( tuple1.hashCode(), equalTo( tuple2.hashCode() ) );
    }

    @Test
    public void testTupleEquality7 ()
    {
        final Tuple tuple1 = new Tuple( false );
        final Tuple tuple2 = new Tuple( true );

        tuple1.set( "k1", "v1" );
        tuple1.set( "k2", "v2" );

        tuple2.set( "k1", "v1" );
        tuple2.set( "k2", "v2" );

        assertThat( tuple1, equalTo( tuple2 ) );
        assertThat( tuple1.hashCode(), equalTo( tuple2.hashCode() ) );
    }

    @Test
    public void testTupleEquality8 ()
    {
        final Tuple tuple1 = new Tuple( false );
        final Tuple tuple2 = new Tuple( true );

        tuple1.set( "k1", "v1" );
        tuple1.set( "k2", "v2" );

        tuple2.set( "k2", "v2" );
        tuple2.set( "k1", "v1" );

        assertThat( tuple1, equalTo( tuple2 ) );
        assertThat( tuple1.hashCode(), equalTo( tuple2.hashCode() ) );
    }
}
