package cs.bilkent.joker.operator.kvstore.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

import cs.bilkent.joker.operator.impl.InMemoryKVStore;
import cs.bilkent.joker.operator.kvstore.KVStore;
import cs.bilkent.joker.test.AbstractJokerTest;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;


public class InMemoryKvStoreTest extends AbstractJokerTest
{

    private final KVStore kvStore = new InMemoryKVStore();

    @Test
    public void shouldSet ()
    {
        kvStore.set( "field", "value" );
        assertThat( kvStore.getObject( "field" ), equalTo( "value" ) );
    }

    @Test
    public void shouldRemove ()
    {
        kvStore.set( "field", "value" );
        assertNotNull( kvStore.remove( "field" ) );
        assertNull( kvStore.getObject( "field" ) );
    }

    @Test
    public void shouldNotRemoveNonExistingField ()
    {
        assertNull( kvStore.remove( "field" ) );
    }

    @Test
    public void shouldDelete ()
    {
        kvStore.set( "field", "value" );
        assertTrue( kvStore.delete( "field" ) );
        assertNull( kvStore.getObject( "field" ) );
    }

    @Test
    public void shouldNotDeleteNonExistingField ()
    {
        assertFalse( kvStore.delete( "field" ) );
    }

    @Test
    public void shouldGetExisting ()
    {
        kvStore.set( "field", "value" );
        assertThat( kvStore.<String>get( "field" ), equalTo( "value" ) );
    }

    @Test
    public void shouldGetExistingOrDefault ()
    {
        kvStore.set( "field", "value" );
        assertThat( kvStore.getOrDefault( "field", "value2" ), equalTo( "value" ) );
    }

    @Test
    public void shouldGetNonExistingOrDefault ()
    {
        assertThat( kvStore.getOrDefault( "field", "value" ), equalTo( "value" ) );
    }

    @Test
    public void shouldContainExistingField ()
    {
        kvStore.set( "field", "value" );
        assertTrue( kvStore.contains( "field" ) );
    }

    @Test
    public void shouldNotContainNonExistingField ()
    {
        assertFalse( kvStore.contains( "field" ) );
    }

    @Test
    public void shouldGetExistingObject ()
    {
        kvStore.set( "field", "value" );
        assertThat( kvStore.getObject( "field" ), equalTo( "value" ) );
    }

    @Test
    public void shouldNotGetNonExistingObject ()
    {
        assertNull( kvStore.getObject( "field" ) );
    }

    @Test
    public void shouldGetExistingObjectOrDefault ()
    {
        kvStore.set( "field", "value" );
        assertThat( kvStore.getObjectOrDefault( "field", "value2" ), equalTo( "value" ) );
    }

    @Test
    public void shouldGetNonExistingObjectOrDefault ()
    {
        assertThat( kvStore.getObjectOrDefault( "field", "value2" ), equalTo( "value2" ) );
    }

    @Test
    public void shouldGetExistingString ()
    {
        kvStore.set( "field", "value" );
        assertThat( kvStore.getString( "field" ), equalTo( "value" ) );
    }

    @Test
    public void shouldNotGetExistingString ()
    {
        assertNull( kvStore.getString( "field" ) );
    }

    @Test
    public void shouldGetExistingStringOrDefault ()
    {
        kvStore.set( "field", "value" );
        assertThat( kvStore.getStringOrDefault( "field", "value2" ), equalTo( "value" ) );
    }

    @Test
    public void shouldGetNonExistingStringOrDefault ()
    {
        assertThat( kvStore.getStringOrDefault( "field", "value2" ), equalTo( "value2" ) );
    }

    @Test
    public void shouldGetExistingInteger ()
    {
        kvStore.set( "field", 1 );
        assertThat( kvStore.getInteger( "field" ), equalTo( 1 ) );
    }

    @Test
    public void shouldNotGetNotExistingInteger ()
    {
        assertNull( kvStore.getInteger( "field" ) );
    }

    @Test
    public void shouldGetExistingIntegerOrDefault ()
    {
        kvStore.set( "field", 1 );
        assertThat( kvStore.getIntegerOrDefault( "field", 2 ), equalTo( 1 ) );
    }

    @Test
    public void shouldGetNonExistingIntegerOrDefault ()
    {
        assertThat( kvStore.getIntegerOrDefault( "field", 2 ), equalTo( 2 ) );
    }

    @Test
    public void shouldGetExistingLong ()
    {
        kvStore.set( "field", 1L );
        assertThat( kvStore.getLong( "field" ), equalTo( 1L ) );
    }

    @Test
    public void shouldNotGetExistingLong ()
    {
        assertNull( kvStore.getLong( "field" ) );
    }

    @Test
    public void shouldGetExistingLongOrDefault ()
    {
        kvStore.set( "field", 1L );
        assertThat( kvStore.getLongOrDefault( "field", 2L ), equalTo( 1L ) );
    }

    @Test
    public void shouldGetNonExistingLongOrDefault ()
    {
        assertThat( kvStore.getLongOrDefault( "field", 2L ), equalTo( 2L ) );
    }

    @Test
    public void shouldGetExistingBoolean ()
    {
        kvStore.set( "field", Boolean.TRUE );
        assertTrue( kvStore.getBoolean( "field" ) );
    }

    @Test
    public void shouldNotGetNonExistingBoolean ()
    {
        assertNull( kvStore.getBoolean( "field" ) );
    }

    @Test
    public void shouldGetExistingBooleanOrDefault ()
    {
        kvStore.set( "field", Boolean.TRUE );
        assertTrue( kvStore.getBooleanOrDefault( "field", Boolean.FALSE ) );
    }

    @Test
    public void shouldGetNonExistingBooleanOrDefault ()
    {
        assertTrue( kvStore.getBooleanOrDefault( "field", Boolean.TRUE ) );
    }

    @Test
    public void shouldGetGetExistingShort ()
    {
        final short val = 1;
        kvStore.set( "field", val );
        assertThat( kvStore.getShort( "field" ), equalTo( val ) );
    }

    @Test
    public void shouldNotGetNonExistingShort ()
    {
        assertNull( kvStore.getShort( "field" ) );
    }

    @Test
    public void shouldGetExistingShortOrDefault ()
    {
        final short val = 1;
        final short other = 2;
        kvStore.set( "field", val );
        assertThat( kvStore.getShortOrDefault( "field", other ), equalTo( val ) );
    }

    @Test
    public void shouldGetNonExistingShortOrDefault ()
    {
        final short val = 1;
        assertThat( kvStore.getShortOrDefault( "field", val ), equalTo( val ) );
    }

    @Test
    public void shouldGetExistingByte ()
    {
        final byte val = 1;
        kvStore.set( "field", val );
        assertThat( kvStore.getByte( "field" ), equalTo( val ) );
    }

    @Test
    public void shouldNotGetNonExistingByte ()
    {
        assertNull( kvStore.getByte( "field" ) );
    }

    @Test
    public void shouldGetExistingByteOrDefault ()
    {
        final byte val = 1;
        final byte other = 1;
        kvStore.set( "field", val );
        assertThat( kvStore.getByteOrDefault( "field", other ), equalTo( val ) );
    }

    @Test
    public void shouldGetNonExistingByteOrDefault ()
    {
        final byte val = 1;
        assertThat( kvStore.getByteOrDefault( "field", val ), equalTo( val ) );
    }

    @Test
    public void shouldGetExistingDouble ()
    {
        kvStore.set( "field", 1d );
        assertThat( kvStore.getDouble( "field" ), equalTo( 1d ) );
    }

    @Test
    public void shouldNotGetNonExistingDouble ()
    {
        assertNull( kvStore.getDouble( "field" ) );
    }

    @Test
    public void shouldGetExistingDoubleOrDefault ()
    {
        kvStore.set( "field", 1d );
        assertThat( kvStore.getDoubleOrDefault( "field", 2d ), equalTo( 1d ) );
    }

    @Test
    public void shouldGetNonExistingDoubleOrDefault ()
    {
        assertThat( kvStore.getDoubleOrDefault( "field", 1d ), equalTo( 1d ) );
    }

    @Test
    public void shouldGetExistingFloat ()
    {
        kvStore.set( "field", 1f );
        assertThat( kvStore.getFloat( "field" ), equalTo( 1f ) );
    }

    @Test
    public void shouldNotGetNonExistingFloat ()
    {
        assertNull( kvStore.getFloat( "field" ) );
    }

    @Test
    public void shouldGetExistingFloatOrDefault ()
    {
        kvStore.set( "field", 1f );
        assertThat( kvStore.getFloatOrDefault( "field", 2f ), equalTo( 1f ) );
    }

    @Test
    public void shouldGetNonExistingFloatOrDefault ()
    {
        assertThat( kvStore.getFloatOrDefault( "field", 1f ), equalTo( 1f ) );
    }

    @Test
    public void shouldGetExistingBinary ()
    {
        final byte[] val = new byte[] { 1 };
        kvStore.set( "field", val );
        assertThat( kvStore.getBinary( "field" ), equalTo( val ) );
    }

    @Test
    public void shouldGetExistingBinaryOrDefault ()
    {
        final byte[] val = new byte[] { 1 };
        kvStore.set( "field", val );
        assertThat( kvStore.getBinaryOrDefault( "field", new byte[] {} ), equalTo( val ) );
    }

    @Test
    public void shouldGetNonExistingBinaryOrDefault ()
    {
        final byte[] val = new byte[] { 1 };
        assertThat( kvStore.getBinaryOrDefault( "field", val ), equalTo( val ) );
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
        kvStore.set( "field", value );
        assertThat( kvStore.getCollection( "field" ), equalTo( value ) );
    }

    @Test
    public void shouldNotGetNonExistingCollection ()
    {
        assertNull( kvStore.getCollection( "field" ) );
    }

    @Test
    public void shouldGetExistingCollectionOrDefault ()
    {
        final Collection<Integer> value = makeArrayList();
        kvStore.set( "field", value );
        assertThat( kvStore.getCollectionOrDefault( "field", Collections.emptyList() ), equalTo( value ) );
    }

    @Test
    public void shouldGetNonExistingCollectionOrDefault ()
    {
        final Collection<Integer> value = makeArrayList();
        assertThat( kvStore.getCollectionOrDefault( "field", value ), equalTo( value ) );
    }

    @Test
    public void shouldGetExistingCollectionOrEmpty ()
    {
        final Collection<Integer> value = makeArrayList();
        kvStore.set( "field", value );
        assertThat( kvStore.getCollectionOrEmpty( "field" ), equalTo( value ) );
    }

    @Test
    public void shouldGetNonExistingCollectionOrEmpty ()
    {
        assertThat( kvStore.getCollectionOrEmpty( "field" ), empty() );
    }

    @Test
    public void shouldGetExistingList ()
    {
        final List<Integer> value = makeArrayList();
        kvStore.set( "field", value );
        assertThat( kvStore.getList( "field" ), equalTo( value ) );
    }

    @Test
    public void shouldNotGetNonExistingList ()
    {
        assertNull( kvStore.getList( "field" ) );
    }

    @Test
    public void shouldGetExistingListOrDefault ()
    {
        final List<Integer> value = makeArrayList();
        kvStore.set( "field", value );
        assertThat( kvStore.getListOrDefault( "field", Collections.emptyList() ), equalTo( value ) );
    }

    @Test
    public void shouldGetNonExistingListOrDefault ()
    {
        final List<Integer> value = makeArrayList();
        assertThat( kvStore.getListOrDefault( "field", value ), equalTo( value ) );
    }

    @Test
    public void shouldGetExistingListOrEmpty ()
    {
        final List<Integer> value = makeArrayList();
        kvStore.set( "field", value );
        assertThat( kvStore.getListOrEmpty( "field" ), equalTo( value ) );
    }

    @Test
    public void shouldGetNonExistingListOrEmpty ()
    {
        assertThat( kvStore.getListOrEmpty( "field" ), empty() );
    }

    @Test
    public void shouldGetExistingSet ()
    {
        final Set<Integer> value = makeHashSet();
        kvStore.set( "field", value );
        assertThat( kvStore.getSet( "field" ), equalTo( value ) );
    }

    @Test
    public void shouldNotGetNonExistingSet ()
    {
        assertNull( kvStore.getSet( "field" ) );
    }

    @Test
    public void shouldGetExistingSetOrDefault ()
    {
        final Set<Integer> value = makeHashSet();
        kvStore.set( "field", value );
        assertThat( kvStore.getSetOrDefault( "field", Collections.emptySet() ), equalTo( value ) );
    }

    @Test
    public void shouldGetNonExistingSetOrDefault ()
    {
        final Set<Integer> value = makeHashSet();
        assertThat( kvStore.getSetOrDefault( "field", value ), equalTo( value ) );
    }

    @Test
    public void shouldGetExistingSetOrEmpty ()
    {
        final Set<Integer> value = makeHashSet();
        kvStore.set( "field", value );
        assertThat( kvStore.getSetOrEmpty( "field" ), equalTo( value ) );
    }

    @Test
    public void shouldGetNonExistingSetOrEmpty ()
    {
        assertThat( kvStore.getSetOrEmpty( "field" ), empty() );
    }

    @Test
    public void shouldGetExistingMap ()
    {
        final Map<Integer, Integer> value = makeHashMap();
        kvStore.set( "field", value );
        assertThat( kvStore.getMap( "field" ), equalTo( value ) );
    }

    @Test
    public void shouldNotGetNonExistingMap ()
    {
        assertNull( kvStore.getMap( "field" ) );
    }

    @Test
    public void shouldGetExistingMapOrDefault ()
    {
        final Map<Integer, Integer> value = makeHashMap();
        kvStore.set( "field", value );
        assertThat( kvStore.getMapOrDefault( "field", Collections.emptyMap() ), equalTo( value ) );
    }

    @Test
    public void shouldGetNonExistingMapOrDefault ()
    {
        final Map<Integer, Integer> value = makeHashMap();
        assertThat( kvStore.getMapOrDefault( "field", value ), equalTo( value ) );
    }

    @Test
    public void shouldGetExistingMapOrEmpty ()
    {
        final Map<Integer, Integer> value = makeHashMap();
        kvStore.set( "field", value );
        assertThat( kvStore.getMapOrEmpty( "field" ), equalTo( value ) );
    }

    @Test
    public void shouldGetNonExistingMapOrEmpty ()
    {
        assertThat( kvStore.getMapOrEmpty( "field" ).size(), equalTo( 0 ) );
    }

    @Test
    public void shouldClearKeys ()
    {
        kvStore.set( "field", 1 );
        kvStore.clear();
        assertNull( kvStore.get( "field" ) );
    }

    @Test
    public void shouldFailToGetSize ()
    {
        kvStore.set( "field", 1 );
        assertThat( kvStore.size(), equalTo( 1 ) );
    }

}
