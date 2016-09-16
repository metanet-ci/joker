package cs.bilkent.joker.operator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

import cs.bilkent.joker.operator.schema.runtime.PortRuntimeSchema;
import cs.bilkent.joker.operator.schema.runtime.RuntimeSchemaField;
import cs.bilkent.joker.testutils.AbstractJokerTest;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;


public class TupleTest extends AbstractJokerTest
{

    @Test
    public void shouldSet ()
    {
        final Tuple tuple = new Tuple();
        tuple.set( "field", "value" );
        assertThat( tuple.getObject( "field" ), equalTo( "value" ) );
    }

    @Test
    public void shouldRemove ()
    {
        final Tuple tuple = new Tuple();
        tuple.set( "field", "value" );
        assertNotNull( tuple.remove( "field" ) );
        assertNull( tuple.getObject( "field" ) );
    }

    @Test
    public void shouldNotRemoveNonExistingField ()
    {
        final Tuple tuple = new Tuple();
        assertNull( tuple.remove( "field" ) );
    }

    @Test
    public void shouldDelete ()
    {
        final Tuple tuple = new Tuple();
        tuple.set( "field", "value" );
        assertTrue( tuple.delete( "field" ) );
        assertNull( tuple.getObject( "field" ) );
    }

    @Test
    public void shouldNotDeleteNonExistingField ()
    {
        final Tuple tuple = new Tuple();
        assertFalse( tuple.delete( "field" ) );
    }

    @Test
    public void shouldGetExisting ()
    {
        final Tuple tuple = new Tuple();
        tuple.set( "field", "value" );
        assertThat( tuple.<String>get( "field" ), equalTo( "value" ) );
    }

    @Test
    public void shouldGetExistingOrDefault ()
    {
        final Tuple tuple = new Tuple();
        tuple.set( "field", "value" );
        assertThat( tuple.getOrDefault( "field", "value2" ), equalTo( "value" ) );
    }

    @Test
    public void shouldGetNonExistingOrDefault ()
    {
        final Tuple tuple = new Tuple();
        assertThat( tuple.getOrDefault( "field", "value" ), equalTo( "value" ) );
    }

    @Test
    public void shouldContainExistingField ()
    {
        final Tuple tuple = new Tuple();
        tuple.set( "field", "value" );
        assertTrue( tuple.contains( "field" ) );
    }

    @Test
    public void shouldNotContainNonExistingField ()
    {
        final Tuple tuple = new Tuple();
        assertFalse( tuple.contains( "field" ) );
    }

    @Test
    public void shouldGetExistingObject ()
    {
        final Tuple tuple = new Tuple();
        tuple.set( "field", "value" );
        assertThat( tuple.getObject( "field" ), equalTo( "value" ) );
    }

    @Test
    public void shouldNotGetNonExistingObject ()
    {
        final Tuple tuple = new Tuple();
        assertNull( tuple.getObject( "field" ) );
    }

    @Test
    public void shouldGetExistingObjectOrDefault ()
    {
        final Tuple tuple = new Tuple();
        tuple.set( "field", "value" );
        assertThat( tuple.getObjectOrDefault( "field", "value2" ), equalTo( "value" ) );
    }

    @Test
    public void shouldGetNonExistingObjectOrDefault ()
    {
        final Tuple tuple = new Tuple();
        assertThat( tuple.getObjectOrDefault( "field", "value2" ), equalTo( "value2" ) );
    }

    @Test
    public void shouldGetExistingString ()
    {
        final Tuple tuple = new Tuple();
        tuple.set( "field", "value" );
        assertThat( tuple.getString( "field" ), equalTo( "value" ) );
    }

    @Test
    public void shouldNotGetExistingString ()
    {
        final Tuple tuple = new Tuple();
        assertNull( tuple.getString( "field" ) );
    }

    @Test
    public void shouldGetExistingStringOrDefault ()
    {
        final Tuple tuple = new Tuple();
        tuple.set( "field", "value" );
        assertThat( tuple.getStringOrDefault( "field", "value2" ), equalTo( "value" ) );
    }

    @Test
    public void shouldGetNonExistingStringOrDefault ()
    {
        final Tuple tuple = new Tuple();
        assertThat( tuple.getStringOrDefault( "field", "value2" ), equalTo( "value2" ) );
    }

    @Test
    public void shouldGetExistingInteger ()
    {
        final Tuple tuple = new Tuple();
        tuple.set( "field", 1 );
        assertThat( tuple.getInteger( "field" ), equalTo( 1 ) );
    }

    @Test
    public void shouldNotGetNotExistingInteger ()
    {
        final Tuple tuple = new Tuple();
        assertNull( tuple.getInteger( "field" ) );
    }

    @Test
    public void shouldGetExistingIntegerOrDefault ()
    {
        final Tuple tuple = new Tuple();
        tuple.set( "field", 1 );
        assertThat( tuple.getIntegerOrDefault( "field", 2 ), equalTo( 1 ) );
    }

    @Test
    public void shouldGetNonExistingIntegerOrDefault ()
    {
        final Tuple tuple = new Tuple();
        assertThat( tuple.getIntegerOrDefault( "field", 2 ), equalTo( 2 ) );
    }

    @Test
    public void shouldGetExistingLong ()
    {
        final Tuple tuple = new Tuple();
        tuple.set( "field", 1L );
        assertThat( tuple.getLong( "field" ), equalTo( 1L ) );
    }

    @Test
    public void shouldNotGetExistingLong ()
    {
        final Tuple tuple = new Tuple();
        assertNull( tuple.getLong( "field" ) );
    }

    @Test
    public void shouldGetExistingLongOrDefault ()
    {
        final Tuple tuple = new Tuple();
        tuple.set( "field", 1L );
        assertThat( tuple.getLongOrDefault( "field", 2L ), equalTo( 1L ) );
    }

    @Test
    public void shouldGetNonExistingLongOrDefault ()
    {
        final Tuple tuple = new Tuple();
        assertThat( tuple.getLongOrDefault( "field", 2L ), equalTo( 2L ) );
    }

    @Test
    public void shouldGetExistingBoolean ()
    {
        final Tuple tuple = new Tuple();
        tuple.set( "field", Boolean.TRUE );
        assertTrue( tuple.getBoolean( "field" ) );
    }

    @Test
    public void shouldNotGetNonExistingBoolean ()
    {
        final Tuple tuple = new Tuple();
        assertNull( tuple.getBoolean( "field" ) );
    }

    @Test
    public void shouldGetExistingBooleanOrDefault ()
    {
        final Tuple tuple = new Tuple();
        tuple.set( "field", Boolean.TRUE );
        assertTrue( tuple.getBooleanOrDefault( "field", Boolean.FALSE ) );
    }

    @Test
    public void shouldGetNonExistingBooleanOrDefault ()
    {
        final Tuple tuple = new Tuple();
        assertTrue( tuple.getBooleanOrDefault( "field", Boolean.TRUE ) );
    }

    @Test
    public void shouldGetGetExistingShort ()
    {
        final Tuple tuple = new Tuple();
        final short val = 1;
        tuple.set( "field", val );
        assertThat( tuple.getShort( "field" ), equalTo( val ) );
    }

    @Test
    public void shouldNotGetNonExistingShort ()
    {
        final Tuple tuple = new Tuple();
        assertNull( tuple.getShort( "field" ) );
    }

    @Test
    public void shouldGetExistingShortOrDefault ()
    {
        final Tuple tuple = new Tuple();
        final short val = 1;
        final short other = 2;
        tuple.set( "field", val );
        assertThat( tuple.getShortOrDefault( "field", other ), equalTo( val ) );
    }

    @Test
    public void shouldGetNonExistingShortOrDefault ()
    {
        final Tuple tuple = new Tuple();
        final short val = 1;
        assertThat( tuple.getShortOrDefault( "field", val ), equalTo( val ) );
    }

    @Test
    public void shouldGetExistingByte ()
    {
        final Tuple tuple = new Tuple();
        final byte val = 1;
        tuple.set( "field", val );
        assertThat( tuple.getByte( "field" ), equalTo( val ) );
    }

    @Test
    public void shouldNotGetNonExistingByte ()
    {
        final Tuple tuple = new Tuple();
        assertNull( tuple.getByte( "field" ) );
    }

    @Test
    public void shouldGetExistingByteOrDefault ()
    {
        final Tuple tuple = new Tuple();
        final byte val = 1;
        final byte other = 1;
        tuple.set( "field", val );
        assertThat( tuple.getByteOrDefault( "field", other ), equalTo( val ) );
    }

    @Test
    public void shouldGetNonExistingByteOrDefault ()
    {
        final Tuple tuple = new Tuple();
        final byte val = 1;
        assertThat( tuple.getByteOrDefault( "field", val ), equalTo( val ) );
    }

    @Test
    public void shouldGetExistingDouble ()
    {
        final Tuple tuple = new Tuple();
        tuple.set( "field", 1d );
        assertThat( tuple.getDouble( "field" ), equalTo( 1d ) );
    }

    @Test
    public void shouldNotGetNonExistingDouble ()
    {
        final Tuple tuple = new Tuple();
        assertNull( tuple.getDouble( "field" ) );
    }

    @Test
    public void shouldGetExistingDoubleOrDefault ()
    {
        final Tuple tuple = new Tuple();
        tuple.set( "field", 1d );
        assertThat( tuple.getDoubleOrDefault( "field", 2d ), equalTo( 1d ) );
    }

    @Test
    public void shouldGetNonExistingDoubleOrDefault ()
    {
        final Tuple tuple = new Tuple();
        assertThat( tuple.getDoubleOrDefault( "field", 1d ), equalTo( 1d ) );
    }

    @Test
    public void shouldGetExistingFloat ()
    {
        final Tuple tuple = new Tuple();
        tuple.set( "field", 1f );
        assertThat( tuple.getFloat( "field" ), equalTo( 1f ) );
    }

    @Test
    public void shouldNotGetNonExistingFloat ()
    {
        final Tuple tuple = new Tuple();
        assertNull( tuple.getFloat( "field" ) );
    }

    @Test
    public void shouldGetExistingFloatOrDefault ()
    {
        final Tuple tuple = new Tuple();
        tuple.set( "field", 1f );
        assertThat( tuple.getFloatOrDefault( "field", 2f ), equalTo( 1f ) );
    }

    @Test
    public void shouldGetNonExistingFloatOrDefault ()
    {
        final Tuple tuple = new Tuple();
        assertThat( tuple.getFloatOrDefault( "field", 1f ), equalTo( 1f ) );
    }

    @Test
    public void shouldGetExistingBinary ()
    {
        final Tuple tuple = new Tuple();
        final byte[] val = new byte[] { 1 };
        tuple.set( "field", val );
        assertThat( tuple.getBinary( "field" ), equalTo( val ) );
    }

    @Test
    public void shouldGetExistingBinaryOrDefault ()
    {
        final Tuple tuple = new Tuple();
        final byte[] val = new byte[] { 1 };
        tuple.set( "field", val );
        assertThat( tuple.getBinaryOrDefault( "field", new byte[] {} ), equalTo( val ) );
    }

    @Test
    public void shouldGetNonExistingBinaryOrDefault ()
    {
        final Tuple tuple = new Tuple();
        final byte[] val = new byte[] { 1 };
        assertThat( tuple.getBinaryOrDefault( "field", val ), equalTo( val ) );
    }

    @Test
    public void testGetWithSchemafulTuple ()
    {
        final List<RuntimeSchemaField> fields = new ArrayList<>();
        fields.add( new RuntimeSchemaField( "intField", Integer.class ) );
        fields.add( new RuntimeSchemaField( "doubleField", Double.class ) );
        fields.add( new RuntimeSchemaField( "stringField", String.class ) );
        final PortRuntimeSchema schema = new PortRuntimeSchema( fields );
        final Tuple tuple = new Tuple( schema );

        tuple.set( "schemalessField", 10 );
        tuple.set( "intField", 5 );
        tuple.set( "doubleField", 1.5 );
        tuple.set( "stringField", "str" );

        assertThat( tuple.get( "schemalessField" ), equalTo( 10 ) );
        assertThat( tuple.get( "intField" ), equalTo( 5 ) );
        assertThat( tuple.get( "doubleField" ), equalTo( 1.5 ) );
        assertThat( tuple.get( "stringField" ), equalTo( "str" ) );
    }

    @Test
    public void testGetAtSchemaIndex ()
    {
        final List<RuntimeSchemaField> fields = new ArrayList<>();
        fields.add( new RuntimeSchemaField( "intField", Integer.class ) );
        fields.add( new RuntimeSchemaField( "doubleField", Double.class ) );
        fields.add( new RuntimeSchemaField( "stringField", String.class ) );
        final PortRuntimeSchema schema = new PortRuntimeSchema( fields );
        final Tuple tuple = new Tuple( schema );

        tuple.set( "schemalessField", 10 );
        tuple.set( "intField", 5 );
        tuple.set( "doubleField", 1.5 );
        tuple.set( "stringField", "str" );

        final int intIndex = tuple.getSchema().getFieldIndex( "intField" );
        final int doubleIndex = tuple.getSchema().getFieldIndex( "doubleField" );
        final int stringIndex = tuple.getSchema().getFieldIndex( "stringField" );

        assertThat( tuple.getAtSchemaIndex( intIndex ), equalTo( 5 ) );
        assertThat( tuple.getAtSchemaIndex( doubleIndex ), equalTo( 1.5 ) );
        assertThat( tuple.getAtSchemaIndex( stringIndex ), equalTo( "str" ) );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotGetAtSchemaIndexWithInvalidValue ()
    {
        final Tuple tuple = new Tuple();

        tuple.getAtSchemaIndex( 0 );
    }

    @Test
    public void testSetWithIndex ()
    {
        final List<RuntimeSchemaField> fields = new ArrayList<>();
        fields.add( new RuntimeSchemaField( "intField", Integer.class ) );
        fields.add( new RuntimeSchemaField( "doubleField", Double.class ) );
        fields.add( new RuntimeSchemaField( "stringField", String.class ) );
        final PortRuntimeSchema schema = new PortRuntimeSchema( fields );
        final Tuple tuple = new Tuple( schema );

        final int intIndex = tuple.getSchema().getFieldIndex( "intField" );
        final int doubleIndex = tuple.getSchema().getFieldIndex( "doubleField" );
        final int stringIndex = tuple.getSchema().getFieldIndex( "stringField" );

        tuple.setAtSchemaIndex( intIndex, 5 );
        tuple.setAtSchemaIndex( doubleIndex, 1.5 );
        tuple.setAtSchemaIndex( stringIndex, "str" );

        assertThat( tuple.get( "intField" ), equalTo( 5 ) );
        assertThat( tuple.get( "doubleField" ), equalTo( 1.5 ) );
        assertThat( tuple.get( "stringField" ), equalTo( "str" ) );
    }

    @Test
    public void testSetIncompatibleValueWithIndex ()
    {
        final List<RuntimeSchemaField> fields = new ArrayList<>();
        fields.add( new RuntimeSchemaField( "intField", Integer.class ) );
        final PortRuntimeSchema schema = new PortRuntimeSchema( fields );
        final Tuple tuple = new Tuple( schema );

        final int intIndex = tuple.getSchema().getFieldIndex( "intField" );

        tuple.setAtSchemaIndex( intIndex, "val" );

        assertThat( tuple.get( "intField" ), equalTo( "val" ) );
    }

    @Test
    public void testSetWithSchemafulTuple ()
    {
        final List<RuntimeSchemaField> fields = new ArrayList<>();
        fields.add( new RuntimeSchemaField( "intField", Integer.class ) );
        fields.add( new RuntimeSchemaField( "doubleField", Double.class ) );
        fields.add( new RuntimeSchemaField( "stringField", String.class ) );
        final PortRuntimeSchema schema = new PortRuntimeSchema( fields );
        final Tuple tuple = new Tuple( schema );

        tuple.set( "schemalessField", 10 );
        tuple.set( "intField", 5 );
        tuple.set( "doubleField", 1.5 );
        tuple.set( "stringField", "str" );

        assertThat( tuple.get( "schemalessField" ), equalTo( 10 ) );
        assertThat( tuple.get( "intField" ), equalTo( 5 ) );
        assertThat( tuple.get( "doubleField" ), equalTo( 1.5 ) );
        assertThat( tuple.get( "stringField" ), equalTo( "str" ) );
    }

    @Test
    public void testSetIncompatibleValueToSchemaField ()
    {
        final List<RuntimeSchemaField> fields = new ArrayList<>();
        fields.add( new RuntimeSchemaField( "intField", Integer.class ) );
        final PortRuntimeSchema schema = new PortRuntimeSchema( fields );
        final Tuple tuple = new Tuple( schema );

        tuple.set( "intField", "val" );

        assertThat( tuple.get( "intField" ), equalTo( "val" ) );
    }

    @Test
    public void testContainsWithSchemafulTuple ()
    {
        final List<RuntimeSchemaField> fields = new ArrayList<>();
        fields.add( new RuntimeSchemaField( "intField", Integer.class ) );
        fields.add( new RuntimeSchemaField( "doubleField", Double.class ) );
        fields.add( new RuntimeSchemaField( "stringField", String.class ) );
        final PortRuntimeSchema schema = new PortRuntimeSchema( fields );
        final Tuple tuple = new Tuple( schema );

        tuple.set( "schemalessField", 10 );
        tuple.set( "intField", 5 );
        tuple.set( "doubleField", 1.5 );
        tuple.set( "stringField", "str" );

        assertTrue( tuple.contains( "schemalessField" ) );
        assertTrue( tuple.contains( "intField" ) );
        assertTrue( tuple.contains( "doubleField" ) );
        assertTrue( tuple.contains( "stringField" ) );
    }

    @Test
    public void testRemoveWithSchemafulTuple ()
    {
        final List<RuntimeSchemaField> fields = new ArrayList<>();
        fields.add( new RuntimeSchemaField( "intField", Integer.class ) );
        fields.add( new RuntimeSchemaField( "doubleField", Double.class ) );
        fields.add( new RuntimeSchemaField( "stringField", String.class ) );
        final PortRuntimeSchema schema = new PortRuntimeSchema( fields );
        final Tuple tuple = new Tuple( schema );

        tuple.set( "schemalessField", 10 );
        tuple.set( "schemalessField2", 20 );
        tuple.set( "intField", 5 );
        tuple.set( "doubleField", 1.5 );
        tuple.set( "stringField", "str" );

        assertThat( tuple.remove( "schemalessField" ), equalTo( 10 ) );
        assertThat( tuple.remove( "schemalessField2" ), equalTo( 20 ) );
        assertThat( tuple.remove( "intField" ), equalTo( 5 ) );
        assertThat( tuple.remove( "doubleField" ), equalTo( 1.5 ) );
        assertThat( tuple.remove( "stringField" ), equalTo( "str" ) );
    }

    @Test
    public void testDeleteWithSchemafulTuple ()
    {
        final List<RuntimeSchemaField> fields = new ArrayList<>();
        fields.add( new RuntimeSchemaField( "intField", Integer.class ) );
        fields.add( new RuntimeSchemaField( "doubleField", Double.class ) );
        fields.add( new RuntimeSchemaField( "stringField", String.class ) );
        final PortRuntimeSchema schema = new PortRuntimeSchema( fields );
        final Tuple tuple = new Tuple( schema );

        tuple.set( "schemalessField", 10 );
        tuple.set( "schemalessField2", 20 );
        tuple.set( "intField", 5 );
        tuple.set( "doubleField", 1.5 );
        tuple.set( "stringField", "str" );

        assertTrue( tuple.delete( "schemalessField" ) );
        assertTrue( tuple.delete( "schemalessField2" ) );
        assertTrue( tuple.delete( "intField" ) );
        assertTrue( tuple.delete( "doubleField" ) );
        assertTrue( tuple.delete( "stringField" ) );
    }

    @Test
    public void testClearWithSchemafulTuple ()
    {
        final List<RuntimeSchemaField> fields = new ArrayList<>();
        fields.add( new RuntimeSchemaField( "intField", Integer.class ) );
        fields.add( new RuntimeSchemaField( "doubleField", Double.class ) );
        fields.add( new RuntimeSchemaField( "stringField", String.class ) );
        final PortRuntimeSchema schema = new PortRuntimeSchema( fields );
        final Tuple tuple = new Tuple( schema );

        tuple.set( "schemalessField", 10 );
        tuple.set( "schemalessField2", 20 );
        tuple.set( "intField", 5 );
        tuple.set( "doubleField", 1.5 );
        tuple.set( "stringField", "str" );

        tuple.clear();

        assertFalse( tuple.delete( "schemalessField" ) );
        assertFalse( tuple.delete( "schemalessField2" ) );
        assertFalse( tuple.delete( "intField" ) );
        assertFalse( tuple.delete( "doubleField" ) );
        assertFalse( tuple.delete( "stringField" ) );
    }

    @Test
    public void testEqualsForTuplesWithSameSchema ()
    {
        final List<RuntimeSchemaField> fields = new ArrayList<>();
        fields.add( new RuntimeSchemaField( "intField", Integer.class ) );
        fields.add( new RuntimeSchemaField( "doubleField", Double.class ) );
        fields.add( new RuntimeSchemaField( "stringField", String.class ) );
        final PortRuntimeSchema schema = new PortRuntimeSchema( fields );
        final Tuple tuple1 = new Tuple( schema );
        final Tuple tuple2 = new Tuple( schema );

        tuple1.set( "schemalessField", 10 );
        tuple1.set( "schemalessField2", 20 );
        tuple1.set( "intField", 5 );
        tuple1.set( "doubleField", 1.5 );
        tuple1.set( "stringField", "str" );
        tuple2.set( "schemalessField", 10 );
        tuple2.set( "schemalessField2", 20 );
        tuple2.set( "intField", 5 );
        tuple2.set( "doubleField", 1.5 );
        tuple2.set( "stringField", "str" );

        assertTrue( tuple1.equals( tuple2 ) );
        assertTrue( tuple2.equals( tuple1 ) );
    }

    @Test
    public void testEqualsForTuplesWithDifferentSchemas ()
    {
        final List<RuntimeSchemaField> fields1 = new ArrayList<>();
        fields1.add( new RuntimeSchemaField( "intField", Integer.class ) );
        fields1.add( new RuntimeSchemaField( "stringField", String.class ) );
        final Tuple tuple1 = new Tuple( new PortRuntimeSchema( fields1 ) );

        final List<RuntimeSchemaField> fields2 = new ArrayList<>();
        fields2.add( new RuntimeSchemaField( "doubleField", Double.class ) );
        fields2.add( new RuntimeSchemaField( "stringField", String.class ) );
        final Tuple tuple2 = new Tuple( new PortRuntimeSchema( fields2 ) );

        tuple1.set( "schemalessField", 10 );
        tuple1.set( "schemalessField2", 20 );
        tuple1.set( "intField", 5 );
        tuple1.set( "doubleField", 1.5 );
        tuple1.set( "stringField", "str" );
        tuple2.set( "schemalessField", 10 );
        tuple2.set( "schemalessField2", 20 );
        tuple2.set( "intField", 5 );
        tuple2.set( "doubleField", 1.5 );
        tuple2.set( "stringField", "str" );

        assertTrue( tuple1.equals( tuple2 ) );
        assertTrue( tuple2.equals( tuple1 ) );
    }

    @Test
    public void testSameHashCodeForTuplesWithSameSchema ()
    {
        final List<RuntimeSchemaField> fields = new ArrayList<>();
        fields.add( new RuntimeSchemaField( "intField", Integer.class ) );
        fields.add( new RuntimeSchemaField( "doubleField", Double.class ) );
        fields.add( new RuntimeSchemaField( "stringField", String.class ) );
        final PortRuntimeSchema schema = new PortRuntimeSchema( fields );
        final Tuple tuple1 = new Tuple( schema );
        final Tuple tuple2 = new Tuple( schema );

        tuple1.set( "schemalessField", 10 );
        tuple1.set( "schemalessField2", 20 );
        tuple1.set( "intField", 5 );
        tuple1.set( "doubleField", 1.5 );
        tuple1.set( "stringField", "str" );
        tuple2.set( "schemalessField", 10 );
        tuple2.set( "schemalessField2", 20 );
        tuple2.set( "intField", 5 );
        tuple2.set( "doubleField", 1.5 );
        tuple2.set( "stringField", "str" );

        assertThat( tuple1.hashCode(), equalTo( tuple2.hashCode() ) );
    }

    @Test
    public void testSameHashCodeForTuplesWithDifferentSchemas ()
    {
        final List<RuntimeSchemaField> fields1 = new ArrayList<>();
        fields1.add( new RuntimeSchemaField( "intField", Integer.class ) );
        fields1.add( new RuntimeSchemaField( "stringField", String.class ) );
        final Tuple tuple1 = new Tuple( new PortRuntimeSchema( fields1 ) );

        final List<RuntimeSchemaField> fields2 = new ArrayList<>();
        fields2.add( new RuntimeSchemaField( "doubleField", Double.class ) );
        fields2.add( new RuntimeSchemaField( "stringField", String.class ) );
        final Tuple tuple2 = new Tuple( new PortRuntimeSchema( fields2 ) );

        tuple1.set( "schemalessField", 10 );
        tuple1.set( "schemalessField2", 20 );
        tuple1.set( "intField", 5 );
        tuple1.set( "doubleField", 1.5 );
        tuple1.set( "stringField", "str" );
        tuple2.set( "schemalessField", 10 );
        tuple2.set( "schemalessField2", 20 );
        tuple2.set( "intField", 5 );
        tuple2.set( "doubleField", 1.5 );
        tuple2.set( "stringField", "str" );

        assertThat( tuple1.hashCode(), equalTo( tuple2.hashCode() ) );
    }

    @Test
    public void testNotEqualsForTuplesWithSameSchema ()
    {
        final List<RuntimeSchemaField> fields = new ArrayList<>();
        fields.add( new RuntimeSchemaField( "intField", Integer.class ) );
        fields.add( new RuntimeSchemaField( "doubleField", Double.class ) );
        fields.add( new RuntimeSchemaField( "stringField", String.class ) );
        final PortRuntimeSchema schema = new PortRuntimeSchema( fields );
        final Tuple tuple1 = new Tuple( schema );
        final Tuple tuple2 = new Tuple( schema );

        tuple1.set( "schemalessField", 10 );
        tuple1.set( "schemalessField2", 20 );
        tuple1.set( "intField", 5 );
        tuple1.set( "doubleField", 1.5 );
        tuple1.set( "stringField", "str" );
        tuple2.set( "schemalessField", 10 );
        tuple2.set( "schemalessField2", 20 );
        tuple2.set( "intField", 5 );
        tuple2.set( "doubleField", 1.5 );

        assertFalse( tuple1.equals( tuple2 ) );
        assertFalse( tuple2.equals( tuple1 ) );
    }

    @Test
    public void testNotEqualsForTuplesWithDifferentSchemas ()
    {
        final List<RuntimeSchemaField> fields1 = new ArrayList<>();
        fields1.add( new RuntimeSchemaField( "intField", Integer.class ) );
        fields1.add( new RuntimeSchemaField( "stringField", String.class ) );
        final Tuple tuple1 = new Tuple( new PortRuntimeSchema( fields1 ) );

        final List<RuntimeSchemaField> fields2 = new ArrayList<>();
        fields2.add( new RuntimeSchemaField( "doubleField", Double.class ) );
        fields2.add( new RuntimeSchemaField( "stringField", String.class ) );
        final Tuple tuple2 = new Tuple( new PortRuntimeSchema( fields2 ) );

        tuple1.set( "schemalessField", 10 );
        tuple1.set( "schemalessField2", 20 );
        tuple1.set( "intField", 5 );
        tuple1.set( "doubleField", 1.5 );
        tuple1.set( "stringField", "str" );
        tuple2.set( "schemalessField", 10 );
        tuple2.set( "schemalessField2", 20 );
        tuple2.set( "doubleField", 1.5 );
        tuple2.set( "stringField", "str" );

        assertFalse( tuple1.equals( tuple2 ) );
        assertFalse( tuple2.equals( tuple1 ) );
    }


    @Test
    public void testSizeWithSchemafulTuple ()
    {
        final List<RuntimeSchemaField> fields = new ArrayList<>();
        fields.add( new RuntimeSchemaField( "intField", Integer.class ) );
        fields.add( new RuntimeSchemaField( "doubleField", Double.class ) );
        fields.add( new RuntimeSchemaField( "stringField", String.class ) );
        final PortRuntimeSchema schema = new PortRuntimeSchema( fields );
        final Tuple tuple = new Tuple( schema );

        tuple.set( "schemalessField", 10 );
        tuple.set( "schemalessField2", 20 );
        tuple.set( "doubleField", 1.5 );
        tuple.set( "stringField", "str" );

        assertThat( tuple.size(), equalTo( 4 ) );
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
        final Tuple tuple = new Tuple();
        final Collection<Integer> value = makeArrayList();
        tuple.set( "field", value );
        assertThat( tuple.getCollection( "field" ), equalTo( value ) );
    }

    @Test
    public void shouldNotGetNonExistingCollection ()
    {
        final Tuple tuple = new Tuple();
        assertNull( tuple.getCollection( "field" ) );
    }

    @Test
    public void shouldGetExistingCollectionOrDefault ()
    {
        final Tuple tuple = new Tuple();
        final Collection<Integer> value = makeArrayList();
        tuple.set( "field", value );
        assertThat( tuple.getCollectionOrDefault( "field", Collections.emptyList() ), equalTo( value ) );
    }

    @Test
    public void shouldGetNonExistingCollectionOrDefault ()
    {
        final Tuple tuple = new Tuple();
        final Collection<Integer> value = makeArrayList();
        assertThat( tuple.getCollectionOrDefault( "field", value ), equalTo( value ) );
    }

    @Test
    public void shouldGetExistingCollectionOrEmpty ()
    {
        final Tuple tuple = new Tuple();
        final Collection<Integer> value = makeArrayList();
        tuple.set( "field", value );
        assertThat( tuple.getCollectionOrEmpty( "field" ), equalTo( value ) );
    }

    @Test
    public void shouldGetNonExistingCollectionOrEmpty ()
    {
        final Tuple tuple = new Tuple();
        assertThat( tuple.getCollectionOrEmpty( "field" ), empty() );
    }

    @Test
    public void shouldGetExistingList ()
    {
        final Tuple tuple = new Tuple();
        final List<Integer> value = makeArrayList();
        tuple.set( "field", value );
        assertThat( tuple.getList( "field" ), equalTo( value ) );
    }

    @Test
    public void shouldNotGetNonExistingList ()
    {
        final Tuple tuple = new Tuple();
        assertNull( tuple.getList( "field" ) );
    }

    @Test
    public void shouldGetExistingListOrDefault ()
    {
        final Tuple tuple = new Tuple();
        final List<Integer> value = makeArrayList();
        tuple.set( "field", value );
        assertThat( tuple.getListOrDefault( "field", Collections.emptyList() ), equalTo( value ) );
    }

    @Test
    public void shouldGetNonExistingListOrDefault ()
    {
        final Tuple tuple = new Tuple();
        final List<Integer> value = makeArrayList();
        assertThat( tuple.getListOrDefault( "field", value ), equalTo( value ) );
    }

    @Test
    public void shouldGetExistingListOrEmpty ()
    {
        final Tuple tuple = new Tuple();
        final List<Integer> value = makeArrayList();
        tuple.set( "field", value );
        assertThat( tuple.getListOrEmpty( "field" ), equalTo( value ) );
    }

    @Test
    public void shouldGetNonExistingListOrEmpty ()
    {
        final Tuple tuple = new Tuple();
        assertThat( tuple.getListOrEmpty( "field" ), empty() );
    }

    @Test
    public void shouldGetExistingSet ()
    {
        final Tuple tuple = new Tuple();
        final Set<Integer> value = makeHashSet();
        tuple.set( "field", value );
        assertThat( tuple.getSet( "field" ), equalTo( value ) );
    }

    @Test
    public void shouldNotGetNonExistingSet ()
    {
        final Tuple tuple = new Tuple();
        assertNull( tuple.getSet( "field" ) );
    }

    @Test
    public void shouldGetExistingSetOrDefault ()
    {
        final Tuple tuple = new Tuple();
        final Set<Integer> value = makeHashSet();
        tuple.set( "field", value );
        assertThat( tuple.getSetOrDefault( "field", Collections.emptySet() ), equalTo( value ) );
    }

    @Test
    public void shouldGetNonExistingSetOrDefault ()
    {
        final Tuple tuple = new Tuple();
        final Set<Integer> value = makeHashSet();
        assertThat( tuple.getSetOrDefault( "field", value ), equalTo( value ) );
    }

    @Test
    public void shouldGetExistingSetOrEmpty ()
    {
        final Tuple tuple = new Tuple();
        final Set<Integer> value = makeHashSet();
        tuple.set( "field", value );
        assertThat( tuple.getSetOrEmpty( "field" ), equalTo( value ) );
    }

    @Test
    public void shouldGetNonExistingSetOrEmpty ()
    {
        final Tuple tuple = new Tuple();
        assertThat( tuple.getSetOrEmpty( "field" ), empty() );
    }

    @Test
    public void shouldGetExistingMap ()
    {
        final Tuple tuple = new Tuple();
        final Map<Integer, Integer> value = makeHashMap();
        tuple.set( "field", value );
        assertThat( tuple.getMap( "field" ), equalTo( value ) );
    }

    @Test
    public void shouldNotGetNonExistingMap ()
    {
        final Tuple tuple = new Tuple();
        assertNull( tuple.getMap( "field" ) );
    }

    @Test
    public void shouldGetExistingMapOrDefault ()
    {
        final Tuple tuple = new Tuple();
        final Map<Integer, Integer> value = makeHashMap();
        tuple.set( "field", value );
        assertThat( tuple.getMapOrDefault( "field", Collections.emptyMap() ), equalTo( value ) );
    }

    @Test
    public void shouldGetNonExistingMapOrDefault ()
    {
        final Tuple tuple = new Tuple();
        final Map<Integer, Integer> value = makeHashMap();
        assertThat( tuple.getMapOrDefault( "field", value ), equalTo( value ) );
    }

    @Test
    public void shouldGetExistingMapOrEmpty ()
    {
        final Tuple tuple = new Tuple();
        final Map<Integer, Integer> value = makeHashMap();
        tuple.set( "field", value );
        assertThat( tuple.getMapOrEmpty( "field" ), equalTo( value ) );
    }

    @Test
    public void shouldGetNonExistingMapOrEmpty ()
    {
        final Tuple tuple = new Tuple();
        assertThat( tuple.getMapOrEmpty( "field" ).size(), equalTo( 0 ) );
    }

    @Test
    public void shouldClear ()
    {
        final Tuple tuple = new Tuple();
        tuple.set( "field", "value" );
        tuple.clear();
        assertThat( tuple.size(), equalTo( 0 ) );
    }

    @Test
    public void shouldGetSize ()
    {
        final Tuple tuple = new Tuple();
        tuple.set( "field", "value" );
        assertThat( tuple.size(), equalTo( 1 ) );
    }

    @Test
    public void testTupleEquality ()
    {
        final Tuple tuple1 = new Tuple();
        final Tuple tuple2 = new Tuple();

        tuple1.set( "k1", "v1" );
        tuple1.set( "k2", "v2" );

        tuple2.set( "k1", "v1" );
        tuple2.set( "k2", "v2" );

        assertThat( tuple1, equalTo( tuple2 ) );
        assertThat( tuple1.hashCode(), equalTo( tuple2.hashCode() ) );
    }

}
