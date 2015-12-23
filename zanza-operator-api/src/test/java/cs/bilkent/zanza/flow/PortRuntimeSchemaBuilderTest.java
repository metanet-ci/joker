package cs.bilkent.zanza.flow;

import java.util.List;

import org.junit.Test;

import static cs.bilkent.zanza.operator.schema.annotation.PortSchemaScope.BASE_FIELD_SET;
import static cs.bilkent.zanza.operator.schema.annotation.PortSchemaScope.EXACT_FIELD_SET;
import cs.bilkent.zanza.operator.schema.runtime.PortRuntimeSchema;
import cs.bilkent.zanza.operator.schema.runtime.RuntimeSchemaField;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class PortRuntimeSchemaBuilderTest
{

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotCreatePortRuntimeSchemaBuilderWithNegativePortIndex ()
    {
        new PortRuntimeSchemaBuilder( -1 );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotCreatePortRuntimeSchemaBuilderWithNegativePortIndex2 ()
    {
        new PortRuntimeSchemaBuilder( -1, EXACT_FIELD_SET, emptyList() );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotCreatePortRuntimeSchemaBuilderWithNullPortSchemaScope ()
    {
        new PortRuntimeSchemaBuilder( 0, null, emptyList() );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotCreatePortRuntimeSchemaBuilderWithNullFieldList ()
    {
        new PortRuntimeSchemaBuilder( 0, EXACT_FIELD_SET, null );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotAddFieldWithNullFieldName ()
    {
        final PortRuntimeSchemaBuilder builder = new PortRuntimeSchemaBuilder( 0, EXACT_FIELD_SET, emptyList() );
        builder.addField( null, int.class );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotAddFieldWithNullFieldType ()
    {
        final PortRuntimeSchemaBuilder builder = new PortRuntimeSchemaBuilder( 0, EXACT_FIELD_SET, emptyList() );
        builder.addField( "field", null );
    }

    @Test( expected = IllegalStateException.class )
    public void shouldNotAddFieldToExactFieldsSetScope ()
    {
        final PortRuntimeSchemaBuilder builder = new PortRuntimeSchemaBuilder( 0, EXACT_FIELD_SET, emptyList() );
        builder.addField( "field1", int.class );
    }

    @Test
    public void shouldAddFieldToBaseFieldsSetScope ()
    {
        final PortRuntimeSchemaBuilder builder = new PortRuntimeSchemaBuilder( 0, BASE_FIELD_SET, emptyList() );
        builder.addField( "field1", int.class );
    }

    @Test( expected = IllegalStateException.class )
    public void shouldNotAddFieldWithSameNameMultipleTimes ()
    {
        final PortRuntimeSchemaBuilder builder = new PortRuntimeSchemaBuilder( 0, BASE_FIELD_SET, emptyList() );
        builder.addField( "field1", int.class );
        builder.addField( "field1", int.class );
    }

    @Test
    public void shouldBuildPortRuntimeSchema ()
    {
        final RuntimeSchemaField field1 = new RuntimeSchemaField( "field1", int.class );
        final PortRuntimeSchemaBuilder builder = new PortRuntimeSchemaBuilder( 0, BASE_FIELD_SET, singletonList( field1 ) );
        final RuntimeSchemaField field2 = new RuntimeSchemaField( "field2", long.class );
        builder.addField( field2 );

        final PortRuntimeSchema schema = builder.build();

        assertThat( schema.getPortIndex(), equalTo( 0 ) );
        final List<RuntimeSchemaField> fields = schema.getFields();
        assertThat( fields, hasSize( 2 ) );
        assertThat( fields.get( 0 ), equalTo( field1 ) );
        assertThat( fields.get( 1 ), equalTo( field2 ) );
    }

}
