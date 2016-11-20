package cs.bilkent.joker.operator.schema.runtime;

import java.util.List;

import org.junit.Test;

import static cs.bilkent.joker.operator.schema.annotation.PortSchemaScope.EXACT_FIELD_SET;
import static cs.bilkent.joker.operator.schema.annotation.PortSchemaScope.EXTENDABLE_FIELD_SET;
import cs.bilkent.joker.test.AbstractJokerTest;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;


public class PortRuntimeSchemaBuilderTest extends AbstractJokerTest
{

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotCreatePortRuntimeSchemaBuilderWithNullPortSchemaScope ()
    {
        new PortRuntimeSchemaBuilder( null, emptyList() );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotCreatePortRuntimeSchemaBuilderWithNullFieldList ()
    {
        new PortRuntimeSchemaBuilder( EXACT_FIELD_SET, null );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotAddFieldWithNullFieldName ()
    {
        final PortRuntimeSchemaBuilder builder = new PortRuntimeSchemaBuilder( EXACT_FIELD_SET, emptyList() );
        builder.addField( null, int.class );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotAddFieldWithNullFieldType ()
    {
        final PortRuntimeSchemaBuilder builder = new PortRuntimeSchemaBuilder( EXACT_FIELD_SET, emptyList() );
        builder.addField( "field", null );
    }

    @Test( expected = IllegalStateException.class )
    public void shouldNotAddFieldToExactFieldsSetScope ()
    {
        final PortRuntimeSchemaBuilder builder = new PortRuntimeSchemaBuilder( EXACT_FIELD_SET, emptyList() );
        builder.addField( "field1", int.class );
    }

    @Test
    public void shouldAddFieldToBaseFieldsSetScope ()
    {
        final PortRuntimeSchemaBuilder builder = new PortRuntimeSchemaBuilder( EXTENDABLE_FIELD_SET, emptyList() );
        builder.addField( "field1", int.class );
    }

    @Test( expected = IllegalStateException.class )
    public void shouldNotAddFieldWithSameNameMultipleTimes ()
    {
        final PortRuntimeSchemaBuilder builder = new PortRuntimeSchemaBuilder( EXTENDABLE_FIELD_SET, emptyList() );
        builder.addField( "field1", int.class );
        builder.addField( "field1", int.class );
    }

    @Test
    public void shouldBuildPortRuntimeSchema ()
    {
        final RuntimeSchemaField field1 = new RuntimeSchemaField( "field1", int.class );
        final PortRuntimeSchemaBuilder builder = new PortRuntimeSchemaBuilder( EXTENDABLE_FIELD_SET, singletonList( field1 ) );
        final RuntimeSchemaField field2 = new RuntimeSchemaField( "field2", long.class );
        builder.addField( field2 );

        final PortRuntimeSchema schema = builder.build();

        final List<RuntimeSchemaField> fields = schema.getFields();
        assertThat( fields, hasSize( 2 ) );
        assertThat( fields.get( 0 ), equalTo( field1 ) );
        assertThat( fields.get( 1 ), equalTo( field2 ) );
    }

}
