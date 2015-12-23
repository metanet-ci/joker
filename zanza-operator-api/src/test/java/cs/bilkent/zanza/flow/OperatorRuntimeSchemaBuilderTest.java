package cs.bilkent.zanza.flow;

import org.junit.Test;

import cs.bilkent.zanza.operator.schema.annotation.OperatorSchema;
import cs.bilkent.zanza.operator.schema.annotation.PortSchema;
import static cs.bilkent.zanza.operator.schema.annotation.PortSchemaScope.BASE_FIELD_SET;
import static cs.bilkent.zanza.operator.schema.annotation.PortSchemaScope.EXACT_FIELD_SET;
import cs.bilkent.zanza.operator.schema.annotation.SchemaField;
import cs.bilkent.zanza.operator.schema.runtime.OperatorRuntimeSchema;
import cs.bilkent.zanza.operator.schema.runtime.PortRuntimeSchema;
import cs.bilkent.zanza.operator.schema.runtime.RuntimeSchemaField;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class OperatorRuntimeSchemaBuilderTest
{

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotCreateOperatorRuntimeSchemaBuilderWithNegativeInputPortCount ()
    {
        new OperatorRuntimeSchemaBuilder( -1, 1 );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotCreateOperatorRuntimeSchemaBuilderWithNegativeOutputInputPortCount ()
    {
        new OperatorRuntimeSchemaBuilder( 1, -1 );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotGetPortRuntimeSchemaBuilderWithNegativeInputPortIndex ()
    {
        final OperatorRuntimeSchemaBuilder builder = new OperatorRuntimeSchemaBuilder( 1, 1 );
        builder.getInputPortSchemaBuilder( -1 );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotGetPortRuntimeSchemaBuilderWithInvalidInputPortIndex ()
    {
        final OperatorRuntimeSchemaBuilder builder = new OperatorRuntimeSchemaBuilder( 1, 1 );
        builder.getInputPortSchemaBuilder( 1 );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotGetPortRuntimeSchemaBuilderWithNegativeOutputPortIndex ()
    {
        final OperatorRuntimeSchemaBuilder builder = new OperatorRuntimeSchemaBuilder( 1, 1 );
        builder.getOutputPortSchemaBuilder( -1 );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotGetPortRuntimeSchemaBuilderWithInvalidOutputPortIndex ()
    {
        final OperatorRuntimeSchemaBuilder builder = new OperatorRuntimeSchemaBuilder( 1, 1 );
        builder.getOutputPortSchemaBuilder( 1 );
    }

    @Test
    public void shouldBuildOperatorRuntimeSchema ()
    {
        final OperatorRuntimeSchemaBuilder builder = new OperatorRuntimeSchemaBuilder( 2, 2 );
        builder.getInputPortSchemaBuilder( 0 ).addField( "field1", int.class );
        builder.getInputPortSchemaBuilder( 1 ).addField( "field2", double.class );
        builder.getOutputPortSchemaBuilder( 0 ).addField( "field3", long.class );
        builder.getOutputPortSchemaBuilder( 1 ).addField( "field4", float.class );

        final OperatorRuntimeSchema schema = builder.build();

        assertThat( schema.getInputSchemas(), hasSize( 2 ) );
        assertThat( schema.getOutputSchemas(), hasSize( 2 ) );

        final PortRuntimeSchema inputSchema0 = schema.getInputSchema( 0 );
        final PortRuntimeSchema inputSchema1 = schema.getInputSchema( 1 );
        final PortRuntimeSchema outputSchema0 = schema.getOutputSchema( 0 );
        final PortRuntimeSchema outputSchema1 = schema.getOutputSchema( 1 );

        assertThat( inputSchema0.getFields(), hasSize( 1 ) );
        assertThat( inputSchema1.getFields(), hasSize( 1 ) );
        assertThat( outputSchema0.getFields(), hasSize( 1 ) );
        assertThat( outputSchema1.getFields(), hasSize( 1 ) );

        final RuntimeSchemaField field1 = inputSchema0.getFields().get( 0 );
        final RuntimeSchemaField field2 = inputSchema1.getFields().get( 0 );
        final RuntimeSchemaField field3 = outputSchema0.getFields().get( 0 );
        final RuntimeSchemaField field4 = outputSchema1.getFields().get( 0 );

        assertThat( field1, equalTo( new RuntimeSchemaField( "field1", int.class ) ) );
        assertThat( field2, equalTo( new RuntimeSchemaField( "field2", double.class ) ) );
        assertThat( field3, equalTo( new RuntimeSchemaField( "field3", long.class ) ) );
        assertThat( field4, equalTo( new RuntimeSchemaField( "field4", float.class ) ) );
    }

    @Test
    public void shouldBuildOperatorRuntimeSchemaFromOperatorSchema ()
    {
        final OperatorSchema schemaAnnotation = AnnotatedClass.class.getDeclaredAnnotation( OperatorSchema.class );
        final OperatorRuntimeSchemaBuilder builder = new OperatorRuntimeSchemaBuilder( 1, 1, schemaAnnotation );

        final OperatorRuntimeSchema schema = builder.build();

        assertThat( schema.getInputSchemas(), hasSize( 1 ) );
        assertThat( schema.getOutputSchemas(), hasSize( 1 ) );

        final PortRuntimeSchema inputSchema0 = schema.getInputSchema( 0 );
        final PortRuntimeSchema outputSchema0 = schema.getOutputSchema( 0 );

        assertThat( inputSchema0.getFields(), hasSize( 1 ) );
        assertThat( outputSchema0.getFields(), hasSize( 1 ) );

        final RuntimeSchemaField field1 = inputSchema0.getFields().get( 0 );
        final RuntimeSchemaField field2 = outputSchema0.getFields().get( 0 );

        assertThat( field1, equalTo( new RuntimeSchemaField( "field1", int.class ) ) );
        assertThat( field2, equalTo( new RuntimeSchemaField( "field2", long.class ) ) );
    }

    @OperatorSchema( inputs = { @PortSchema( portIndex = 0, scope = EXACT_FIELD_SET,
            fields = { @SchemaField( name = "field1", type = int.class ) } ) },
            outputs = { @PortSchema( portIndex = 0, scope = BASE_FIELD_SET,
                    fields = { @SchemaField( name = "field2", type = long.class ) } ) } )
    private static class AnnotatedClass
    {

    }

}
