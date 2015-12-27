package cs.bilkent.zanza.flow;

import java.util.List;

import org.junit.Test;

import cs.bilkent.zanza.flow.FlowBuilderTest.OperatorWithDynamicPortCounts;
import cs.bilkent.zanza.flow.FlowBuilderTest.OperatorWithFixedPortCounts;
import cs.bilkent.zanza.flow.FlowBuilderTest.OperatorWithInvalidInputPortCount;
import cs.bilkent.zanza.flow.FlowBuilderTest.OperatorWithInvalidOutputPortCount;
import cs.bilkent.zanza.flow.FlowBuilderTest.OperatorWithNoSpec;
import static cs.bilkent.zanza.flow.Port.DEFAULT_PORT_INDEX;
import cs.bilkent.zanza.operator.InitializationContext;
import cs.bilkent.zanza.operator.InvocationContext;
import cs.bilkent.zanza.operator.InvocationResult;
import cs.bilkent.zanza.operator.Operator;
import cs.bilkent.zanza.operator.schema.annotation.OperatorSchema;
import cs.bilkent.zanza.operator.schema.annotation.PortSchema;
import static cs.bilkent.zanza.operator.schema.annotation.PortSchemaScope.BASE_FIELD_SET;
import static cs.bilkent.zanza.operator.schema.annotation.PortSchemaScope.EXACT_FIELD_SET;
import cs.bilkent.zanza.operator.schema.annotation.SchemaField;
import cs.bilkent.zanza.operator.schema.runtime.OperatorRuntimeSchema;
import cs.bilkent.zanza.operator.schema.runtime.PortRuntimeSchema;
import cs.bilkent.zanza.operator.schema.runtime.RuntimeSchemaField;
import cs.bilkent.zanza.operator.spec.OperatorSpec;
import static cs.bilkent.zanza.operator.spec.OperatorType.PARTITIONED_STATEFUL;
import cs.bilkent.zanza.scheduling.SchedulingStrategy;
import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class OperatorDefinitionBuilderTest
{
    @Test( expected = IllegalArgumentException.class )
    public void shouldNotCreateBuilderWithNullId ()
    {
        OperatorDefinitionBuilder.newInstance( null, OperatorWithDynamicPortCounts.class );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotCreateBuilderWithEmptyId ()
    {
        OperatorDefinitionBuilder.newInstance( "", OperatorWithDynamicPortCounts.class );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotCreateBuilderWithNullClass ()
    {
        OperatorDefinitionBuilder.newInstance( "op1", null );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotCreateBuilderWithoutOperatorSpec ()
    {
        OperatorDefinitionBuilder.newInstance( "op1", OperatorWithNoSpec.class );
    }

    @Test( expected = IllegalStateException.class )
    public void shouldNotCreateBuilderWithInvalidFixedInputCountAndNoConfig ()
    {
        OperatorDefinitionBuilder.newInstance( "op1", OperatorWithDynamicPortCounts.class ).build();
    }

    @Test( expected = IllegalStateException.class )
    public void shouldNotCreateBuilderWithDynamicInputPortCount ()
    {
        OperatorDefinitionBuilder.newInstance( "op1", OperatorWithDynamicPortCounts.class ).setOutputPortCount( 1 ).build();
    }

    @Test( expected = IllegalStateException.class )
    public void shouldNotCreateBuilderWithDynamicOutputPortCount ()
    {
        OperatorDefinitionBuilder.newInstance( "op1", OperatorWithDynamicPortCounts.class ).setInputPortCount( 1 ).build();
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotCreateBuilderWithInvalidFixedInputCount ()
    {
        OperatorDefinitionBuilder.newInstance( "op1", OperatorWithInvalidInputPortCount.class ).build();
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotCreateBuilderWithInvalidFixedOutputCount ()
    {
        OperatorDefinitionBuilder.newInstance( "op1", OperatorWithInvalidOutputPortCount.class ).build();
    }

    @Test
    public void shouldBuildEmptyRuntimeSchemaWithNoSchemaDefinition ()
    {
        final OperatorDefinition definition = OperatorDefinitionBuilder.newInstance( "op1", OperatorWithFixedPortCounts.class ).build();
        assertNotNull( definition );

        final OperatorRuntimeSchema schema = definition.schema;
        for ( int portIndex = 0; portIndex < definition.inputPortCount; portIndex++ )
        {
            final PortRuntimeSchema inputSchema = schema.getInputSchema( portIndex );
            assertTrue( inputSchema.getFields().isEmpty() );
        }
        for ( int portIndex = 0; portIndex < definition.outputPortCount; portIndex++ )
        {
            final PortRuntimeSchema outputSchema = schema.getOutputSchema( portIndex );
            assertTrue( outputSchema.getFields().isEmpty() );
        }
    }

    @Test
    public void shouldBuildWithPortSchemaDefinition ()
    {
        final OperatorDefinition definition = OperatorDefinitionBuilder.newInstance( "op1", OperatorWithExactInputPortSchema.class )
                                                                       .build();
        final OperatorRuntimeSchema schema = definition.schema;
        assertThat( schema.getInputSchemas(), hasSize( 1 ) );
        final PortRuntimeSchema inputSchema = schema.getInputSchema( DEFAULT_PORT_INDEX );
        final List<RuntimeSchemaField> inputFields = inputSchema.getFields();
        assertThat( inputFields, hasSize( 1 ) );
        assertThat( inputFields.get( 0 ), equalTo( new RuntimeSchemaField( "field1", int.class ) ) );
        assertThat( schema.getOutputSchemas(), hasSize( 1 ) );
        final PortRuntimeSchema outputSchema = schema.getOutputSchema( DEFAULT_PORT_INDEX );
        final List<RuntimeSchemaField> outputFields = outputSchema.getFields();
        assertThat( outputFields, hasSize( 1 ) );
        assertThat( outputFields.get( 0 ), equalTo( new RuntimeSchemaField( "field2", long.class ) ) );
    }

    @Test( expected = IllegalStateException.class )
    public void shouldNotExtendExactPortSchema ()
    {
        final OperatorRuntimeSchemaBuilder schemaBuilder = new OperatorRuntimeSchemaBuilder( 1, 1 );
        schemaBuilder.getInputPortSchemaBuilder( DEFAULT_PORT_INDEX ).addField( "field3", boolean.class );
        OperatorDefinitionBuilder.newInstance( "op1", OperatorWithExactInputPortSchema.class )
                                 .setExtendingSchema( schemaBuilder.build() )
                                 .build();
    }

    @Test
    public void shouldBuildWithExtendedOutputPortSchema ()
    {
        final OperatorRuntimeSchemaBuilder schemaBuilder = new OperatorRuntimeSchemaBuilder( 1, 1 );
        schemaBuilder.getOutputPortSchemaBuilder( DEFAULT_PORT_INDEX ).addField( "field3", boolean.class );

        final OperatorDefinition definition = OperatorDefinitionBuilder.newInstance( "op1", OperatorWithExactInputPortSchema.class )
                                                                       .setExtendingSchema( schemaBuilder.build() )
                                                                       .build();
        final OperatorRuntimeSchema schema = definition.schema;
        assertThat( schema.getOutputSchemas(), hasSize( 1 ) );
        final PortRuntimeSchema outputSchema = schema.getOutputSchema( DEFAULT_PORT_INDEX );
        final List<RuntimeSchemaField> outputFields = outputSchema.getFields();
        assertThat( outputFields, hasSize( 2 ) );
        assertThat( outputFields.get( 0 ), equalTo( new RuntimeSchemaField( "field2", long.class ) ) );
        assertThat( outputFields.get( 1 ), equalTo( new RuntimeSchemaField( "field3", boolean.class ) ) );
    }

    @Test( expected = IllegalStateException.class )
    public void shouldNotSetPartitionFieldNamesToStatelessOperator ()
    {
        OperatorDefinitionBuilder.newInstance( "op1", OperatorWithDynamicPortCounts.class )
                                 .setPartitionFieldNames( singletonList( "field1" ) );
    }

    @Test( expected = IllegalStateException.class )
    public void shouldNotSetPartitionFieldNamesToStatefulOperator ()
    {
        OperatorDefinitionBuilder.newInstance( "op1", OperatorWithFixedPortCounts.class )
                                 .setPartitionFieldNames( singletonList( "field1" ) );
    }

    @Test
    public void shouldSetPartitionFieldNamesToPartitionedStatefulOperator ()
    {
        final List<String> partitionFieldNames = singletonList( "field1" );
        final OperatorDefinition definition = OperatorDefinitionBuilder.newInstance( "op1", OperatorWithExactInputPortSchema.class )
                                                                       .setPartitionFieldNames( partitionFieldNames )
                                                                       .build();

        assertTrue( partitionFieldNames.equals( definition.partitionFieldNames ) );
    }

    @OperatorSpec( type = PARTITIONED_STATEFUL, inputPortCount = 1, outputPortCount = 1 )
    @OperatorSchema( inputs = { @PortSchema( portIndex = DEFAULT_PORT_INDEX, scope = EXACT_FIELD_SET, fields = { @SchemaField( name = "field1", type = int.class ) } ) },
            outputs = { @PortSchema( portIndex = DEFAULT_PORT_INDEX, scope = BASE_FIELD_SET, fields = { @SchemaField( name = "field2", type = long.class ) } ) } )
    public static class OperatorWithExactInputPortSchema implements Operator
    {

        @Override
        public InvocationResult process ( InvocationContext invocationContext )
        {
            return null;
        }

        @Override
        public SchedulingStrategy init ( final InitializationContext context )
        {
            return null;
        }
    }

}
