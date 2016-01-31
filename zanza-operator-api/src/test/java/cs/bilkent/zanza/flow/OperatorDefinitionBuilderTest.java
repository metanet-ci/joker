package cs.bilkent.zanza.flow;

import java.util.List;

import org.junit.Test;

import cs.bilkent.zanza.flow.FlowDefinitionBuilderTest.NopOperator;
import cs.bilkent.zanza.flow.FlowDefinitionBuilderTest.OperatorWithNoSpec;
import cs.bilkent.zanza.flow.FlowDefinitionBuilderTest.StatefulOperatorWithFixedPortCounts;
import cs.bilkent.zanza.flow.FlowDefinitionBuilderTest.StatefulOperatorWithInvalidInputPortCount;
import cs.bilkent.zanza.flow.FlowDefinitionBuilderTest.StatefulOperatorWithInvalidOutputPortCount;
import cs.bilkent.zanza.flow.FlowDefinitionBuilderTest.StatelessOperatorWithDynamicPortCounts;
import static cs.bilkent.zanza.flow.Port.DEFAULT_PORT_INDEX;
import cs.bilkent.zanza.operator.schema.annotation.OperatorSchema;
import cs.bilkent.zanza.operator.schema.annotation.PortSchema;
import static cs.bilkent.zanza.operator.schema.annotation.PortSchemaScope.BASE_FIELD_SET;
import static cs.bilkent.zanza.operator.schema.annotation.PortSchemaScope.EXACT_FIELD_SET;
import cs.bilkent.zanza.operator.schema.annotation.SchemaField;
import cs.bilkent.zanza.operator.schema.runtime.OperatorRuntimeSchema;
import cs.bilkent.zanza.operator.schema.runtime.PortRuntimeSchema;
import cs.bilkent.zanza.operator.schema.runtime.RuntimeSchemaField;
import cs.bilkent.zanza.operator.spec.OperatorSpec;
import cs.bilkent.zanza.operator.spec.OperatorType;
import static cs.bilkent.zanza.operator.spec.OperatorType.PARTITIONED_STATEFUL;
import static cs.bilkent.zanza.operator.spec.OperatorType.STATELESS;
import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;


public class OperatorDefinitionBuilderTest
{
    @Test( expected = IllegalArgumentException.class )
    public void shouldNotBuildBuilderWithNullId ()
    {
        OperatorDefinitionBuilder.newInstance( null, StatelessOperatorWithDynamicPortCounts.class );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotBuildBuilderWithEmptyId ()
    {
        OperatorDefinitionBuilder.newInstance( "", StatelessOperatorWithDynamicPortCounts.class );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotBuildBuilderWithNullClass ()
    {
        OperatorDefinitionBuilder.newInstance( "op1", null );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotBuildBuilderWithoutOperatorSpec ()
    {
        OperatorDefinitionBuilder.newInstance( "op1", OperatorWithNoSpec.class );
    }

    @Test( expected = IllegalStateException.class )
    public void shouldNotBuildOperatorDefinitionWithInvalidFixedInputCountAndNoConfig ()
    {
        OperatorDefinitionBuilder.newInstance( "op1", StatelessOperatorWithDynamicPortCounts.class ).build();
    }

    @Test( expected = IllegalStateException.class )
    public void shouldNotBuildOperatorDefinitionWithDynamicInputPortCount ()
    {
        OperatorDefinitionBuilder.newInstance( "op1", StatelessOperatorWithDynamicPortCounts.class ).setOutputPortCount( 1 ).build();
    }

    @Test
    public void shouldSetSingleInputPortCountForStatelessOperator ()
    {
        OperatorDefinitionBuilder.newInstance( "op1", StatelessOperatorWithDynamicPortCounts.class ).setInputPortCount( 1 );
    }

    @Test
    public void shouldSetZeroInputPortCountForStatelessOperator ()
    {
        OperatorDefinitionBuilder.newInstance( "op1", StatelessOperatorWithDynamicPortCounts.class ).setInputPortCount( 0 );
    }

    @Test
    public void shouldSetSingleOutputPortCountForStatelessOperator ()
    {
        OperatorDefinitionBuilder.newInstance( "op1", StatelessOperatorWithDynamicPortCounts.class ).setOutputPortCount( 1 );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotSetMultipleInputPortCountForStatelessOperator ()
    {
        OperatorDefinitionBuilder.newInstance( "op1", StatelessOperatorWithDynamicPortCounts.class ).setInputPortCount( 2 );
    }

    @Test
    public void shouldSetMultipleOutputPortCountForStatelessOperator ()
    {
        OperatorDefinitionBuilder.newInstance( "op1", StatelessOperatorWithDynamicPortCounts.class ).setOutputPortCount( 2 );
    }

    @Test( expected = IllegalStateException.class )
    public void shouldNotBuildOperatorDefinitionWithDynamicOutputPortCount ()
    {
        OperatorDefinitionBuilder.newInstance( "op1", StatelessOperatorWithDynamicPortCounts.class ).setInputPortCount( 1 ).build();
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotBuildBuilderWithMultipleInputPortCount ()
    {
        OperatorDefinitionBuilder.newInstance( "op1", StatelessOperatorWithMultipleInputPortCount.class );
    }

    @Test
    public void shouldBuildStatelessOperatorDefinitionWithMultipleOutputPortCount ()
    {
        OperatorDefinitionBuilder.newInstance( "op1", StatelessOperatorWithMultipleOutputPortCount.class ).setInputPortCount( 1 ).build();
    }

    @Test
    public void shouldBuildBuilderWithSingleInputOutputPortCount ()
    {
        OperatorDefinitionBuilder.newInstance( "op1", StatelessOperatorWithSingleInputOutputPortCount.class );
    }

    @Test
    public void shouldBuildBuilderWithZeroInputOutputPortCount ()
    {
        OperatorDefinitionBuilder.newInstance( "op1", StatelessOperatorWithZeroInputOutputPortCount.class );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotBuildOperatorDefinitionWithInvalidFixedInputCount ()
    {
        OperatorDefinitionBuilder.newInstance( "op1", StatefulOperatorWithInvalidInputPortCount.class ).build();
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotBuildOperatorDefinitionWithInvalidFixedOutputCount ()
    {
        OperatorDefinitionBuilder.newInstance( "op1", StatefulOperatorWithInvalidOutputPortCount.class ).build();
    }

    @Test( expected = IllegalStateException.class )
    public void shouldNotExtendExactPortSchema ()
    {
        final OperatorRuntimeSchemaBuilder schemaBuilder = new OperatorRuntimeSchemaBuilder( 1, 1 );
        schemaBuilder.getInputPortSchemaBuilder( DEFAULT_PORT_INDEX ).addField( "field3", boolean.class );
        OperatorDefinitionBuilder.newInstance( "op1", PartitionedStatefulOperatorWithExactInputPortSchema.class )
                                 .setExtendingSchema( schemaBuilder.build() )
                                 .build();
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotExtendWithExceedingInputPortRuntimeSchema ()
    {
        final OperatorRuntimeSchemaBuilder schemaBuilder = new OperatorRuntimeSchemaBuilder( 2, 1 );
        schemaBuilder.getInputPortSchemaBuilder( DEFAULT_PORT_INDEX ).addField( "field3", boolean.class );
        OperatorDefinitionBuilder.newInstance( "op1", PartitionedStatefulOperatorWithExactInputPortSchema.class )
                                 .setExtendingSchema( schemaBuilder.build() )
                                 .build();
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotExtendWithExceedingOutputPortRuntimeSchema ()
    {
        final OperatorRuntimeSchemaBuilder schemaBuilder = new OperatorRuntimeSchemaBuilder( 1, 2 );
        schemaBuilder.getInputPortSchemaBuilder( DEFAULT_PORT_INDEX ).addField( "field3", boolean.class );
        OperatorDefinitionBuilder.newInstance( "op1", PartitionedStatefulOperatorWithExactInputPortSchema.class )
                                 .setExtendingSchema( schemaBuilder.build() )
                                 .build();
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotExtendSchemaWithDuplicateInputField ()
    {
        final OperatorRuntimeSchemaBuilder schemaBuilder = new OperatorRuntimeSchemaBuilder( 1, 1 );
        schemaBuilder.getInputPortSchemaBuilder( DEFAULT_PORT_INDEX ).addField( "field1", boolean.class );
        OperatorDefinitionBuilder.newInstance( "op1", PartitionedStatefulOperatorWithExactInputPortSchema.class )
                                 .setExtendingSchema( schemaBuilder.build() )
                                 .build();
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotExtendSchemaWithDuplicateOutputField ()
    {
        final OperatorRuntimeSchemaBuilder schemaBuilder = new OperatorRuntimeSchemaBuilder( 1, 1 );
        schemaBuilder.getOutputPortSchemaBuilder( DEFAULT_PORT_INDEX ).addField( "field2", boolean.class );
        OperatorDefinitionBuilder.newInstance( "op1", PartitionedStatefulOperatorWithExactInputPortSchema.class )
                                 .setExtendingSchema( schemaBuilder.build() )
                                 .build();
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotSetNonExistingPartitionFieldNameInPortSchema ()
    {
        OperatorDefinitionBuilder.newInstance( "op1", PartitionedStatefulOperatorWithExactInputPortSchema.class )
                                 .setPartitionFieldNames( singletonList( "field2" ) );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotSetNonExistingPartitionFieldNamePortSchemas ()
    {
        final OperatorRuntimeSchemaBuilder schemaBuilder = new OperatorRuntimeSchemaBuilder( 1, 1 );
        OperatorDefinitionBuilder.newInstance( "op1", PartitionedStatefulOperatorWithExactInputPortSchema.class )
                                 .setExtendingSchema( schemaBuilder )
                                 .setPartitionFieldNames( singletonList( "field2" ) );
    }

    @Test( expected = IllegalStateException.class )
    public void shouldNotSetPartitionFieldNamesToStatelessOperator ()
    {
        OperatorDefinitionBuilder.newInstance( "op1", StatelessOperatorWithDynamicPortCounts.class )
                                 .setPartitionFieldNames( singletonList( "field1" ) );
    }

    @Test( expected = IllegalStateException.class )
    public void shouldNotSetPartitionFieldNamesToStatefulOperator ()
    {
        OperatorDefinitionBuilder.newInstance( "op1", StatefulOperatorWithFixedPortCounts.class )
                                 .setPartitionFieldNames( singletonList( "field1" ) );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotSetPartitionFieldNameNotExistOnAllInputPorts ()
    {
        OperatorDefinitionBuilder.newInstance( "op1", PartitionedStatefulOperatorWithBaseInputPortSchema.class )
                                 .setPartitionFieldNames( singletonList( "field1" ) );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotSetPartitionFieldNameWithDifferentTypesOnInputPorts ()
    {
        final OperatorRuntimeSchemaBuilder schemaBuilder = new OperatorRuntimeSchemaBuilder( 2, 1 );
        schemaBuilder.getInputPortSchemaBuilder( 1 ).addField( "field1", long.class );
        OperatorDefinitionBuilder.newInstance( "op1", PartitionedStatefulOperatorWithBaseInputPortSchema.class )
                                 .setExtendingSchema( schemaBuilder )
                                 .setPartitionFieldNames( singletonList( "field1" ) );
    }

    @Test
    public void shouldSetPartitionFieldNameExistOnAllInputPorts ()
    {
        final OperatorRuntimeSchemaBuilder schemaBuilder = new OperatorRuntimeSchemaBuilder( 2, 1 );
        schemaBuilder.getInputPortSchemaBuilder( 1 ).addField( "field1", int.class );
        OperatorDefinitionBuilder.newInstance( "op1", PartitionedStatefulOperatorWithBaseInputPortSchema.class )
                                 .setExtendingSchema( schemaBuilder )
                                 .setPartitionFieldNames( singletonList( "field1" ) );
    }

    @Test
    public void shouldSetPartitionFieldNamesToPartitionedStatefulOperator ()
    {
        final List<String> partitionFieldNames = singletonList( "field1" );
        final OperatorDefinition definition = OperatorDefinitionBuilder.newInstance( "op1",
                                                                                     PartitionedStatefulOperatorWithExactInputPortSchema
                                                                                             .class )
                                                                       .setPartitionFieldNames( partitionFieldNames )
                                                                       .build();

        assertTrue( partitionFieldNames.equals( definition.partitionFieldNames ) );
    }

    @Test( expected = IllegalStateException.class )
    public void shouldNotSetInputPortCountToOperatorWithInputPortCountInSpec ()
    {
        OperatorDefinitionBuilder.newInstance( "op1", PartitionedStatefulOperatorWithExactInputPortSchema.class ).setInputPortCount( 1 );
    }

    @Test( expected = IllegalStateException.class )
    public void shouldNotSetOutputPortCountToOperatorWithOutputPortCountInSpec ()
    {
        OperatorDefinitionBuilder.newInstance( "op1", PartitionedStatefulOperatorWithExactInputPortSchema.class ).setOutputPortCount( 1 );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotBuildBuilderForOperatorWithNoInputPortCountButInputSchema ()
    {
        OperatorDefinitionBuilder.newInstance( "op1", OperatorWithNoInputPortCountButInputSchema.class );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotBuildBuilderForOperatorWithNoOutputPortCountButOutputSchema ()
    {
        OperatorDefinitionBuilder.newInstance( "op1", OperatorWithNoOutputPortCountButOutputSchema.class );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotBuildBuilderForOperatorWithDuplicateInputPortSchema ()
    {
        OperatorDefinitionBuilder.newInstance( "op1", OperatorWithDuplicateInputPortSchema.class );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotBuildBuilderForOperatorWithDuplicateOutputPortSchema ()
    {
        OperatorDefinitionBuilder.newInstance( "op1", OperatorWithDuplicateOutputPortSchema.class );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotBuildBuilderForOperatorWithNegativeInputPortSchema ()
    {
        OperatorDefinitionBuilder.newInstance( "op1", OperatorWithNegativeInputPortSchema.class );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotBuildBuilderForOperatorWithNegativeOutputPortSchema ()
    {
        OperatorDefinitionBuilder.newInstance( "op1", OperatorWithNegativeOutputPortSchema.class );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotBuildBuilderForOperatorWithExceedingInputPortSchema ()
    {
        OperatorDefinitionBuilder.newInstance( "op1", OperatorWithExceedingInputPortSchema.class );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotBuildBuilderForOperatorWithExceedingOutputPortSchema ()
    {
        OperatorDefinitionBuilder.newInstance( "op1", OperatorWithExceedingOutputPortSchema.class );
    }

    @Test( expected = IllegalStateException.class )
    public void shouldNotBuildWithExtendedOutputPortSchemaWithoutPartitionFieldNames ()
    {
        final OperatorRuntimeSchemaBuilder schemaBuilder = new OperatorRuntimeSchemaBuilder( 1, 1 );
        schemaBuilder.getOutputPortSchemaBuilder( DEFAULT_PORT_INDEX ).addField( "field3", boolean.class );

        OperatorDefinitionBuilder.newInstance( "op1", PartitionedStatefulOperatorWithExactInputPortSchema.class )
                                 .setExtendingSchema( schemaBuilder.build() )
                                 .build();
    }

    @Test
    public void shouldBuildOperatorDefinitionWithExtendedOutputPortSchema ()
    {
        final OperatorRuntimeSchemaBuilder schemaBuilder = new OperatorRuntimeSchemaBuilder( 1, 1 );
        schemaBuilder.getOutputPortSchemaBuilder( DEFAULT_PORT_INDEX ).addField( "field3", boolean.class );

        final OperatorDefinition definition = OperatorDefinitionBuilder.newInstance( "op1",
                                                                                     PartitionedStatefulOperatorWithExactInputPortSchema
                                                                                             .class )
                                                                       .setExtendingSchema( schemaBuilder.build() )
                                                                       .setPartitionFieldNames( singletonList( "field1" ) )
                                                                       .build();
        final OperatorRuntimeSchema schema = definition.schema;
        assertThat( schema.getOutputSchemas(), hasSize( 1 ) );
        final PortRuntimeSchema outputSchema = schema.getOutputSchema( DEFAULT_PORT_INDEX );
        final List<RuntimeSchemaField> outputFields = outputSchema.getFields();
        assertThat( outputFields, hasSize( 2 ) );
        assertThat( outputFields.get( 0 ), equalTo( new RuntimeSchemaField( "field2", long.class ) ) );
        assertThat( outputFields.get( 1 ), equalTo( new RuntimeSchemaField( "field3", boolean.class ) ) );
    }


    @Test
    public void shouldBuildOperatorDefinitionWithPortSchemaDefinition ()
    {
        final OperatorDefinition definition = OperatorDefinitionBuilder.newInstance( "op1",
                                                                                     PartitionedStatefulOperatorWithExactInputPortSchema
                                                                                             .class )
                                                                       .setPartitionFieldNames( singletonList( "field1" ) )
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

    @Test
    public void shouldBuildEmptyRuntimeSchemaWithNoSchemaDefinition ()
    {
        final OperatorDefinition definition = OperatorDefinitionBuilder.newInstance( "op1", StatefulOperatorWithFixedPortCounts.class )
                                                                       .build();
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

    @OperatorSpec( type = PARTITIONED_STATEFUL, inputPortCount = 1, outputPortCount = 1 )
    @OperatorSchema( inputs = { @PortSchema( portIndex = DEFAULT_PORT_INDEX, scope = EXACT_FIELD_SET, fields = { @SchemaField( name = "field1", type = int.class ) } ) },
            outputs = { @PortSchema( portIndex = DEFAULT_PORT_INDEX, scope = BASE_FIELD_SET, fields = { @SchemaField( name = "field2",
                    type = long.class ) } ) } )
    public static class PartitionedStatefulOperatorWithExactInputPortSchema extends NopOperator
    {
    }


    @OperatorSpec( type = PARTITIONED_STATEFUL, inputPortCount = 2, outputPortCount = 1 )
    @OperatorSchema( inputs = { @PortSchema( portIndex = DEFAULT_PORT_INDEX, scope = BASE_FIELD_SET, fields = { @SchemaField( name = "field1", type = int.class ) } ) },
            outputs = {} )
    public static class PartitionedStatefulOperatorWithBaseInputPortSchema extends NopOperator
    {
    }


    @OperatorSpec( type = STATELESS, outputPortCount = 1 )
    @OperatorSchema( inputs = { @PortSchema( portIndex = DEFAULT_PORT_INDEX, scope = EXACT_FIELD_SET, fields = {} ) },
            outputs = {} )
    public static class OperatorWithNoInputPortCountButInputSchema extends NopOperator
    {
    }


    @OperatorSpec( type = STATELESS, inputPortCount = 1 )
    @OperatorSchema( inputs = {},
            outputs = { @PortSchema( portIndex = DEFAULT_PORT_INDEX, scope = EXACT_FIELD_SET, fields = {} ) } )
    public static class OperatorWithNoOutputPortCountButOutputSchema extends NopOperator
    {
    }


    @OperatorSpec( type = STATELESS, inputPortCount = 1 )
    @OperatorSchema( inputs = { @PortSchema( portIndex = DEFAULT_PORT_INDEX, scope = EXACT_FIELD_SET, fields = {} ),
                                @PortSchema( portIndex = DEFAULT_PORT_INDEX, scope = EXACT_FIELD_SET, fields = {} ) },
            outputs = {} )
    public static class OperatorWithDuplicateInputPortSchema extends NopOperator
    {
    }


    @OperatorSpec( type = STATELESS, outputPortCount = 1 )
    @OperatorSchema( inputs = {},
            outputs = { @PortSchema( portIndex = DEFAULT_PORT_INDEX, scope = EXACT_FIELD_SET, fields = {} ),
                        @PortSchema( portIndex = DEFAULT_PORT_INDEX, scope = EXACT_FIELD_SET, fields = {} ) } )
    public static class OperatorWithDuplicateOutputPortSchema extends NopOperator
    {
    }


    @OperatorSpec( type = STATELESS, inputPortCount = 1 )
    @OperatorSchema( inputs = { @PortSchema( portIndex = -1, scope = EXACT_FIELD_SET, fields = {} ) },
            outputs = {} )
    public static class OperatorWithNegativeInputPortSchema extends NopOperator
    {
    }


    @OperatorSpec( type = STATELESS, outputPortCount = 1 )
    @OperatorSchema( inputs = {},
            outputs = { @PortSchema( portIndex = -1, scope = EXACT_FIELD_SET, fields = {} ) } )
    public static class OperatorWithNegativeOutputPortSchema extends NopOperator
    {
    }


    @OperatorSpec( type = STATELESS, inputPortCount = 1 )
    @OperatorSchema( inputs = { @PortSchema( portIndex = 1, scope = EXACT_FIELD_SET, fields = {} ) },
            outputs = {} )
    public static class OperatorWithExceedingInputPortSchema extends NopOperator
    {
    }


    @OperatorSpec( type = STATELESS, outputPortCount = 1 )
    @OperatorSchema( inputs = {},
            outputs = { @PortSchema( portIndex = 1, scope = EXACT_FIELD_SET, fields = {} ) } )
    public static class OperatorWithExceedingOutputPortSchema extends NopOperator
    {
    }


    @OperatorSpec( type = OperatorType.STATELESS, inputPortCount = 2 )
    public static class StatelessOperatorWithMultipleInputPortCount extends NopOperator
    {

    }


    @OperatorSpec( type = OperatorType.STATELESS, outputPortCount = 2 )
    public static class StatelessOperatorWithMultipleOutputPortCount extends NopOperator
    {

    }


    @OperatorSpec( type = OperatorType.STATELESS, inputPortCount = 1, outputPortCount = 1 )
    public static class StatelessOperatorWithSingleInputOutputPortCount extends NopOperator
    {

    }


    @OperatorSpec( type = OperatorType.STATELESS, inputPortCount = 0, outputPortCount = 1 )
    public static class StatelessOperatorWithZeroInputOutputPortCount extends NopOperator
    {

    }

}
