package cs.bilkent.joker.operator;


import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

import cs.bilkent.joker.flow.Port;
import static cs.bilkent.joker.flow.Port.DYNAMIC_PORT_COUNT;
import static cs.bilkent.joker.impl.com.google.common.base.Preconditions.checkArgument;
import static cs.bilkent.joker.impl.com.google.common.base.Preconditions.checkState;
import cs.bilkent.joker.operator.schema.annotation.OperatorSchema;
import cs.bilkent.joker.operator.schema.annotation.PortSchema;
import cs.bilkent.joker.operator.schema.annotation.SchemaField;
import cs.bilkent.joker.operator.schema.runtime.OperatorRuntimeSchema;
import cs.bilkent.joker.operator.schema.runtime.OperatorRuntimeSchemaBuilder;
import cs.bilkent.joker.operator.schema.runtime.PortRuntimeSchema;
import cs.bilkent.joker.operator.schema.runtime.PortRuntimeSchemaBuilder;
import cs.bilkent.joker.operator.schema.runtime.RuntimeSchemaField;
import cs.bilkent.joker.operator.spec.OperatorSpec;
import cs.bilkent.joker.operator.spec.OperatorType;
import static cs.bilkent.joker.operator.spec.OperatorType.PARTITIONED_STATEFUL;
import static cs.bilkent.joker.operator.spec.OperatorType.STATELESS;
import static java.util.stream.Collectors.toList;


/**
 * Used for defining an operator during flow generation
 */
public final class OperatorDefBuilder
{

    /**
     * Creates the builder object using the given operator id and class
     *
     * @param id
     *         id of the operator to be build
     * @param clazz
     *         class of the operator to be build
     *
     * @return builder to be used for specifying the operator definition
     */
    public static OperatorDefBuilder newInstance ( final String id, final Class<? extends Operator> clazz )
    {
        failIfEmptyOperatorId( id );
        checkArgument( clazz != null, "clazz must be provided" );

        final OperatorSpec spec = getOperatorSpecOrFail( clazz );
        final OperatorSchema schema = getOperatorSchema( clazz );
        if ( schema != null )
        {
            if ( schema.inputs().length > 0 )
            {
                checkArgument( spec.inputPortCount() != DYNAMIC_PORT_COUNT,
                               "input port count must be defined in OperatorSpec if operator is annotated with OperatorSchema" );
                checkArgument( spec.inputPortCount() >= schema.inputs().length,
                               "Number of input port schemas in OperatorSchema exceeds input port count in OperatorSpec" );
            }
            if ( schema.outputs().length > 0 )
            {
                checkArgument( spec.outputPortCount() != DYNAMIC_PORT_COUNT,
                               "output port count must be defined in OperatorSpec if operator is annotated with OperatorSchema" );
                checkArgument( spec.outputPortCount() >= schema.outputs().length,
                               "Number of output port schemas in OperatorSchema exceeds output port count in OperatorSpec" );
            }
            failIfOperatorSchemaHasDuplicatePortIndices( schema );
            failIfInvalidPortIndexOnPortSchemas( spec.inputPortCount(), schema.inputs() );
            failIfInvalidPortIndexOnPortSchemas( spec.outputPortCount(), schema.outputs() );
        }

        if ( spec.inputPortCount() != DYNAMIC_PORT_COUNT )
        {
            failIfInvalidPortCount( spec.type(), spec.inputPortCount(), "input" );
            failIfStatelessOperatorWithMultipleInputPorts( spec.type(), spec.inputPortCount(), "input" );
        }

        if ( spec.outputPortCount() != DYNAMIC_PORT_COUNT )
        {
            failIfInvalidPortCount( spec.type(), spec.outputPortCount(), "output" );
            failIfStatelessOperatorWithNoOutputPorts( spec.type(), spec.outputPortCount(), "output" );
        }

        return new OperatorDefBuilder( id, clazz, spec, schema );
    }

    private static OperatorSpec getOperatorSpecOrFail ( Class<? extends Operator> clazz )
    {
        final OperatorSpec[] annotations = clazz.getDeclaredAnnotationsByType( OperatorSpec.class );
        checkArgument( annotations.length == 1, clazz + " must have " + OperatorSpec.class.getSimpleName() + " annotation!" );
        return annotations[ 0 ];
    }

    private static OperatorSchema getOperatorSchema ( Class<? extends Operator> clazz )
    {
        final OperatorSchema[] annotations = clazz.getDeclaredAnnotationsByType( OperatorSchema.class );
        checkArgument( annotations.length <= 1, clazz + " can have at most 1 " + OperatorSchema.class.getSimpleName() + " annotation!" );
        return annotations.length > 0 ? annotations[ 0 ] : null;
    }

    private static void failIfOperatorSchemaHasDuplicatePortIndices ( final OperatorSchema schema )
    {
        if ( schema != null )
        {
            checkArgument( schema.inputs().length == getPortIndexCount( schema.inputs() ),
                           "There are multiple schemas for some input ports in OperatorSpec " );
            checkArgument( schema.outputs().length == getPortIndexCount( schema.outputs() ),
                           "There are multiple schemas for some output ports in OperatorSpec " );
        }
    }

    private static void failIfInvalidPortIndexOnPortSchemas ( final int portCount, final PortSchema[] portSchemas )
    {
        for ( PortSchema portSchema : portSchemas )
        {
            final int portIndex = portSchema.portIndex();
            checkArgument( portIndex >= 0 && portIndex < portCount, "invalid port index: " + portIndex + " in OperatorSchema" );
        }
    }

    private static void failIfInvalidPortCount ( final OperatorType type, final int portCount, final String portType )
    {
        checkArgument( portCount >= DYNAMIC_PORT_COUNT,
                       "invalid " + portType + " port count: " + portCount + " for " + type + " operator" );
    }

    private static void failIfStatelessOperatorWithMultipleInputPorts ( final OperatorType type,
                                                                        final int portCount,
                                                                        final String portType )
    {
        checkArgument( type != STATELESS || portCount <= 1, STATELESS + " operators can have 1 " + portType + " ports!" );
    }

    private static void failIfStatelessOperatorWithNoOutputPorts ( final OperatorType type, final int portCount, final String portType )
    {
        checkArgument( type != STATELESS || portCount > 0, STATELESS + " operators should have at least 1 " + portType + " ports!" );
    }

    private static int getPortIndexCount ( final PortSchema[] portSchemas )
    {
        return (int) Arrays.stream( portSchemas ).map( PortSchema::portIndex ).distinct().count();
    }


    private final String id;

    private final Class<? extends Operator> clazz;

    private final OperatorType type;

    private int inputPortCount;

    private int outputPortCount;

    private final OperatorSchema schema;

    private OperatorRuntimeSchema extendingSchema;

    private OperatorConfig config;

    private List<String> partitionFieldNames;

    private OperatorDefBuilder ( final String id,
                                 final Class<? extends Operator> clazz,
                                 final OperatorSpec spec,
                                 final OperatorSchema schema )
    {
        this.id = id;
        this.clazz = clazz;
        this.type = spec.type();
        this.inputPortCount = spec.inputPortCount();
        this.outputPortCount = spec.outputPortCount();
        this.schema = schema;
    }

    /**
     * Sets input port count if the operator has {@link Port#DYNAMIC_PORT_COUNT} for its input port count.
     *
     * @param inputPortCount
     *         to be set
     *
     * @return the current builder object
     *
     * @throws IllegalArgumentException
     *         if the method argument is an invalid input port count
     * @throws IllegalStateException
     *         if the input port count is already set
     */
    public OperatorDefBuilder setInputPortCount ( final int inputPortCount )
    {
        checkState( this.inputPortCount == DYNAMIC_PORT_COUNT, "input port count can be set only once" );
        checkArgument( inputPortCount >= 0, "input port count must be non-negative" );
        failIfInvalidPortCount( type, inputPortCount, "input" );
        failIfStatelessOperatorWithMultipleInputPorts( type, inputPortCount, "input" );
        this.inputPortCount = inputPortCount;
        return this;
    }

    /**
     * Sets output port count if the operator has {@link Port#DYNAMIC_PORT_COUNT} for its output port count.
     *
     * @param outputPortCount
     *         to be set
     *
     * @return the current builder object
     *
     * @throws IllegalArgumentException
     *         if the method argument is an invalid output port count
     * @throws IllegalStateException
     *         if the output port count is already set
     */
    public OperatorDefBuilder setOutputPortCount ( final int outputPortCount )
    {
        checkState( this.outputPortCount == DYNAMIC_PORT_COUNT, "output port count can be set only once" );
        checkArgument( outputPortCount >= 0, "output port count must be non-negative" );
        failIfInvalidPortCount( type, outputPortCount, "output" );
        failIfStatelessOperatorWithNoOutputPorts( type, outputPortCount, "output" );
        this.outputPortCount = outputPortCount;
        return this;
    }

    /**
     * Sets config object of the operator. Internally, the config argument is not copied. Therefore, it is recommended to
     * treat the config object as effectively final after provided to this method as an argument.
     *
     * @param config
     *         to be set for the operator
     *
     * @return the current builder object
     *
     * @throws IllegalArgumentException
     *         if the method argument is null
     * @throws IllegalStateException
     *         if the operator config is already set
     */
    public OperatorDefBuilder setConfig ( final OperatorConfig config )
    {
        checkState( this.config == null, "config can be set only once" );
        checkArgument( config != null, "config argument can not be null" );
        this.config = config;
        return this;
    }

    /**
     * Extends runtime schema of the operator by building the extending schema using the {@link OperatorRuntimeSchemaBuilder} argument.
     * It is recommended to treat the schema object as effectively final after provided to this method as an argument.
     * Please see {@link #setExtendingSchema(OperatorRuntimeSchema)} for more information
     *
     * @param extendingSchemaBuilder
     *         the builder object to build the extending schema
     *
     * @return the current builder object
     *
     * @throws IllegalArgumentException
     *         if the method argument is null
     */
    public OperatorDefBuilder setExtendingSchema ( final OperatorRuntimeSchemaBuilder extendingSchemaBuilder )
    {
        checkArgument( extendingSchemaBuilder != null, "extending schema builder can not be null" );
        return setExtendingSchema( extendingSchemaBuilder.build() );
    }

    /**
     * Extends runtime schema of the operator
     *
     * @param extendingSchema
     *         the schema object with field definitions which will be added to the underlying operator's input and output ports
     *
     * @return the current builder object
     *
     * @throws IllegalArgumentException
     *         if one of the following conditions hold:
     *         - the method argument is null
     *         - input port count of the extending schema is different than the underlying operator's input port count
     *         - output port count of the extending schema is different than the underlying operator's output port count
     *         - the extending schema tries to add a field which is already present in the compile-time schema of the underlying operator
     * @throws IllegalStateException
     *         if one of the following conditions hold:
     *         - extending schema is already set
     *         - input port count of the underlying operator is not specified yet
     *         - output port count of the underlying operator is not specified yet
     */
    public OperatorDefBuilder setExtendingSchema ( final OperatorRuntimeSchema extendingSchema )
    {
        checkArgument( extendingSchema != null, "extending schema argument can not be null" );
        checkState( this.extendingSchema == null, "extending schema can be set only once" );
        failIfExtendingSchemaPortSchemaSizesMismatch( extendingSchema.getInputSchemas().size(), this.inputPortCount );
        failIfExtendingSchemaPortSchemaSizesMismatch( extendingSchema.getOutputSchemas().size(), this.outputPortCount );
        if ( this.schema != null )
        {
            for ( int portIndex = 0; portIndex < extendingSchema.getInputPortCount(); portIndex++ )
            {
                failIfExtendingSchemaContainsDuplicateField( portIndex, extendingSchema.getInputSchema( portIndex ), this.schema.inputs() );
            }
            for ( int portIndex = 0; portIndex < extendingSchema.getOutputPortCount(); portIndex++ )
            {
                failIfExtendingSchemaContainsDuplicateField( portIndex,
                                                             extendingSchema.getOutputSchema( portIndex ),
                                                             this.schema.outputs() );
            }
        }
        this.extendingSchema = extendingSchema;
        return this;
    }

    /**
     * Sets the partition field names
     *
     * @param partitionFieldNames
     *         partition field names to be set for {@link OperatorType#PARTITIONED_STATEFUL} operator
     *
     * @return the current builder object
     *
     * @throws IllegalArgumentException
     *         if one of the following conditions hold:
     *         - the method argument is null
     *         - the underlying operator in the builder is {@link OperatorType#PARTITIONED_STATEFUL} and the method argument is null or
     *         empty
     *         - the underlying operator in the builder is not {@link OperatorType#PARTITIONED_STATEFUL} and the method argument is
     *         non-empty
     *         - a partition key field specified in the method argument does not exist in all input ports of the operator, or it exists
     *         with different types on different input ports
     */
    public OperatorDefBuilder setPartitionFieldNames ( final List<String> partitionFieldNames )
    {
        checkArgument( partitionFieldNames != null, "partition field names must be non-null" );
        checkState( ( this.type == PARTITIONED_STATEFUL && !partitionFieldNames.isEmpty() ) || ( this.type != PARTITIONED_STATEFUL
                                                                                                 && partitionFieldNames.isEmpty() ),
                    "partition field names can be only used with " + PARTITIONED_STATEFUL + " operators!" );
        checkState( this.partitionFieldNames == null, "partition key extractor can be set only once" );
        partitionFieldNames.forEach( this::failIfFieldNotExistInInputPortsOrExistWithDifferentTypes );
        this.partitionFieldNames = partitionFieldNames;
        return this;
    }

    /**
     * Builds the {@link OperatorDef} object with the current status of the builder
     *
     * @return {@link OperatorDef} object which is build with the current status of the builder
     *
     * @throws IllegalStateException
     *         if input port count or output port count is not set, or partition field names are not
     *         specified for {@link OperatorType#PARTITIONED_STATEFUL} operator
     */
    public OperatorDef build ()
    {
        checkState( inputPortCount != DYNAMIC_PORT_COUNT, "input port count must be set" );
        checkState( outputPortCount != DYNAMIC_PORT_COUNT, "output port count must be set" );
        checkState( !( type == PARTITIONED_STATEFUL && ( partitionFieldNames == null || partitionFieldNames.isEmpty() ) ),
                    "invalid partition field names" );
        return new OperatorDef( id,
                                clazz,
                                type,
                                inputPortCount,
                                outputPortCount,
                                buildOperatorRuntimeSchema(),
                                getConfigOrEmptyConfig(),
                                partitionFieldNames );
    }

    private void failIfExtendingSchemaPortSchemaSizesMismatch ( final int schemaSize, final int portCount )
    {
        checkState( portCount != DYNAMIC_PORT_COUNT, "port count of operator must be set before extending schema is given" );
        checkArgument( schemaSize <= portCount, "number of port schemas in extending schema exceeds port count of operator" );
    }

    private void failIfExtendingSchemaContainsDuplicateField ( final int portIndex,
                                                               final PortRuntimeSchema runtimeSchema,
                                                               final PortSchema[] portSchemas )
    {
        if ( portSchemas != null )
        {
            final List<String> runtimeSchemaFieldNames = runtimeSchema.getFields().stream().map( field -> field.name ).collect( toList() );
            final boolean duplicate = Arrays.stream( portSchemas )
                                            .filter( portSchema -> portSchema.portIndex() == portIndex )
                                            .flatMap( portSchema -> Arrays.stream( portSchema.fields() ).map( SchemaField::name ) )
                                            .anyMatch( runtimeSchemaFieldNames::contains );
            checkArgument( !duplicate, runtimeSchema + " contains duplicate fields with OperatorSchema" );
        }
    }


    private void failIfFieldNotExistInInputPortsOrExistWithDifferentTypes ( final String fieldName )
    {
        final String errorMessage = "partition field " + fieldName + " must have same type on all input port schemas";
        Class<?> type = null;
        for ( int i = 0; i < inputPortCount; i++ )
        {
            final PortRuntimeSchema portRuntimeSchema = extendingSchema != null ? extendingSchema.getInputSchema( i ) : null;
            if ( portRuntimeSchema != null )
            {
                final RuntimeSchemaField field = portRuntimeSchema.getField( fieldName );
                if ( field != null )
                {
                    if ( type == null )
                    {
                        type = field.type;
                    }
                    else
                    {

                        checkArgument( type.equals( field.type ), errorMessage );
                    }
                }
                else
                {
                    final SchemaField schemaField = getFieldFromInputPortSchemaOrFail( i, fieldName );
                    if ( type == null )
                    {
                        type = schemaField.type();
                    }
                    else
                    {
                        checkArgument( type.equals( schemaField.type() ), errorMessage );
                    }
                }
            }
            else
            {
                final SchemaField schemaField = getFieldFromInputPortSchemaOrFail( i, fieldName );
                if ( type == null )
                {
                    type = schemaField.type();
                }
                else
                {
                    checkArgument( type.equals( schemaField.type() ), errorMessage );
                }
            }
        }
    }

    private SchemaField getFieldFromInputPortSchemaOrFail ( final int portIndex, final String fieldName )
    {
        checkArgument( schema != null );
        for ( PortSchema portSchema : schema.inputs() )
        {
            if ( portSchema.portIndex() == portIndex )
            {
                for ( SchemaField field : portSchema.fields() )
                {
                    if ( field.name().equals( fieldName ) )
                    {
                        return field;
                    }
                }
            }
        }

        throw new IllegalArgumentException( fieldName + " not found in schema of input port: " + portIndex );
    }

    private OperatorConfig getConfigOrEmptyConfig ()
    {
        return config != null ? config : new OperatorConfig();
    }

    private static void failIfEmptyOperatorId ( final String operatorId )
    {
        checkArgument( operatorId != null && operatorId.length() > 0, "operator id must be non-empty!" );
    }

    private OperatorRuntimeSchema buildOperatorRuntimeSchema ()
    {
        final OperatorRuntimeSchemaBuilder schemaBuilder = new OperatorRuntimeSchemaBuilder( inputPortCount, outputPortCount, schema );

        if ( extendingSchema != null )
        {
            addToSchema( extendingSchema.getInputSchemas(), schemaBuilder::getInputPortSchemaBuilder );
            addToSchema( extendingSchema.getOutputSchemas(), schemaBuilder::getOutputPortSchemaBuilder );
        }

        return schemaBuilder.build();
    }

    private void addToSchema ( final List<PortRuntimeSchema> portSchemas,
                               final Function<Integer, PortRuntimeSchemaBuilder> portSchemaBuilderProvider )
    {
        for ( int portIndex = 0; portIndex < portSchemas.size(); portIndex++ )
        {
            final PortRuntimeSchema portSchema = portSchemas.get( portIndex );
            final PortRuntimeSchemaBuilder portSchemaBuilder = portSchemaBuilderProvider.apply( portIndex );
            for ( RuntimeSchemaField schemaField : portSchema.getFields() )
            {
                portSchemaBuilder.addField( schemaField.name, schemaField.type );
            }
        }
    }

}
