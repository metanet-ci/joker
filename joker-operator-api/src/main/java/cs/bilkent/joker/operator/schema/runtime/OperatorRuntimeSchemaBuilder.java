package cs.bilkent.joker.operator.schema.runtime;


import java.util.Arrays;
import java.util.List;

import static cs.bilkent.joker.impl.com.google.common.base.Preconditions.checkArgument;
import cs.bilkent.joker.operator.schema.annotation.OperatorSchema;
import cs.bilkent.joker.operator.schema.annotation.PortSchema;
import static java.util.stream.Collectors.toList;

/**
 * Builder for {@link OperatorRuntimeSchema}
 */
public final class OperatorRuntimeSchemaBuilder
{

    private final PortRuntimeSchemaBuilder[] inputSchemaBuilders;

    private final PortRuntimeSchemaBuilder[] outputSchemaBuilders;

    /**
     * Creates a runtime schema builder with no design time schema
     *
     * @param inputPortCount
     *         input port count of the operator
     * @param outputPortCount
     *         output port count of the operator
     */
    public OperatorRuntimeSchemaBuilder ( final int inputPortCount, final int outputPortCount )
    {
        checkArgument( inputPortCount >= 0, "input port count must be non-negative" );
        checkArgument( outputPortCount >= 0, "output port count must be non-negative" );
        this.inputSchemaBuilders = new PortRuntimeSchemaBuilder[ inputPortCount ];
        this.outputSchemaBuilders = new PortRuntimeSchemaBuilder[ outputPortCount ];
    }

    /**
     * Creates a runtime schema builder which is initiated with a design time schema
     *
     * @param inputPortCount
     *         input port count of the operator
     * @param outputPortCount
     *         output port count of the operator
     * @param operatorSchema
     *         design time schema of the operator
     */
    public OperatorRuntimeSchemaBuilder ( final int inputPortCount, final int outputPortCount, final OperatorSchema operatorSchema )
    {
        checkArgument( inputPortCount >= 0, "input port count must be non-negative" );
        checkArgument( outputPortCount >= 0, "output port count must be non-negative" );
        this.inputSchemaBuilders = new PortRuntimeSchemaBuilder[ inputPortCount ];
        this.outputSchemaBuilders = new PortRuntimeSchemaBuilder[ outputPortCount ];
        if ( operatorSchema != null )
        {
            addToBuilder( inputSchemaBuilders, operatorSchema.inputs() );
            addToBuilder( outputSchemaBuilders, operatorSchema.outputs() );
        }
    }

    /**
     * Returns a builder object to be used for extending schema of the given input port
     *
     * @param portIndex
     *         input port of the operator
     *
     * @return the builder object for schema of the input port
     */
    public PortRuntimeSchemaBuilder getInputPortSchemaBuilder ( final int portIndex )
    {
        return getOrCreate( inputSchemaBuilders, portIndex );
    }

    /**
     * Returns a builder object to be used for extending schema of the given output port
     *
     * @param portIndex
     *         output port of the operator
     *
     * @return the builder object for schema of the output port
     */
    public PortRuntimeSchemaBuilder getOutputPortSchemaBuilder ( final int portIndex )
    {
        return getOrCreate( outputSchemaBuilders, portIndex );
    }

    /**
     * Add a new field to schema of the given input port
     *
     * @param portIndex
     *         input port to add the field
     * @param fieldName
     *         name of the field
     * @param type
     *         type of the field
     *
     * @return the current builder object
     */
    public OperatorRuntimeSchemaBuilder addInputField ( final int portIndex, final String fieldName, final Class<?> type )
    {
        getOrCreate( inputSchemaBuilders, portIndex ).addField( fieldName, type );
        return this;
    }

    /**
     * Add a new field to schema of the given output port
     *
     * @param portIndex
     *         output port to add the field
     * @param fieldName
     *         name of the field
     * @param type
     *         type of the field
     *
     * @return the current builder object
     */
    public OperatorRuntimeSchemaBuilder addOutputField ( final int portIndex, final String fieldName, final Class<?> type )
    {
        getOrCreate( outputSchemaBuilders, portIndex ).addField( fieldName, type );
        return this;
    }

    /**
     * Builds the {@link OperatorRuntimeSchema} object for the operator using the field definitions given for input and output ports
     *
     * @return the {@link OperatorRuntimeSchema} object built for the operator using the field definitions given for input and output ports
     */
    public OperatorRuntimeSchema build ()
    {
        final PortRuntimeSchema[] inputSchemas = new PortRuntimeSchema[ inputSchemaBuilders.length ];
        final PortRuntimeSchema[] outputSchemas = new PortRuntimeSchema[ outputSchemaBuilders.length ];

        for ( int i = 0; i < inputSchemaBuilders.length; i++ )
        {
            final PortRuntimeSchemaBuilder builder = getOrCreate( inputSchemaBuilders, i );
            inputSchemas[ i ] = builder.build();
        }

        for ( int i = 0; i < outputSchemaBuilders.length; i++ )
        {
            final PortRuntimeSchemaBuilder builder = getOrCreate( outputSchemaBuilders, i );
            outputSchemas[ i ] = builder.build();
        }

        return new OperatorRuntimeSchema( Arrays.asList( inputSchemas ), Arrays.asList( outputSchemas ) );
    }

    private void addToBuilder ( final PortRuntimeSchemaBuilder[] builders, final PortSchema[] portSchemas )
    {
        for ( PortSchema portSchema : portSchemas )
        {
            final int portIndex = portSchema.portIndex();
            checkArgument( builders[ portIndex ] == null, "port index: %s has multiple schemas!", portIndex );
            final List<RuntimeSchemaField> fields = Arrays.stream( portSchema.fields() )
                                                          .map( f -> new RuntimeSchemaField( f.name(), f.type() ) )
                                                          .collect( toList() );
            final PortRuntimeSchemaBuilder builder = new PortRuntimeSchemaBuilder( portSchema.scope(), fields );
            builders[ portIndex ] = builder;
        }
    }

    private PortRuntimeSchemaBuilder getOrCreate ( final PortRuntimeSchemaBuilder[] builders, final int portIndex )
    {
        checkArgument( portIndex >= 0 && portIndex < builders.length,
                       "invalid port index! input port count: %s port index: %s",
                       builders.length,
                       portIndex );
        PortRuntimeSchemaBuilder builder = builders[ portIndex ];
        if ( builder == null )
        {
            builder = new PortRuntimeSchemaBuilder();
            builders[ portIndex ] = builder;
        }

        return builder;
    }

}
