package cs.bilkent.joker.flow;


import java.util.Arrays;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import cs.bilkent.joker.operator.schema.annotation.OperatorSchema;
import cs.bilkent.joker.operator.schema.annotation.PortSchema;
import cs.bilkent.joker.operator.schema.runtime.OperatorRuntimeSchema;
import cs.bilkent.joker.operator.schema.runtime.PortRuntimeSchema;
import cs.bilkent.joker.operator.schema.runtime.RuntimeSchemaField;
import static java.util.stream.Collectors.toList;


public final class OperatorRuntimeSchemaBuilder
{

    private final PortRuntimeSchemaBuilder[] inputSchemaBuilders;

    private final PortRuntimeSchemaBuilder[] outputSchemaBuilders;

    public OperatorRuntimeSchemaBuilder ( final int inputPortCount, final int outputPortCount )
    {
        checkArgument( inputPortCount >= 0, "input port count must be non-negative" );
        checkArgument( outputPortCount >= 0, "output port count must be non-negative" );
        this.inputSchemaBuilders = new PortRuntimeSchemaBuilder[ inputPortCount ];
        this.outputSchemaBuilders = new PortRuntimeSchemaBuilder[ outputPortCount ];
    }

    OperatorRuntimeSchemaBuilder ( final int inputPortCount, final int outputPortCount, final OperatorSchema operatorSchema )
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

    private void addToBuilder ( final PortRuntimeSchemaBuilder[] builders, final PortSchema[] portSchemas )
    {
        for ( PortSchema portSchema : portSchemas )
        {
            final int portIndex = portSchema.portIndex();
            checkArgument( builders[ portIndex ] == null, "port index: " + portIndex + " has multiple schemas!" );
            final List<RuntimeSchemaField> fields = Arrays.stream( portSchema.fields() )
                                                          .map( f -> new RuntimeSchemaField( f.name(), f.type() ) )
                                                          .collect( toList() );
            final PortRuntimeSchemaBuilder builder = new PortRuntimeSchemaBuilder( portIndex, portSchema.scope(), fields );
            builders[ portIndex ] = builder;
        }
    }

    public PortRuntimeSchemaBuilder getInputPortSchemaBuilder ( final int portIndex )
    {
        return getOrCreate( inputSchemaBuilders, portIndex );
    }

    public PortRuntimeSchemaBuilder getOutputPortSchemaBuilder ( final int portIndex )
    {
        return getOrCreate( outputSchemaBuilders, portIndex );
    }

    public OperatorRuntimeSchemaBuilder addInputField ( final int portIndex, final String fieldName, final Class<?> type )
    {
        getOrCreate( inputSchemaBuilders, portIndex ).addField( fieldName, type );
        return this;
    }

    public OperatorRuntimeSchemaBuilder addOutputField ( final int portIndex, final String fieldName, final Class<?> type )
    {
        getOrCreate( outputSchemaBuilders, portIndex ).addField( fieldName, type );
        return this;
    }

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

    private PortRuntimeSchemaBuilder getOrCreate ( final PortRuntimeSchemaBuilder[] builders, final int portIndex )
    {
        checkArgument( portIndex >= 0 && portIndex < builders.length,
                       "invalid port index! input port count: " + builders.length + " port index: " + portIndex );
        PortRuntimeSchemaBuilder builder = builders[ portIndex ];
        if ( builder == null )
        {
            builder = new PortRuntimeSchemaBuilder( portIndex );
            builders[ portIndex ] = builder;
        }

        return builder;
    }

}
