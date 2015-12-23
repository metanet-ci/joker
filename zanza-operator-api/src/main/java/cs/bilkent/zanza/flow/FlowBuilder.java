package cs.bilkent.zanza.flow;


import java.lang.annotation.Annotation;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static cs.bilkent.zanza.flow.Port.DEFAULT_PORT_INDEX;
import cs.bilkent.zanza.operator.Operator;
import cs.bilkent.zanza.operator.OperatorConfig;
import cs.bilkent.zanza.operator.schema.annotation.OperatorSchema;
import cs.bilkent.zanza.operator.schema.runtime.OperatorRuntimeSchema;
import cs.bilkent.zanza.operator.schema.runtime.PortRuntimeSchema;
import cs.bilkent.zanza.operator.schema.runtime.RuntimeSchemaField;
import cs.bilkent.zanza.operator.spec.OperatorSpec;


public class FlowBuilder
{

    private static final Predicate<Annotation> OPERATOR_SPEC_ANNOTATION_PREDICATE = input -> input instanceof OperatorSpec;

    private final Map<String, OperatorDefinition> operators = new LinkedHashMap<>();

    private final Multimap<Port, Port> connections = HashMultimap.create();

    private boolean built;

    public FlowDefinition build ()
    {
        built = true;
        return new FlowDefinition( operators, connections );
    }

    public FlowBuilder add ( final String operatorId, final Class<? extends Operator> clazz )
    {
        return add( operatorId, clazz, null );
    }

    public FlowBuilder add ( final String operatorId, final Class<? extends Operator> clazz, OperatorConfig config )
    {
        failIfAlreadyBuilt();
        failIfEmptyOperatorId( operatorId );
        checkArgument( !operators.containsKey( operatorId ), "only 1 operator can be added with a operator id!" );
        checkArgument( clazz != null, "Operator class must be provided!" );

        final OperatorSpec spec = getOperatorSpecOrFail( clazz );

        if ( config == null )
        {
            config = new OperatorConfig();
        }

        setPortCounts( config, spec );
        setOperatorRuntimeSchema( clazz, config );

        operators.put( operatorId, new OperatorDefinition( operatorId, clazz, spec.type(), config ) );
        return this;
    }

    public FlowBuilder connect ( final String sourceOperatorId, final String targetOperatorId )
    {
        return connect( sourceOperatorId, DEFAULT_PORT_INDEX, targetOperatorId, DEFAULT_PORT_INDEX );
    }

    public FlowBuilder connect ( final String sourceOperatorId, int sourcePort, final String targetOperatorId )
    {
        return connect( sourceOperatorId, sourcePort, targetOperatorId, DEFAULT_PORT_INDEX );
    }

    public FlowBuilder connect ( final String sourceOperatorId, final String targetOperatorId, final int targetPort )
    {
        return connect( sourceOperatorId, DEFAULT_PORT_INDEX, targetOperatorId, targetPort );
    }

    public FlowBuilder connect ( final String sourceOperatorId, final int sourcePort, final String targetOperatorId, final int targetPort )
    {
        failIfAlreadyBuilt();
        failIfEmptyOperatorId( sourceOperatorId );
        failIfNonExistingOperatorId( sourceOperatorId );
        failIfInvalidPort( operators.get( sourceOperatorId ).config.getOutputPortCount(), sourcePort );
        failIfEmptyOperatorId( targetOperatorId );
        failIfNonExistingOperatorId( targetOperatorId );
        failIfInvalidPort( operators.get( targetOperatorId ).config.getInputPortCount(), targetPort );
        checkArgument( !sourceOperatorId.equals( targetOperatorId ), "operator ids must be different!" );

        final Port source = new Port( sourceOperatorId, sourcePort );
        final Port target = new Port( targetOperatorId, targetPort );
        connections.put( source, target );
        return this;
    }

    public OperatorDefinition getOperator ( final String operatorId )
    {
        checkNotNull( operatorId, "operator id can't be null" );
        return operators.get( operatorId );
    }

    private void setOperatorRuntimeSchema ( Class<? extends Operator> clazz, final OperatorConfig config )
    {
        final OperatorSchema schema = getOperatorSchema( clazz );
        final OperatorRuntimeSchemaBuilder schemaBuilder = new OperatorRuntimeSchemaBuilder( config.getInputPortCount(),
                                                                                             config.getOutputPortCount(),
                                                                                             schema );

        final OperatorRuntimeSchema configSchema = config.getOperatorRuntimeSchema();
        if ( configSchema != null )
        {
            addToSchema( configSchema.getInputSchemas(), schemaBuilder::getInputPortSchemaBuilder );
            addToSchema( configSchema.getOutputSchemas(), schemaBuilder::getOutputPortSchemaBuilder );
        }

        config.setOperatorRuntimeSchema( schemaBuilder.build() );
    }

    private void addToSchema ( final List<PortRuntimeSchema> portSchemas,
                               final Function<Integer, PortRuntimeSchemaBuilder> portSchemaBuilderProvider )
    {
        for ( PortRuntimeSchema portSchema : portSchemas )
        {
            final PortRuntimeSchemaBuilder portSchemaBuilder = portSchemaBuilderProvider.apply( portSchema.getPortIndex() );
            for ( RuntimeSchemaField schemaField : portSchema.getFields() )
            {
                portSchemaBuilder.addField( schemaField.name, schemaField.type );
            }
        }
    }

    private OperatorSpec getOperatorSpecOrFail ( Class<? extends Operator> clazz )
    {
        final OperatorSpec[] annotations = clazz.getDeclaredAnnotationsByType( OperatorSpec.class );
        checkArgument                                                                                             ( annotations.length == 1,
                       clazz + " Operator class must have " + OperatorSpec.class.getSimpleName() + " annotation!" );
        return annotations[ 0 ];
    }

    private OperatorSchema getOperatorSchema ( Class<? extends Operator> clazz )
    {
        final OperatorSchema[] annotations = clazz.getDeclaredAnnotationsByType( OperatorSchema.class );
        checkArgument                                                                                                        ( annotations.length <= 1,
                       clazz + " Operator class can have at most 1 " + OperatorSchema.class.getSimpleName() + " annotation!" );
        return annotations.length > 0 ? annotations[ 0 ] : null;
    }


    private void setPortCounts ( final OperatorConfig config, final OperatorSpec spec )
    {
        if ( spec.inputPortCount() != Port.DYNAMIC_PORT_COUNT )
        {
            failIfNegativePortCount( spec.inputPortCount(), "input" );
            config.setInputPortCount( spec.inputPortCount() );
        }
        else
        {
            failIfNegativePortCount( config.getInputPortCount(), "input" );
        }

        if ( spec.outputPortCount() != Port.DYNAMIC_PORT_COUNT )
        {
            failIfNegativePortCount( spec.outputPortCount(), "input" );
            config.setOutputPortCount( spec.outputPortCount() );
        }
        else
        {
            failIfNegativePortCount( config.getOutputPortCount(), "output" );
        }
    }

    private void failIfEmptyOperatorId ( final String operatorId )
    {
        checkArgument( !Strings.isNullOrEmpty( operatorId ), "operator id must be non-empty!" );
    }

    private void failIfNonExistingOperatorId ( final String operatorId )
    {
        checkArgument( operators.containsKey( operatorId ), "Non-existing operator id!" );
    }

    private void failIfInvalidPort ( final int validPortCount, final int port )
    {
        checkArgument( port >= DEFAULT_PORT_INDEX && port < validPortCount, "Invalid port!" );
    }

    private void failIfAlreadyBuilt ()
    {
        checkState( !built, "Flow already built!" );
    }

    private void failIfNegativePortCount ( final int portCount, final String portType )
    {
        checkArgument( portCount >= 0, portType + " port count in config must be non-negative!" );
    }

}
