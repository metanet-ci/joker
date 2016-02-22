package cs.bilkent.zanza.flow;


import java.util.LinkedHashMap;
import java.util.Map;

import com.google.common.base.Strings;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static cs.bilkent.zanza.flow.Port.DEFAULT_PORT_INDEX;
import cs.bilkent.zanza.operator.schema.runtime.OperatorRuntimeSchema;


public class FlowDefinitionBuilder
{

    private final Map<String, OperatorDefinition> operators = new LinkedHashMap<>();

    private final Multimap<Port, Port> connections = HashMultimap.create();

    private boolean built;

    public FlowDefinition build ()
    {
        built = true;
        return new FlowDefinition( operators, connections );
    }

    public FlowDefinitionBuilder add ( final OperatorDefinitionBuilder operatorDefinitionBuilder )
    {
        add( operatorDefinitionBuilder.build() );
        return this;
    }

    public FlowDefinitionBuilder add ( final OperatorDefinition operatorDefinition )
    {
        failIfAlreadyBuilt();
        checkArgument( !operators.containsKey( operatorDefinition.id() ), "only 1 operator can be added with a operator id!" );
        operators.put( operatorDefinition.id(), operatorDefinition );

        return this;
    }

    public FlowDefinitionBuilder connect ( final String sourceOperatorId, final String targetOperatorId )
    {
        return connect( sourceOperatorId, DEFAULT_PORT_INDEX, targetOperatorId, DEFAULT_PORT_INDEX );
    }

    public FlowDefinitionBuilder connect ( final String sourceOperatorId, int sourcePort, final String targetOperatorId )
    {
        return connect( sourceOperatorId, sourcePort, targetOperatorId, DEFAULT_PORT_INDEX );
    }

    public FlowDefinitionBuilder connect ( final String sourceOperatorId, final String targetOperatorId, final int targetPort )
    {
        return connect( sourceOperatorId, DEFAULT_PORT_INDEX, targetOperatorId, targetPort );
    }

    public FlowDefinitionBuilder connect ( final String sourceOperatorId,
                                           final int sourcePort,
                                           final String targetOperatorId,
                                           final int targetPort )
    {
        failIfAlreadyBuilt();
        failIfEmptyOperatorId( sourceOperatorId );
        failIfNonExistingOperatorId( sourceOperatorId );
        failIfInvalidPort( operators.get( sourceOperatorId ).outputPortCount(), sourcePort );
        failIfEmptyOperatorId( targetOperatorId );
        failIfNonExistingOperatorId( targetOperatorId );
        failIfInvalidPort( operators.get( targetOperatorId ).inputPortCount(), targetPort );
        checkArgument( !sourceOperatorId.equals( targetOperatorId ), "operator ids must be different!" );

        final OperatorRuntimeSchema sourceOperatorSchema = operators.get( sourceOperatorId ).schema();
        final OperatorRuntimeSchema targetOperatorSchema = operators.get( targetOperatorId ).schema();
        checkState( sourceOperatorSchema.getOutputSchema( sourcePort )
                                        .isCompatibleWith( targetOperatorSchema.getInputSchema( targetPort ) ) );

        final Port source = new Port( sourceOperatorId, sourcePort );
        final Port target = new Port( targetOperatorId, targetPort );
        connections.put( source, target );
        return this;
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

}
