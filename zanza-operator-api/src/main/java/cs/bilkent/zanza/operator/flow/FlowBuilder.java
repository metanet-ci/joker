package cs.bilkent.zanza.operator.flow;


import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import cs.bilkent.zanza.operator.Operator;
import cs.bilkent.zanza.operator.OperatorConfig;
import cs.bilkent.zanza.operator.Port;


public class FlowBuilder
{

    private final Map<String, OperatorDefinition> operators = new LinkedHashMap<>();

    private final Set<ConnectionDefinition> connections = new LinkedHashSet<>();

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

    public FlowBuilder add ( final String operatorId, final Class<? extends Operator> clazz, final OperatorConfig config )
    {
        failIfEmptyOperatorId( operatorId );
        Preconditions.checkArgument( clazz != null, "Operator class must be provided!" );
        failIfAlreadyBuilt();

        operators.put( operatorId, new OperatorDefinition( operatorId, clazz, config ) );
        return this;
    }

    public FlowBuilder connect ( final String sourceOperatorId, final String targetOperatorId )
    {
        return connect( sourceOperatorId, Port.DEFAULT_PORT_INDEX, targetOperatorId, Port.DEFAULT_PORT_INDEX );
    }

    public FlowBuilder connect ( final String sourceOperatorId, int sourcePort, final String targetOperatorId )
    {
        return connect( sourceOperatorId, sourcePort, targetOperatorId, Port.DEFAULT_PORT_INDEX );
    }

    public FlowBuilder connect ( final String sourceOperatorId, final String targetOperatorId, final int targetPort )
    {
        return connect( sourceOperatorId, Port.DEFAULT_PORT_INDEX, targetOperatorId, targetPort );
    }

    public FlowBuilder connect ( final String sourceOperatorId, final int sourcePort, final String targetOperatorId, final int targetPort )
    {
        failIfEmptyOperatorId( sourceOperatorId );
        failIfNonExistingOperatorId( sourceOperatorId );
        failIfNegativePort( sourcePort );
        failIfEmptyOperatorId( targetOperatorId );
        failIfNonExistingOperatorId( targetOperatorId );
        failIfNegativePort( targetPort );
        Preconditions.checkArgument( !sourceOperatorId.equals( targetOperatorId ), "operator ids must be different!" );
        failIfAlreadyBuilt();

        final Port source = new Port( sourceOperatorId, sourcePort );
        final Port target = new Port( targetOperatorId, targetPort );
        connections.add( new ConnectionDefinition( source, target ) );
        return this;
    }

    private void failIfEmptyOperatorId ( final String operatorId )
    {
        Preconditions.checkArgument( !Strings.isNullOrEmpty( operatorId ), "operator id must be non-empty!" );
    }

    private void failIfNonExistingOperatorId ( final String operatorId )
    {
        Preconditions.checkArgument( operators.containsKey( operatorId ), "Non-existing operator id!" );
    }

    private void failIfNegativePort ( final int port )
    {
        Preconditions.checkArgument( port >= Port.DEFAULT_PORT_INDEX, "Invalid port!" );
    }

    private void failIfAlreadyBuilt ()
    {
        Preconditions.checkState( !built, "Flow already built!" );
    }
}
