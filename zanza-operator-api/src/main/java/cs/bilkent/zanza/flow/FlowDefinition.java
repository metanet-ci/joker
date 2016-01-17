package cs.bilkent.zanza.flow;


import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.Multimap;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Multimaps.unmodifiableMultimap;
import static java.util.Collections.unmodifiableMap;


public class FlowDefinition
{
    private final Map<String, OperatorDefinition> operators;

    private final Multimap<Port, Port> connections;

    public FlowDefinition ( final Map<String, OperatorDefinition> operators, final Multimap<Port, Port> connections )
    {
        validateFlowDefinition( operators, connections );
        this.operators = unmodifiableMap( operators );
        this.connections = unmodifiableMultimap( connections );
    }

    public OperatorDefinition getOperator ( final String operatorId )
    {
        return operators.get( operatorId );
    }

    public Collection<OperatorDefinition> getOperators ()
    {
        return operators.values();
    }

    public Collection<OperatorDefinition> getOperatorsWithNoInputPorts ()
    {
        final Set<OperatorDefinition> result = new HashSet<>( operators.values() );
        for ( Port targetPort : connections.values() )
        {
            final OperatorDefinition operatorToExclude = operators.get( targetPort.operatorId );
            result.remove( operatorToExclude );
        }

        return result;
    }

    // TODO improve flow validation
    private void validateFlowDefinition ( final Map<String, OperatorDefinition> operators, final Multimap<Port, Port> connections )
    {
        checkNotNull( operators );
        checkNotNull( connections );
        checkAllOperatorsHaveConnection( operators, connections );
    }

    private void checkAllOperatorsHaveConnection ( final Map<String, OperatorDefinition> operators, final Multimap<Port, Port> connections )
    {
        final long connectedOperatorCount = connections.entries()
                                                       .stream()
                                                       .flatMap( entry -> Stream.of( entry.getKey().operatorId,
                                                                                     entry.getValue().operatorId ) )
                                                       .distinct()
                                                       .count();
        checkState( operators.size() == connectedOperatorCount, "Invalid flow definition!" );
    }

    public Collection<Map.Entry<Port, Port>> getAllConnections ()
    {
        return connections.entries();
    }

    public Collection<Port> getUpstreamPorts ( final String operatorId )
    {
        return getUpstreamPortsStream( operatorId ).collect( Collectors.toList() );
    }

    public Collection<Port> getDownstreamPorts ( final String operatorId )
    {
        return getDownstreamPortsStream( operatorId ).collect( Collectors.toList() );
    }

    public Collection<OperatorDefinition> getDownstreamOperators ( final String operatorId )
    {
        return getDownstreamPortsStream( operatorId ).map( port -> getOperator( port.operatorId ) ).collect( Collectors.toList() );
    }

    private Stream<Port> getUpstreamPortsStream ( final String operatorId )
    {
        return connections.entries().stream().filter( entry -> entry.getValue().operatorId.equals( operatorId ) ).map( Entry::getKey );
    }

    private Stream<Port> getDownstreamPortsStream ( final String operatorId )
    {
        return connections.entries().stream().filter( entry -> entry.getKey().operatorId.equals( operatorId ) ).map( Entry::getValue );
    }

}
