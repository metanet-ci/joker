package cs.bilkent.zanza.flow;


import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
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

        if ( operators.size() == 1 )
        {
            checkState( connectedOperatorCount == 0, "Invalid flow definition!" );
        }
        else
        {
            checkState( operators.size() == connectedOperatorCount, "Invalid flow definition!" );
        }
    }

    public Collection<Entry<Port, Port>> getAllConnections ()
    {
        return connections.entries();
    }

    public Map<Port, Collection<Port>> getUpstreamConnections ( final String operatorId )
    {
        final Map<Port, Collection<Port>> upstream = new HashMap<>();
        for ( Entry<Port, Port> e : connections.entries() )
        {
            if ( e.getValue().operatorId.equals( operatorId ) )
            {
                upstream.computeIfAbsent( e.getValue(), port -> new ArrayList<>() ).add( e.getKey() );
            }
        }

        return upstream;
    }

    public Collection<Port> getUpstreamConnections ( final Port port )
    {
        return getUpstreamConnectionsStream( port ).collect( Collectors.toList() );
    }

    public Map<Port, Collection<Port>> getDownstreamConnections ( final String operatorId )
    {
        final Map<Port, Collection<Port>> downstream = new HashMap<>();

        for ( Port upstream : connections.keySet() )
        {
            if ( upstream.operatorId.equals( operatorId ) )
            {
                downstream.put( upstream, new ArrayList<>( connections.get( upstream ) ) );
            }
        }

        return downstream;
    }

    public Collection<Port> getDownstreamConnections ( final Port port )
    {
        return getDownstreamConnectionsStream( port ).collect( Collectors.toList() );
    }

    private Stream<Port> getUpstreamConnectionsStream ( final Port port )
    {
        return getConnectionsStream( entry -> entry.getValue().equals( port ), Entry::getKey );
    }

    private Stream<Port> getDownstreamConnectionsStream ( final Port port )
    {
        return connections.get( port ).stream();
    }

    private Stream<Port> getConnectionsStream ( final Predicate<Entry<Port, Port>> predicate,
                                                final Function<Entry<Port, Port>, Port> mapper )
    {
        return connections.entries().stream().filter( predicate ).map( mapper );
    }

}
