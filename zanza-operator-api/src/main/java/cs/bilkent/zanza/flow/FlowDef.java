package cs.bilkent.zanza.flow;


import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Multimaps.unmodifiableMultimap;
import static java.util.Collections.unmodifiableMap;
import static java.util.stream.Collectors.toList;


public final class FlowDef
{
    private final Map<String, OperatorDef> operators;

    private final Multimap<Port, Port> connections;

    public FlowDef ( final Map<String, OperatorDef> operators, final Multimap<Port, Port> connections )
    {
        validateFlowDef( operators, connections );
        this.operators = unmodifiableMap( operators );
        this.connections = unmodifiableMultimap( connections );
    }

    public OperatorDef getOperator ( final String operatorId )
    {
        return operators.get( operatorId );
    }

    public Collection<OperatorDef> getOperators ()
    {
        return new ArrayList<>( operators.values() );
    }

    public Map<String, OperatorDef> getOperatorsMap ()
    {
        return new HashMap<>( operators );
    }

    public Collection<OperatorDef> getOperatorsWithNoInputPorts ()
    {
        final Set<OperatorDef> result = new HashSet<>( operators.values() );
        for ( Port targetPort : connections.values() )
        {
            final OperatorDef operatorToExclude = operators.get( targetPort.operatorId );
            result.remove( operatorToExclude );
        }

        return result;
    }

    private void validateFlowDef ( final Map<String, OperatorDef> operators, final Multimap<Port, Port> connections )
    {
        checkArgument( operators != null, "operators cannot be null" );
        checkArgument( connections != null, "connections cannot be null" );
        checkArgument( !operators.isEmpty(), "there should be at least one operator" );
        failIfMultipleIndependentDAGs( operators, connections );
    }

    private void failIfMultipleIndependentDAGs ( final Map<String, OperatorDef> operators, final Multimap<Port, Port> connections )
    {
        final Set<String> visited = new HashSet<>();
        final Set<String> toVisit = new HashSet<>();
        toVisit.add( operators.keySet().iterator().next() );
        while ( !toVisit.isEmpty() )
        {
            final Iterator<String> it = toVisit.iterator();
            final String current = it.next();
            it.remove();

            if ( visited.add( current ) )
            {
                for ( Entry<Port, Port> e : connections.entries() )
                {
                    if ( e.getKey().operatorId.equals( current ) )
                    {
                        toVisit.add( e.getValue().operatorId );
                    }
                    else if ( e.getValue().operatorId.equals( current ) )
                    {
                        toVisit.add( e.getKey().operatorId );
                    }
                }
            }
        }

        checkState( operators.size() == visited.size(), "there are multiple DAGs in the flow!" );
    }

    public Collection<Entry<Port, Port>> getAllConnections ()
    {
        return new ArrayList<>( connections.entries() );
    }

    public Multimap<Port, Port> getConnectionsMap ()
    {
        return HashMultimap.create( connections );
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
        return getUpstreamConnectionsStream( port ).collect( toList() );
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
        return getDownstreamConnectionsStream( port ).collect( toList() );
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
