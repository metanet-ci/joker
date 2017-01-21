package cs.bilkent.joker.flow;


import java.util.AbstractMap.SimpleEntry;
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

import static cs.bilkent.joker.com.google.common.base.Preconditions.checkArgument;
import static cs.bilkent.joker.com.google.common.base.Preconditions.checkState;
import cs.bilkent.joker.operator.OperatorDef;
import static java.util.Collections.unmodifiableMap;
import static java.util.stream.Collectors.toSet;


public final class FlowDef
{
    private final Map<String, OperatorDef> operators;

    private final Map<Port, Set<Port>> connections;

    public FlowDef ( final Map<String, OperatorDef> operators, final Map<Port, Set<Port>> connections )
    {
        validateFlowDef( operators, connections );
        this.operators = unmodifiableMap( operators );
        this.connections = unmodifiableMap( connections );
    }

    public OperatorDef getOperator ( final String operatorId )
    {
        return operators.get( operatorId );
    }

    public Collection<OperatorDef> getOperators ()
    {
        return new HashSet<>( operators.values() );
    }

    public Map<String, OperatorDef> getOperatorsMap ()
    {
        return new HashMap<>( operators );
    }

    public Collection<OperatorDef> getOperatorsWithNoInputPorts ()
    {
        final Set<OperatorDef> result = new HashSet<>( operators.values() );
        for ( Entry<Port, Set<Port>> e : connections.entrySet() )
        {
            for ( Port targetPort : e.getValue() )
            {
                final OperatorDef operatorToExclude = operators.get( targetPort.operatorId );
                result.remove( operatorToExclude );
            }
        }

        return result;
    }

    private void validateFlowDef ( final Map<String, OperatorDef> operators, final Map<Port, Set<Port>> connections )
    {
        checkArgument( operators != null, "operators cannot be null" );
        checkArgument( connections != null, "connections cannot be null" );
        checkArgument( !operators.isEmpty(), "there should be at least one operator" );
        failIfMultipleIndependentDAGs( operators, connections );
    }

    private void failIfMultipleIndependentDAGs ( final Map<String, OperatorDef> operators, final Map<Port, Set<Port>> connections )
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
                for ( Entry<Port, Set<Port>> e : connections.entrySet() )
                {
                    for ( Port p : e.getValue() )
                    {
                        if ( e.getKey().operatorId.equals( current ) )
                        {
                            toVisit.add( p.operatorId );
                        }
                        else if ( p.operatorId.equals( current ) )
                        {
                            toVisit.add( e.getKey().operatorId );
                        }
                    }
                }
            }
        }

        checkState( operators.size() == visited.size(), "there are multiple DAGs in the flow!" );
    }

    public Set<Entry<Port, Port>> getConnections ()
    {
        final Set<Entry<Port, Port>> c = new HashSet<>();
        for ( Entry<Port, Set<Port>> e : connections.entrySet() )
        {
            for ( Port p : e.getValue() )
            {
                c.add( new SimpleEntry<>( e.getKey(), p ) );
            }
        }

        return c;
    }

    public Map<Port, Set<Port>> getUpstreamConnections ( final String operatorId )
    {
        final Map<Port, Set<Port>> upstream = new HashMap<>();
        for ( Entry<Port, Set<Port>> e : connections.entrySet() )
        {
            for ( Port p : e.getValue() )
            {
                if ( p.operatorId.equals( operatorId ) )
                {
                    upstream.computeIfAbsent( p, port -> new HashSet<>() ).add( e.getKey() );
                }
            }
        }

        return upstream;
    }

    public Set<Port> getUpstreamConnections ( final Port port )
    {
        return getUpstreamConnectionsStream( port ).collect( toSet() );
    }

    public Map<Port, Set<Port>> getDownstreamConnections ( final String operatorId )
    {
        final Map<Port, Set<Port>> downstream = new HashMap<>();

        for ( Entry<Port, Set<Port>> e : connections.entrySet() )
        {
            final Port upstream = e.getKey();
            if ( upstream.operatorId.equals( operatorId ) )
            {
                downstream.put( upstream, new HashSet<>( e.getValue() ) );
            }
        }

        return downstream;
    }

    public Set<Port> getDownstreamConnections ( final Port port )
    {
        return getDownstreamConnectionsStream( port ).collect( toSet() );
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
        return getConnections().stream().filter( predicate ).map( mapper );
    }

}
