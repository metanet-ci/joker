package cs.bilkent.joker.flow;


import java.util.AbstractMap.SimpleEntry;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Stream;

import static cs.bilkent.joker.impl.com.google.common.base.Preconditions.checkArgument;
import static cs.bilkent.joker.impl.com.google.common.base.Preconditions.checkState;
import cs.bilkent.joker.operator.OperatorDef;
import static java.util.Collections.unmodifiableMap;
import static java.util.Collections.unmodifiableSet;
import static java.util.stream.Collectors.toSet;

/**
 * Represents a data flow graph which contains user-defined operators and connections among them.
 */
public final class FlowDef
{
    private final Map<String, OperatorDef> operators;

    private final Map<Port, Set<Port>> connections;

    public FlowDef ( final Map<String, OperatorDef> operators, final Map<Port, Set<Port>> connections )
    {
        validateFlowDef( operators, connections );
        this.operators = unmodifiableMap( new HashMap<>( operators ) );
        final Map<Port, Set<Port>> c = new HashMap<>();
        for ( Entry<Port, Set<Port>> e : connections.entrySet() )
        {
            c.put( e.getKey(), unmodifiableSet( new HashSet<>( e.getValue() ) ) );
        }
        this.connections = unmodifiableMap( c );
    }

    /**
     * Returns the operator definition given with the operator id
     *
     * @param operatorId
     *         operator id to get the operator definition object
     *
     * @return the operator definition given with the operator id
     */
    public OperatorDef getOperator ( final String operatorId )
    {
        return operators.get( operatorId );
    }

    /**
     * Returns all operator definitions added to the current {@link FlowDef} object
     *
     * @return all operator definitions added to the current {@link FlowDef} object
     */
    public Set<OperatorDef> getOperators ()
    {
        return new HashSet<>( operators.values() );
    }

    /**
     * Returns all operator definitions added to the current {@link FlowDef} object
     *
     * @return all operator definitions added to the current {@link FlowDef} object
     */
    public Map<String, OperatorDef> getOperatorsMap ()
    {
        return new HashMap<>( operators );
    }

    /**
     * Returns the set of operators with no active inbound connection
     *
     * @return the set of operators with no active inbound connection
     */
    public Set<OperatorDef> getSourceOperators ()
    {
        final Set<String> nonSourceOperators = connections.values()
                                                          .stream()
                                                          .flatMap( ports -> ports.stream().map( Port::getOperatorId ) )
                                                          .collect( toSet() );

        return operators.values().stream().filter( o -> !nonSourceOperators.contains( o.getId() ) ).collect( toSet() );
    }

    private void validateFlowDef ( final Map<String, OperatorDef> operators, final Map<Port, Set<Port>> connections )
    {
        checkArgument( operators != null, "operators cannot be null" );
        checkArgument( connections != null, "connections cannot be null" );
        checkArgument( !operators.isEmpty(), "there should be at least one operator" );
        failIfMultipleIndependentDAGs( operators, connections );
        failIfNoInboundConnectionForTailOperators( operators, connections );
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
                        if ( e.getKey().getOperatorId().equals( current ) )
                        {
                            toVisit.add( p.getOperatorId() );
                        }
                        else if ( p.getOperatorId().equals( current ) )
                        {
                            toVisit.add( e.getKey().getOperatorId() );
                        }
                    }
                }
            }
        }

        checkState( operators.size() == visited.size(), "there are multiple DAGs in the flow!" );
    }

    private void failIfNoInboundConnectionForTailOperators ( final Map<String, OperatorDef> operators,
                                                             final Map<Port, Set<Port>> connections )
    {
        for ( OperatorDef operator : operators.values() )
        {
            checkState( checkInboundConnection( connections, operator ),
                        "Operator: %s with input port count: %s has no inbound connection!",
                        operator.getId(),
                        operator.getInputPortCount() );
        }
    }

    private boolean checkInboundConnection ( final Map<Port, Set<Port>> connections, final OperatorDef operator )
    {
        if ( operator.getInputPortCount() == 0 )
        {
            return true;
        }

        for ( Set<Port> c : connections.values() )
        {
            for ( Port p : c )
            {
                if ( p.getOperatorId().equals( operator.getId() ) )
                {
                    return true;
                }
            }
        }

        return false;
    }

    /**
     * Returns all connections added to the current {@link FlowDef} object. Each connection is represented with a source and destination
     * port. Each port contains an operator id and a port index.
     *
     * @return all connections added to the current {@link FlowDef} object.
     */
    public Set<Entry<Port, Port>> getConnections ()
    {
        final Function<Entry<Port, Set<Port>>, Stream<Entry<Port, Port>>> flatMapper = e -> {
            final Function<Port, Entry<Port, Port>> mapper = k -> new SimpleEntry<>( e.getKey(), k );
            return e.getValue().stream().map( mapper );
        };

        return connections.entrySet().stream().flatMap( flatMapper ).collect( toSet() );
    }

    /**
     * Returns all inbound connections of the given operator. Keys are input ports of the given operator, which are destinations of the
     * connections, and values are sources of the inbound connections.
     *
     * @param operatorId
     *         operator id to get the inbound connections
     *
     * @return all inbound connections for the given operator. Keys are input ports of the given operator, which are destinations of the
     * connections, and values are sources of the inbound connections.
     */
    public Map<Port, Set<Port>> getInboundConnections ( final String operatorId )
    {
        final Map<Port, Set<Port>> upstream = new HashMap<>();
        for ( Entry<Port, Set<Port>> e : connections.entrySet() )
        {
            final Port source = e.getKey();
            for ( Port destination : e.getValue() )
            {
                if ( destination.getOperatorId().equals( operatorId ) )
                {
                    upstream.computeIfAbsent( destination, port -> new HashSet<>() ).add( source );
                }
            }
        }

        return upstream;
    }

    /**
     * Returns sources of all inbound connections for the given operator input port.
     *
     * @param port
     *         operator port to get inbound connections
     *
     * @return sources of all inbound connections for the given operator input port.
     */
    public Set<Port> getInboundConnections ( final Port port )
    {
        return getConnections().stream().filter( entry -> entry.getValue().equals( port ) ).map( Entry::getKey ).collect( toSet() );
    }

    /**
     * Returns true if the given operator id has no inbound connection
     *
     * @param operatorId
     *         opererator id to check
     *
     * @return true if the given operator id has no inbound connection
     *
     * @throws IllegalArgumentException
     *         if the operator id is not present in the flow
     */
    public boolean isSourceOperator ( final String operatorId )
    {
        checkArgument( operators.containsKey( operatorId ) );
        return getInboundConnections( operatorId ).isEmpty();
    }

    /**
     * Returns all outbound connections of the given operator. Keys are output ports of the given operator, which are sources of the
     * connections, and values are destinations of the outbound connections.
     *
     * @param operatorId
     *         operator id to get the outbound connections
     *
     * @return all outbound connections of the given operator. Keys are output ports of the given operator, which are sources of the
     * connections, and values are destinations of the outbound connections.
     */
    public Map<Port, Set<Port>> getOutboundConnections ( final String operatorId )
    {
        final Map<Port, Set<Port>> downstream = new HashMap<>();

        for ( Entry<Port, Set<Port>> e : connections.entrySet() )
        {
            final Port upstream = e.getKey();
            if ( upstream.getOperatorId().equals( operatorId ) )
            {
                downstream.put( upstream, new HashSet<>( e.getValue() ) );
            }
        }

        return downstream;
    }

    /**
     * Returns destinations of all outbound connections for the given operator output port.
     *
     * @param port
     *         operator port to get outbound connections
     *
     * @return destinations of all outbound connections for the given operator output port.
     */
    public Set<Port> getOutboundConnections ( final Port port )
    {
        return new HashSet<>( connections.get( port ) );
    }

    @Override
    public boolean equals ( final Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }

        final FlowDef flowDef = (FlowDef) o;

        return operators.equals( flowDef.operators ) && connections.equals( flowDef.connections );
    }

    @Override
    public int hashCode ()
    {
        int result = operators.hashCode();
        result = 31 * result + connections.hashCode();
        return result;
    }

}
