package cs.bilkent.joker.flow;


import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import static cs.bilkent.joker.flow.Port.DEFAULT_PORT_INDEX;
import static cs.bilkent.joker.impl.com.google.common.base.Preconditions.checkArgument;
import static cs.bilkent.joker.impl.com.google.common.base.Preconditions.checkState;
import cs.bilkent.joker.operator.OperatorDef;
import cs.bilkent.joker.operator.OperatorDefBuilder;
import cs.bilkent.joker.operator.schema.runtime.OperatorRuntimeSchema;


public final class FlowDefBuilder
{

    private final Map<String, OperatorDef> operators = new LinkedHashMap<>();

    private final Map<Port, Set<Port>> connections = new HashMap<>();

    private boolean built;

    public FlowDef build ()
    {
        built = true;
        return new FlowDef( operators, connections );
    }

    public FlowDefBuilder add ( final OperatorDefBuilder operatorDefBuilder )
    {
        add( operatorDefBuilder.build() );
        return this;
    }

    public FlowDefBuilder add ( final OperatorDef operatorDef )
    {
        failIfAlreadyBuilt();
        checkArgument( !operators.containsKey( operatorDef.getId() ),
                       "only 1 operator can be added with the same operator id: %s", operatorDef.getId() );
        operators.put( operatorDef.getId(), operatorDef );

        return this;
    }

    public FlowDefBuilder connect ( final String sourceOperatorId, final String destinationOperatorId )
    {
        return connect( sourceOperatorId, DEFAULT_PORT_INDEX, destinationOperatorId, DEFAULT_PORT_INDEX );
    }

    public FlowDefBuilder connect ( final String sourceOperatorId, int sourcePort, final String destinationOperatorId )
    {
        return connect( sourceOperatorId, sourcePort, destinationOperatorId, DEFAULT_PORT_INDEX );
    }

    public FlowDefBuilder connect ( final String sourceOperatorId, final String destinationOperatorId, final int destinationPort )
    {
        return connect( sourceOperatorId, DEFAULT_PORT_INDEX, destinationOperatorId, destinationPort );
    }

    public FlowDefBuilder connect ( final String sourceOperatorId,
                                    final int sourcePort,
                                    final String destinationOperatorId,
                                    final int destinationPort )
    {
        failIfAlreadyBuilt();
        failIfEmptyOperatorId( sourceOperatorId );
        failIfNonExistingOperatorId( sourceOperatorId );
        failIfInvalidPort( operators.get( sourceOperatorId ).getOutputPortCount(), sourcePort );
        failIfEmptyOperatorId( destinationOperatorId );
        failIfNonExistingOperatorId( destinationOperatorId );
        failIfInvalidPort( operators.get( destinationOperatorId ).getInputPortCount(), destinationPort );
        checkArgument( !sourceOperatorId.equals( destinationOperatorId ), "operator ids must be different!" );
        failIfConnected( destinationOperatorId, sourceOperatorId );
        failIfIncompatibleSchemas( sourceOperatorId, sourcePort, destinationOperatorId, destinationPort );

        final Port source = new Port( sourceOperatorId, sourcePort );
        final Port target = new Port( destinationOperatorId, destinationPort );
        final Set<Port> downstream = connections.computeIfAbsent( source, port -> new HashSet<>() );
        downstream.add( target );
        return this;
    }

    private void failIfAlreadyBuilt ()
    {
        checkState( !built, "Flow already built!" );
    }

    private void failIfEmptyOperatorId ( final String operatorId )
    {
        checkArgument( operatorId != null && operatorId.length() > 0, "operator id must be non-empty!" );
    }

    private void failIfNonExistingOperatorId ( final String operatorId )
    {
        checkArgument( operators.containsKey( operatorId ), "Non-existing operator id! %s", operatorId );
    }

    private void failIfInvalidPort ( final int validPortCount, final int port )
    {
        checkArgument( port >= DEFAULT_PORT_INDEX && port < validPortCount, "Invalid port!" );
    }

    private void failIfConnected ( final String sourceOperatorId, final String destinationOperatorId )
    {
        final Set<String> visited = new HashSet<>();
        final Set<String> toVisit = new HashSet<>();
        toVisit.add( sourceOperatorId );
        while ( !toVisit.isEmpty() )
        {
            final Iterator<String> it = toVisit.iterator();
            final String current = it.next();
            it.remove();
            checkArgument( !current.equals( destinationOperatorId ),
                           "Cycle detected between %s and %s",
                           sourceOperatorId,
                           destinationOperatorId );
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
                    }
                }
            }
        }
    }

    private void failIfIncompatibleSchemas ( final String sourceOperatorId,
                                             final int sourcePort,
                                             final String destinationOperatorId,
                                             final int destinationPort )
    {
        final OperatorRuntimeSchema sourceOperatorSchema = operators.get( sourceOperatorId ).getSchema();
        final OperatorRuntimeSchema targetOperatorSchema = operators.get( destinationOperatorId ).getSchema();
        checkState( sourceOperatorSchema.getOutputSchema( sourcePort )
                                        .isCompatibleWith( targetOperatorSchema.getInputSchema( destinationPort ) ),
                    "incompatible schemas between source %s and destination %s",
                    sourcePort,
                    destinationPort );
    }

}
