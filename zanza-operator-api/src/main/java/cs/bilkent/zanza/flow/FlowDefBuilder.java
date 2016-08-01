package cs.bilkent.zanza.flow;


import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.google.common.base.Strings;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static cs.bilkent.zanza.flow.Port.DEFAULT_PORT_INDEX;
import cs.bilkent.zanza.operator.schema.runtime.OperatorRuntimeSchema;


public final class FlowDefBuilder
{

    private final Map<String, OperatorDef> operators = new LinkedHashMap<>();

    private final Multimap<Port, Port> connections = HashMultimap.create();

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
        checkArgument( !operators.containsKey( operatorDef.id() ),
                       "only 1 operator can be added with the same operator id: %s",
                       operatorDef.id() );
        operators.put( operatorDef.id(), operatorDef );

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
        failIfInvalidPort( operators.get( sourceOperatorId ).outputPortCount(), sourcePort );
        failIfEmptyOperatorId( destinationOperatorId );
        failIfNonExistingOperatorId( destinationOperatorId );
        failIfInvalidPort( operators.get( destinationOperatorId ).inputPortCount(), destinationPort );
        checkArgument( !sourceOperatorId.equals( destinationOperatorId ), "operator ids must be different!" );
        failIfConnected( destinationOperatorId, sourceOperatorId );
        failIfIncompatibleSchemas( sourceOperatorId, sourcePort, destinationOperatorId, destinationPort );

        final Port source = new Port( sourceOperatorId, sourcePort );
        final Port target = new Port( destinationOperatorId, destinationPort );
        connections.put( source, target );
        return this;
    }

    private void failIfAlreadyBuilt ()
    {
        checkState( !built, "Flow already built!" );
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
                for ( Entry<Port, Port> e : connections.entries() )
                {
                    if ( e.getKey().operatorId.equals( current ) )
                    {
                        toVisit.add( e.getValue().operatorId );
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
        final OperatorRuntimeSchema sourceOperatorSchema = operators.get( sourceOperatorId ).schema();
        final OperatorRuntimeSchema targetOperatorSchema = operators.get( destinationOperatorId ).schema();
        checkState( sourceOperatorSchema.getOutputSchema( sourcePort )
                                        .isCompatibleWith( targetOperatorSchema.getInputSchema( destinationPort ) ),
                    "incompatible schemas between source %s and destination %s",
                    sourcePort,
                    destinationPort );
    }

}
