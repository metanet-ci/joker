package cs.bilkent.joker.engine.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import cs.bilkent.joker.engine.region.RegionDef;
import cs.bilkent.joker.flow.Port;
import cs.bilkent.joker.operator.OperatorDef;

public class RegionUtil
{

    public static List<RegionDef> sortTopologically ( final Map<String, OperatorDef> operators,
                                                      final Collection<Entry<Port, Port>> connectionsMap,
                                                      final List<RegionDef> regions )
    {
        final List<RegionDef> sorted = new ArrayList<>();
        final Collection<RegionDef> curr = new ArrayList<>();
        final Collection<Entry<Port, Port>> connections = new ArrayList<>( connectionsMap );

        for ( OperatorDef o : getOperatorsWithNoInputPorts( operators, connectionsMap ) )
        {
            curr.add( getRegionByFirstOperator( regions, o ) );
        }

        while ( curr.size() > 0 )
        {
            final Iterator<RegionDef> it1 = curr.iterator();
            final RegionDef region = it1.next();
            it1.remove();

            sorted.add( region );

            final String lastOperatorId = getLastOperator( region ).id();

            final Iterator<Entry<Port, Port>> it2 = connections.iterator();
            while ( it2.hasNext() )
            {
                final Entry<Port, Port> e = it2.next();
                if ( !e.getKey().operatorId.equals( lastOperatorId ) )
                {
                    continue;
                }

                it2.remove();

                final String downstreamOperatorId = e.getValue().operatorId;
                if ( !checkIfIncomingConnectionExists( connections, downstreamOperatorId ) )
                {
                    curr.add( getRegionByFirstOperator( regions, operators.get( downstreamOperatorId ) ) );
                }
            }
        }

        return sorted;
    }

    private static Collection<OperatorDef> getOperatorsWithNoInputPorts ( final Map<String, OperatorDef> operators,
                                                                          final Collection<Entry<Port, Port>> connections )
    {
        final Set<OperatorDef> result = new HashSet<>( operators.values() );
        for ( Entry<Port, Port> e : connections )
        {
            final OperatorDef operatorToExclude = operators.get( e.getValue().operatorId );
            result.remove( operatorToExclude );
        }

        return result;
    }

    private static boolean checkIfIncomingConnectionExists ( final Collection<Entry<Port, Port>> connections, final String operatorId )
    {
        for ( Entry<Port, Port> e : connections )
        {
            if ( e.getValue().operatorId.equals( operatorId ) )
            {
                return true;
            }
        }

        return false;
    }

    public static RegionDef getRegionByLastOperator ( final List<RegionDef> regions, final OperatorDef operator )
    {
        for ( RegionDef region : regions )
        {
            if ( getLastOperator( region ).equals( operator ) )
            {
                return region;
            }
        }

        throw new IllegalStateException( "No region found for operator " + operator.id() );
    }

    public static RegionDef getRegionByFirstOperator ( final List<RegionDef> regions, final OperatorDef operator )
    {
        for ( RegionDef region : regions )
        {
            if ( getFirstOperator( region ).equals( operator ) )
            {
                return region;
            }
        }

        throw new IllegalStateException( "No region found for operator " + operator.id() );
    }

    public static OperatorDef getFirstOperator ( final RegionDef regionDef )
    {
        return regionDef.getOperators().get( 0 );
    }

    public static OperatorDef getLastOperator ( final RegionDef regionDef )
    {
        return regionDef.getOperators().get( regionDef.getOperatorCount() - 1 );
    }

}
