package cs.bilkent.joker.engine.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import cs.bilkent.joker.engine.flow.RegionDef;
import cs.bilkent.joker.flow.Port;
import cs.bilkent.joker.operator.OperatorDef;
import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.toList;

public class RegionUtil
{

    public static List<RegionDef> sortTopologically ( final Map<String, OperatorDef> operators,
                                                      final Collection<Entry<Port, Port>> connections,
                                                      final List<RegionDef> regions )
    {
        final List<RegionDef> sorted = new ArrayList<>();
        final List<RegionDef> curr = new ArrayList<>();
        final Collection<Entry<Port, Port>> connectionsCopy = new HashSet<>( connections );

        for ( OperatorDef o : getOperatorsWithNoInputPorts( operators, connections ) )
        {
            curr.add( getRegionByFirstOperator( regions, o.getId() ) );
        }

        while ( curr.size() > 0 )
        {
            final RegionDef region = curr.remove( 0 );
            sorted.add( region );

            final String lastOperatorId = getLastOperator( region ).getId();

            final List<Entry<Port, Port>> downstreamConnections = connectionsCopy.stream()
                                                                                 .filter( e -> e.getKey()
                                                                                                .getOperatorId()
                                                                                                .equals( lastOperatorId ) )
                                                                                 .sorted( comparing( e -> e.getValue().getOperatorId() ) )
                                                                                 .collect( toList() );

            for ( Entry<Port, Port> e : downstreamConnections )
            {
                connectionsCopy.remove( e );

                final String downstreamOperatorId = e.getValue().getOperatorId();
                if ( !checkIfIncomingConnectionExists( connectionsCopy, downstreamOperatorId ) )
                {
                    curr.add( getRegionByFirstOperator( regions, downstreamOperatorId ) );
                }
            }
        }

        return sorted;
    }

    private static Collection<OperatorDef> getOperatorsWithNoInputPorts ( final Map<String, OperatorDef> operators,
                                                                          final Collection<Entry<Port, Port>> connections )
    {
        final List<OperatorDef> result = new ArrayList<>( operators.values() );
        for ( Entry<Port, Port> e : connections )
        {
            final OperatorDef operatorToExclude = operators.get( e.getValue().getOperatorId() );
            result.remove( operatorToExclude );
        }

        result.sort( comparing( OperatorDef::getId ) );

        return result;
    }

    private static boolean checkIfIncomingConnectionExists ( final Collection<Entry<Port, Port>> connections, final String operatorId )
    {
        for ( Entry<Port, Port> e : connections )
        {
            if ( e.getValue().getOperatorId().equals( operatorId ) )
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

        throw new IllegalStateException( "No region found for operator " + operator.getId() );
    }

    public static RegionDef getRegionByFirstOperator ( final List<RegionDef> regions, final String operatorId )
    {
        for ( RegionDef region : regions )
        {
            if ( getFirstOperator( region ).getId().equals( operatorId ) )
            {
                return region;
            }
        }

        throw new IllegalStateException( "No region found for operator " + operatorId );
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
