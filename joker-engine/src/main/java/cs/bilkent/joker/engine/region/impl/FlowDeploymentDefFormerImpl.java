package cs.bilkent.joker.engine.region.impl;

import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import javax.annotation.concurrent.NotThreadSafe;
import javax.inject.Inject;
import javax.inject.Singleton;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import static com.google.common.base.Preconditions.checkArgument;
import cs.bilkent.joker.engine.config.FlowDeploymentConfig;
import cs.bilkent.joker.engine.config.JokerConfig;
import cs.bilkent.joker.engine.region.FlowDeploymentDef;
import cs.bilkent.joker.engine.region.FlowDeploymentDef.RegionGroup;
import cs.bilkent.joker.engine.region.FlowDeploymentDefFormer;
import cs.bilkent.joker.engine.region.RegionDef;
import static cs.bilkent.joker.engine.util.RegionUtil.getRegionByLastOperator;
import static cs.bilkent.joker.engine.util.RegionUtil.sortTopologically;
import cs.bilkent.joker.flow.FlowDef;
import cs.bilkent.joker.flow.FlowDefBuilder;
import cs.bilkent.joker.flow.Port;
import cs.bilkent.joker.operator.OperatorDef;
import cs.bilkent.joker.operator.spec.OperatorType;
import static cs.bilkent.joker.operator.spec.OperatorType.PARTITIONED_STATEFUL;
import static cs.bilkent.joker.operator.spec.OperatorType.STATEFUL;
import static cs.bilkent.joker.operator.spec.OperatorType.STATELESS;
import static java.util.stream.Collectors.toList;

@NotThreadSafe
@Singleton
public class FlowDeploymentDefFormerImpl implements FlowDeploymentDefFormer
{

    private static final Logger LOGGER = LoggerFactory.getLogger( FlowDeploymentDefFormerImpl.class );


    private final FlowDeploymentConfig flowDeploymentConfig;

    private final IdGenerator idGenerator;

    @Inject
    public FlowDeploymentDefFormerImpl ( final JokerConfig jokerConfig, final IdGenerator idGenerator )
    {
        this.flowDeploymentConfig = jokerConfig.getFlowDeploymentConfig();
        this.idGenerator = idGenerator;
    }

    @Override
    public FlowDeploymentDef createFlowDeploymentDef ( final FlowDef flow, final List<RegionDef> regions )
    {
        final Map<String, OperatorDef> operators = flow.getOperatorsMap();
        final Collection<Entry<Port, Port>> connections = flow.getConnections();
        final List<RegionDef> regionsToDeploy = new ArrayList<>( regions );

        if ( flowDeploymentConfig.isDuplicateStatelessRegionsEnabled() )
        {
            LOGGER.debug( "Duplicating {} regions", STATELESS );
            duplicateStatelessRegions( operators, connections, regionsToDeploy );
        }

        if ( flowDeploymentConfig.isMergeStatelessRegionsWithStatefulRegionsEnabled() )
        {
            LOGGER.debug( "Merging regions" );
            mergeRegions( operators, connections, regionsToDeploy );
        }

        if ( flowDeploymentConfig.isPairStatelessRegionsWithPartitionedStatefulRegionsEnabled() )
        {
            LOGGER.debug( "Pairing {} regions with {} regions", STATELESS, PARTITIONED_STATEFUL );
            final List<RegionGroup> regionGroups = pairStatelessRegionsWithPartitionedStatefulRegions( operators,
                                                                                                       connections,
                                                                                                       regionsToDeploy );
            return new FlowDeploymentDef( createFlow( operators, connections ), regionsToDeploy, regionGroups );
        }
        else
        {
            return new FlowDeploymentDef( createFlow( operators, connections ), regionsToDeploy );
        }
    }

    private FlowDef createFlow ( final Map<String, OperatorDef> operators, final Collection<Entry<Port, Port>> connections )
    {
        final FlowDefBuilder flowDefBuilder = new FlowDefBuilder();

        for ( OperatorDef operator : operators.values() )
        {
            flowDefBuilder.add( operator );
        }

        for ( Entry<Port, Port> e : connections )
        {
            flowDefBuilder.connect( e.getKey().operatorId, e.getKey().portIndex, e.getValue().operatorId, e.getValue().portIndex );
        }

        return flowDefBuilder.build();
    }

    List<RegionGroup> pairStatelessRegionsWithPartitionedStatefulRegions ( final Map<String, OperatorDef> operators,
                                                                           final Collection<Entry<Port, Port>> connections,
                                                                           final List<RegionDef> regions )
    {
        final List<List<RegionDef>> regionGroups = new ArrayList<>();
        for ( RegionDef region : sortTopologically( operators, connections, regions ) )
        {
            if ( region.getRegionType() != STATELESS )
            {
                addSingletonRegionGroup( regionGroups, region );
            }
            else
            {
                final List<OperatorDef> upstreamOperators = getUpstreamOperators( operators, connections, region.getFirstOperator().id() );
                if ( upstreamOperators.size() == 1 )
                {
                    final RegionDef upstreamRegion = getRegionByLastOperator( regions, upstreamOperators.get( 0 ) );
                    final OperatorType upstreamRegionType = upstreamRegion.getRegionType();
                    if ( upstreamRegionType == STATEFUL )
                    {
                        addSingletonRegionGroup( regionGroups, region );
                    }
                    else if ( upstreamRegionType == PARTITIONED_STATEFUL )
                    {
                        boolean added = false;
                        for ( List<RegionDef> regionGroup : regionGroups )
                        {
                            if ( regionGroup.get( 0 ).equals( upstreamRegion ) )
                            {
                                LOGGER.debug( "Adding {} to region group {}", region, regionGroup );
                                regionGroup.add( region );
                                added = true;
                                break;
                            }
                        }
                        checkArgument( added, "Cannot find region group of upstream {} of {}", upstreamRegion, region );
                    }
                    else if ( upstreamRegionType == STATELESS )
                    {
                        boolean newGroup = true;
                        for ( List<RegionDef> regionGroup : regionGroups )
                        {
                            if ( regionGroup.get( 0 ).getRegionType() == PARTITIONED_STATEFUL && regionGroup.contains( region ) )
                            {
                                LOGGER.debug( "Adding {} to region group {}", region, regionGroup );
                                regionGroup.add( region );
                                newGroup = false;
                                break;
                            }
                        }
                        if ( newGroup )
                        {
                            addSingletonRegionGroup( regionGroups, region );
                        }
                    }
                    else
                    {
                        throw new IllegalStateException( "Invalid region type: " + upstreamRegionType );
                    }
                }
                else
                {
                    addSingletonRegionGroup( regionGroups, region );
                }
            }
        }

        return regionGroups.stream().map( RegionGroup::new ).collect( toList() );
    }

    private void addSingletonRegionGroup ( final List<List<RegionDef>> regionGroups, final RegionDef region )
    {
        LOGGER.debug( "Adding region group for {}", region );
        final List<RegionDef> l = new ArrayList<>();
        l.add( region );
        regionGroups.add( l );
    }

    void mergeRegions ( final Map<String, OperatorDef> operators,
                        final Collection<Entry<Port, Port>> connections,
                        final List<RegionDef> regions )
    {
        int before, after = regions.size();
        do
        {
            before = after;
            attemptToMergeRegions( operators, connections, regions );
            after = regions.size();
        } while ( before != after );
    }

    private void attemptToMergeRegions ( final Map<String, OperatorDef> operators, final Collection<Entry<Port, Port>> connections,
                                         final List<RegionDef> regions )
    {
        for ( RegionDef region : regions )
        {
            final OperatorDef firstOperator = region.getFirstOperator();
            final List<OperatorDef> upstreamOperators = getUpstreamOperators( operators, connections, firstOperator.id() );
            if ( upstreamOperators.size() == 1 )
            {
                final RegionDef upstreamRegion = getRegionByLastOperator( regions, upstreamOperators.get( 0 ) );

                if ( region.getRegionType() == PARTITIONED_STATEFUL )
                {
                    checkArgument( upstreamRegion.getRegionType() != STATELESS,
                                   "Invalid upstream %s for downstream %s",
                                   upstreamRegion,
                                   region );
                    LOGGER.debug( "Will not merge downstream {} with upstream {} with single downstream",
                                  region.getRegionType(),
                                  upstreamRegion.getRegionType() );
                    continue;
                }
                else if ( region.getRegionType() == STATEFUL )
                {
                    if ( upstreamRegion.getRegionType() == PARTITIONED_STATEFUL )
                    {
                        LOGGER.debug( "Will not merge downstream {} with upstream {} with single downstream",
                                      region.getRegionType(),
                                      upstreamRegion.getRegionType() );
                        continue;
                    }
                }

                if ( getDownstreamOperators( operators, connections, upstreamRegion.getLastOperator().id() ).size() == 1 )
                {
                    final OperatorType newRegionType;
                    if ( upstreamRegion.getRegionType() == PARTITIONED_STATEFUL )
                    {
                        newRegionType = PARTITIONED_STATEFUL;
                    }
                    else if ( upstreamRegion.getRegionType() == STATEFUL || region.getRegionType() == STATEFUL )
                    {
                        newRegionType = STATEFUL;
                    }
                    else
                    {
                        newRegionType = STATELESS;
                    }

                    LOGGER.debug( "Downstream {} is appended to Upstream {}. New region is {}", region, upstreamRegion, newRegionType );
                    final List<OperatorDef> regionOperators = new ArrayList<>( upstreamRegion.getOperators() );
                    regionOperators.addAll( region.getOperators() );
                    final RegionDef newRegion = new RegionDef( idGenerator.nextId(),
                                                               newRegionType,
                                                               upstreamRegion.getPartitionFieldNames(),
                                                               regionOperators );
                    regions.remove( region );
                    regions.remove( upstreamRegion );
                    regions.add( newRegion );
                    break;
                }
            }
            else
            {
                LOGGER.debug( "Will not merge downstream {} since it has {} upstream regions",
                              region.getRegionType(),
                              upstreamOperators.size() );
            }
        }
    }

    void duplicateStatelessRegions ( final Map<String, OperatorDef> operators, final Collection<Entry<Port, Port>> connections,
                                     final List<RegionDef> regions )
    {
        boolean optimized;
        do
        {
            optimized = false;

            for ( RegionDef region : sortTopologically( operators, connections, regions ) )
            {
                if ( optimized )
                {
                    break;
                }
                else if ( region.getRegionType() != STATELESS )
                {
                    continue;
                }

                optimized = duplicateStatelessRegion( operators, connections, regions, region );
            }

        } while ( optimized );
    }

    private boolean duplicateStatelessRegion ( final Map<String, OperatorDef> operators, final Collection<Entry<Port, Port>> connections,
                                               final List<RegionDef> optimizedRegions,
                                               final RegionDef region )
    {
        final OperatorDef firstOperator = region.getFirstOperator();
        final List<OperatorDef> upstreamOperators = getUpstreamOperators( operators, connections, firstOperator.id() );
        final int upstreamOperatorCount = upstreamOperators.size();
        if ( upstreamOperatorCount > 1 )
        {
            LOGGER.debug( "Optimizing {} as its first operator {} has {} upstream operators",
                          region,
                          firstOperator.id(),
                          upstreamOperatorCount );

            // remove the region along with its operators and their connections

            optimizedRegions.remove( region );
            final List<OperatorDef> regionOperators = region.getOperators();
            for ( OperatorDef o : regionOperators )
            {
                operators.remove( o.id() );
                LOGGER.debug( "Removing operator {}", o.id() );
            }

            final Multimap<Port, Port> upstreamConnections = getUpstreamConnections( connections, firstOperator.id() );
            for ( Entry<Port, Port> e : upstreamConnections.entries() )
            {
                LOGGER.debug( "Removing upstream connection <{}, {}>", e.getValue(), e.getKey() );
                connections.remove( new SimpleEntry<>( e.getValue(), e.getKey() ) );
            }
            final Map<String, Multimap<Port, Port>> downstreamConnections = new HashMap<>();

            for ( OperatorDef o : regionOperators )
            {
                final Multimap<Port, Port> d = getDownstreamConnections( connections, o.id() );
                downstreamConnections.put( o.id(), d );
                for ( Entry<Port, Port> e : d.entries() )
                {
                    LOGGER.debug( "Removing downstream connection <{}, {}>", e.getKey(), e.getValue() );
                    connections.remove( new SimpleEntry<>( e.getKey(), e.getValue() ) );
                }
            }

            // duplicate the region using its duplicate operators and connections

            for ( int duplicateIndex = 0; duplicateIndex < upstreamOperatorCount; duplicateIndex++ )
            {
                final List<OperatorDef> duplicateOperators = new ArrayList<>();
                for ( OperatorDef o : regionOperators )
                {
                    final String duplicateOperatorId = toDuplicateOperatorId( o, duplicateIndex );
                    final OperatorDef duplicateOperator = new OperatorDef( duplicateOperatorId,
                                                                           o.operatorClazz(),
                                                                           o.operatorType(),
                                                                           o.inputPortCount(),
                                                                           o.outputPortCount(),
                                                                           o.schema(),
                                                                           o.config(),
                                                                           o.partitionFieldNames() );
                    duplicateOperators.add( duplicateOperator );
                    operators.put( duplicateOperatorId, duplicateOperator );
                    LOGGER.debug( "Operator {} is duplicated with id {}", o.id(), duplicateOperatorId );
                }

                final RegionDef regionDuplicate = new RegionDef( idGenerator.nextId(),
                                                                 STATELESS,
                                                                 region.getPartitionFieldNames(),
                                                                 duplicateOperators );
                optimizedRegions.add( regionDuplicate );

                final OperatorDef upstreamOperator = upstreamOperators.get( duplicateIndex );
                for ( Entry<Port, Port> e : upstreamConnections.entries() )
                {
                    if ( e.getValue().operatorId.equals( upstreamOperator.id() ) )
                    {
                        final Port duplicateDestinationPort = new Port( duplicateOperators.get( 0 ).id(), e.getKey().portIndex );
                        connections.add( new SimpleEntry<>( e.getValue(), duplicateDestinationPort ) );
                        LOGGER.debug( "Connection < {} , {} > is duplicated as < {} , {} >",
                                      e.getValue(),
                                      e.getKey(),
                                      e.getValue(),
                                      duplicateDestinationPort );
                    }
                }

                for ( int i = 0; i < duplicateOperators.size() - 1; i++ )
                {
                    final OperatorDef duplicateOperator = duplicateOperators.get( i );
                    final OperatorDef originalOperator = regionOperators.get( i );

                    for ( Entry<Port, Port> e : downstreamConnections.get( originalOperator.id() ).entries() )
                    {
                        final Port sourcePort = new Port( duplicateOperator.id(), e.getKey().portIndex );
                        final Port destinationPort = new Port( duplicateOperators.get( i + 1 ).id(), e.getValue().portIndex );
                        connections.add( new SimpleEntry<>( sourcePort, destinationPort ) );
                        LOGGER.debug( "Connection < {} , {} > is duplicated as < {} , {} >",
                                      e.getKey(),
                                      e.getValue(),
                                      sourcePort,
                                      destinationPort );
                    }
                }

                for ( Entry<Port, Port> e : downstreamConnections.get( region.getLastOperator().id() ).entries() )
                {
                    final OperatorDef duplicateOperator = duplicateOperators.get( duplicateOperators.size() - 1 );
                    final Port duplicateSourcePort = new Port( duplicateOperator.id(), e.getKey().portIndex );
                    connections.add( new SimpleEntry<>( duplicateSourcePort, e.getValue() ) );
                    LOGGER.debug( "Connection < {} , {} > is duplicated as < {} , {} >",
                                  e.getKey(),
                                  e.getValue(),
                                  duplicateSourcePort,
                                  e.getValue() );
                }
            }

            return true;
        }

        return false;
    }

    private String toDuplicateOperatorId ( final OperatorDef o, final int duplicateIndex )
    {
        return o.id() + "_d" + duplicateIndex;
    }

    private List<OperatorDef> getUpstreamOperators ( final Map<String, OperatorDef> operators,
                                                     final Collection<Entry<Port, Port>> connections,
                                                     final String operatorId )
    {
        final List<OperatorDef> upstream = new ArrayList<>();
        for ( Entry<Port, Port> e : getUpstreamConnections( connections, operatorId ).entries() )
        {
            final OperatorDef operator = operators.get( e.getValue().operatorId );
            if ( !upstream.contains( operator ) )
            {
                upstream.add( operator );
            }
        }

        return upstream;
    }

    private List<OperatorDef> getDownstreamOperators ( final Map<String, OperatorDef> operators,
                                                       final Collection<Entry<Port, Port>> connections,
                                                       final String operatorId )
    {
        final List<OperatorDef> downstream = new ArrayList<>();
        for ( Entry<Port, Port> e : getDownstreamConnections( connections, operatorId ).entries() )
        {
            final OperatorDef operator = operators.get( e.getValue().operatorId );
            if ( !downstream.contains( operator ) )
            {
                downstream.add( operator );
            }
        }

        return downstream;
    }

    private Multimap<Port, Port> getUpstreamConnections ( final Collection<Entry<Port, Port>> connections, final String operatorId )
    {
        final Multimap<Port, Port> upstream = HashMultimap.create();
        for ( Entry<Port, Port> e : connections )
        {
            if ( e.getValue().operatorId.equals( operatorId ) )
            {
                upstream.put( e.getValue(), e.getKey() );
            }
        }

        return upstream;
    }

    private Multimap<Port, Port> getDownstreamConnections ( final Collection<Entry<Port, Port>> connections, final String operatorId )
    {
        final Multimap<Port, Port> downstream = HashMultimap.create();
        for ( Entry<Port, Port> e : connections )
        {
            if ( e.getKey().operatorId.equals( operatorId ) )
            {
                downstream.put( e.getKey(), e.getValue() );
            }
        }

        return downstream;
    }

}
