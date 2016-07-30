package cs.bilkent.zanza.engine.region.impl;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;
import javax.inject.Singleton;

import com.google.common.base.Splitter;

import static com.google.common.base.Preconditions.checkArgument;
import cs.bilkent.zanza.engine.exception.InitializationException;
import cs.bilkent.zanza.engine.region.RegionConfig;
import cs.bilkent.zanza.engine.region.RegionConfigFactory;
import cs.bilkent.zanza.engine.region.RegionDef;
import cs.bilkent.zanza.flow.FlowDef;
import cs.bilkent.zanza.flow.OperatorDef;
import cs.bilkent.zanza.flow.Port;
import cs.bilkent.zanza.operator.spec.OperatorType;

@Singleton
public class InteractiveRegionConfigFactory implements RegionConfigFactory
{

    private static final int MAX_REPLICA_COUNT = 16;

    @Override
    public List<RegionConfig> createRegionConfigs ( final FlowDef flow, final List<RegionDef> regions )
    {
        checkArgument( regions != null, "null region definitions" );
        try ( final BufferedReader br = new BufferedReader( new InputStreamReader( System.in ) ) )
        {
            final List<RegionConfig> regionConfigs = new ArrayList<>( regions.size() );

            int regionId = 0;
            for ( RegionDef region : getRegionsBFSOrdered( flow, regions ) )
            {
                System.out.println( "####+####+####+####+####+####+####+####+####+####" );
                System.out.println( "REGION: " + regionId + " with type: " + region.getRegionType() );
                if ( !region.getPartitionFieldNames().isEmpty() )
                {
                    System.out.println( "PARTITION FIELD NAMES: " + region.getPartitionFieldNames() );
                }

                final List<OperatorDef> operators = region.getOperators();
                for ( int i = 0; i < operators.size(); i++ )
                {
                    System.out.println( "Operator " + i + ": " + operators.get( i ).id() );
                }

                final List<Integer> pipelineStartIndices;
                if ( operators.size() > 1 )
                {
                    System.out.println( "Enter pipeline start indices (monotonic, comma-delimited):" );
                    pipelineStartIndices = Splitter.on( "," )
                                                   .splitToList( br.readLine() )
                                                   .stream()
                                                   .map( String::trim )
                                                   .map( Integer::valueOf )
                                                   .collect( Collectors.toList() );

                    validatePipelineStartIndices( operators, pipelineStartIndices );
                }
                else
                {
                    System.out.println( "Setting pipeline start indices as [0] automatically since there is a single operator" );
                    pipelineStartIndices = Collections.singletonList( 0 );
                }

                final int replicaCount;
                if ( region.getRegionType() == OperatorType.PARTITIONED_STATEFUL )
                {
                    System.out.println( "Enter replica count: " );
                    replicaCount = Integer.parseInt( br.readLine() );
                    checkArgument( replicaCount > 0 && replicaCount <= MAX_REPLICA_COUNT,
                                   "replica count must be between 0 and %s",
                                   MAX_REPLICA_COUNT );
                }
                else
                {
                    replicaCount = 1;
                    System.out.println( "Replica count is " + replicaCount + " since " + region.getRegionType() + " region" );
                }

                regionConfigs.add( new RegionConfig( regionId++, region, pipelineStartIndices, replicaCount ) );
            }

            return regionConfigs;
        }
        catch ( IOException e )
        {
            throw new InitializationException( "Region config formation failed", e );
        }
    }

    private void validatePipelineStartIndices ( final List<OperatorDef> operators, final List<Integer> pipelineStartIndices )
    {
        checkArgument( pipelineStartIndices.size() <= operators.size() );
        int i = -1;
        for ( int startIndex : pipelineStartIndices )
        {
            checkArgument( startIndex > i, "invalid pipeline start indices: ", pipelineStartIndices );
            i = startIndex;
        }
    }

    private List<RegionDef> getRegionsBFSOrdered ( final FlowDef flow, final List<RegionDef> regions )
    {
        final List<RegionDef> curr = new LinkedList<>();
        final List<RegionDef> ordered = new LinkedList<>();

        for ( OperatorDef operator : flow.getOperatorsWithNoInputPorts() )
        {
            curr.add( getRegion( regions, operator ) );
        }

        while ( !curr.isEmpty() )
        {
            final RegionDef region = curr.remove( 0 );
            if ( !ordered.contains( region ) )
            {
                ordered.add( region );

                final List<OperatorDef> operators = region.getOperators();
                final OperatorDef lastOperator = operators.get( operators.size() - 1 );
                for ( Collection<Port> c : flow.getDownstreamConnections( lastOperator.id() ).values() )
                {
                    for ( Port p : c )
                    {
                        final OperatorDef downstreamOperator = flow.getOperator( p.operatorId );
                        final RegionDef downstreamRegion = getRegion( regions, downstreamOperator );
                        curr.add( downstreamRegion );
                    }

                }
            }
        }
        return ordered;
    }

    private RegionDef getRegion ( final List<RegionDef> regions, final OperatorDef operator )
    {
        for ( RegionDef region : regions )
        {
            if ( region.getOperators().get( 0 ).equals( operator ) )
            {
                return region;
            }
        }

        throw new IllegalStateException( "No region found for operator " + operator.id() );
    }

}
