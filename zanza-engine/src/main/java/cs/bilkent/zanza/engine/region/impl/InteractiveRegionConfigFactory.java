package cs.bilkent.zanza.engine.region.impl;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.annotation.concurrent.NotThreadSafe;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.common.base.Charsets;
import com.google.common.base.Splitter;

import static com.google.common.base.Preconditions.checkArgument;
import cs.bilkent.zanza.engine.config.ZanzaConfig;
import cs.bilkent.zanza.engine.exception.InitializationException;
import cs.bilkent.zanza.engine.region.FlowDeploymentDef.RegionGroup;
import cs.bilkent.zanza.engine.region.RegionConfig;
import cs.bilkent.zanza.engine.region.RegionDef;
import cs.bilkent.zanza.flow.OperatorDef;
import cs.bilkent.zanza.operator.spec.OperatorType;
import static java.util.stream.Collectors.toList;

@Singleton
@NotThreadSafe
public class InteractiveRegionConfigFactory extends AbstractRegionConfigFactory
{

    private BufferedReader br;

    @Inject
    public InteractiveRegionConfigFactory ( final ZanzaConfig zanzaConfig )
    {
        super( zanzaConfig );
    }

    @Override
    public void init ()
    {
        br = new BufferedReader( new InputStreamReader( System.in, Charsets.UTF_8 ) );
    }

    @Override
    public void destroy ()
    {
        try
        {
            br.close();
        }
        catch ( IOException e )
        {
            throw new InitializationException( "destroy failed", e );
        }
    }

    @Override
    protected List<RegionConfig> createRegionConfigs ( final RegionGroup regionGroup )
    {
        checkArgument( regionGroup != null, "null region group!" );
        final List<RegionDef> regions = regionGroup.getRegions();
        checkArgument( regions != null && regions.size() > 0, "no region definitions!" );

        try
        {
            System.out.println( "Region config with first " + regions.get( 0 ).getRegionType() + " region..." );
            final int replicaCount = readReplicaCount( regions );

            final List<List<Integer>> pipelineStartIndicesList = new ArrayList<>();
            for ( RegionDef region : regions )
            {
                pipelineStartIndicesList.add( readPipelineStartIndices( region ) );
            }

            final List<RegionConfig> regionConfigs = new ArrayList<>( regions.size() );
            for ( int i = 0; i < regions.size(); i++ )
            {
                regionConfigs.add( new RegionConfig( regions.get( 0 ), pipelineStartIndicesList.get( i ), replicaCount ) );
            }

            return regionConfigs;
        }
        catch ( Exception e )
        {
            if ( e instanceof InterruptedException )
            {
                Thread.currentThread().interrupt();
            }
            throw new InitializationException( "create region configs failed for " + regionGroup, e );
        }

    }

    private List<Integer> readPipelineStartIndices ( final RegionDef region ) throws IOException
    {
        System.out.println( "####+####+####+####+####+####+####+####+####+####" );
        System.out.println( "REGION: " + region.getRegionId() + " with type: " + region.getRegionType() );
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
                                           .collect( toList() );

            validatePipelineStartIndices( operators, pipelineStartIndices );
        }
        else
        {
            System.out.println( "Setting pipeline start indices as [0] automatically since there is a single operator" );
            pipelineStartIndices = Collections.singletonList( 0 );
        }
        return pipelineStartIndices;
    }

    private int readReplicaCount ( final List<RegionDef> regions ) throws IOException
    {
        final int replicaCount;
        if ( regions.get( 0 ).getRegionType() == OperatorType.PARTITIONED_STATEFUL )
        {
            System.out.println( "Enter replica count: " );
            replicaCount = Integer.parseInt( br.readLine() );
            checkArgument( replicaCount > 0 && replicaCount <= maxReplicaCount, "replica count must be between 0 and %s", maxReplicaCount );
        }
        else
        {
            replicaCount = 1;
            System.out.println( "Replica count is " + replicaCount + " since " + regions.get( 0 ).getRegionType() + " region" );
        }

        return replicaCount;
    }

}
