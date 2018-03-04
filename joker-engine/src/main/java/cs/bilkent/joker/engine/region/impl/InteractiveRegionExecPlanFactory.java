package cs.bilkent.joker.engine.region.impl;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Collections;
import java.util.List;
import javax.annotation.concurrent.NotThreadSafe;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.common.base.Charsets;
import com.google.common.base.Splitter;

import static com.google.common.base.Preconditions.checkArgument;
import cs.bilkent.joker.engine.config.JokerConfig;
import cs.bilkent.joker.engine.exception.InitializationException;
import cs.bilkent.joker.engine.flow.RegionDef;
import cs.bilkent.joker.engine.flow.RegionExecPlan;
import static cs.bilkent.joker.engine.util.ExceptionUtils.checkInterruption;
import cs.bilkent.joker.operator.OperatorDef;
import cs.bilkent.joker.operator.spec.OperatorType;
import static java.util.stream.Collectors.toList;

@Singleton
@NotThreadSafe
public class InteractiveRegionExecPlanFactory extends AbstractRegionExecPlanFactory
{

    private BufferedReader br;

    @Inject
    public InteractiveRegionExecPlanFactory ( final JokerConfig jokerConfig )
    {
        super( jokerConfig );
    }

    @Override
    public void init ()
    {
        br = new BufferedReader( new InputStreamReader( System.in, Charsets.UTF_8 ) );
    }

    @Override
    public void destroy ()
    {
        br = null;
    }

    @Override
    protected RegionExecPlan createRegionExecPlan ( final RegionDef regionDef )
    {
        checkArgument( regionDef != null, "null region def!" );

        try
        {
            System.out.println( "Region execution plan for " + regionDef.getRegionType() + " region..." );
            final int replicaCount = readReplicaCount( regionDef );
            final List<Integer> pipelineStartIndices = readPipelineStartIndices( regionDef );

            return new RegionExecPlan( regionDef, pipelineStartIndices, replicaCount );
        }
        catch ( Exception e )
        {
            checkInterruption( e );
            throw new InitializationException( "create region execution plan failed for " + regionDef, e );
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
            System.out.println( "Operator " + i + ": " + operators.get( i ).getId() );
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

    private int readReplicaCount ( final RegionDef regionDef ) throws IOException
    {
        final int replicaCount;
        if ( regionDef.getRegionType() == OperatorType.PARTITIONED_STATEFUL )
        {
            System.out.println( "Enter replica count: " );
            replicaCount = Integer.parseInt( br.readLine() );
            checkArgument( replicaCount > 0 && replicaCount <= maxReplicaCount, "replica count must be between 0 and %s", maxReplicaCount );
        }
        else
        {
            replicaCount = 1;
            System.out.println( "Replica count is " + replicaCount + " since " + regionDef.getRegionType() + " region" );
        }

        return replicaCount;
    }

}
