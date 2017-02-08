package cs.bilkent.joker.engine.region;

import java.util.Arrays;

import cs.bilkent.joker.engine.flow.RegionExecutionPlan;
import cs.bilkent.joker.engine.pipeline.PipelineId;
import cs.bilkent.joker.engine.pipeline.PipelineReplica;

public class Region
{

    private final RegionExecutionPlan executionPlan;

    // [pipelineIndex, replicaIndex]
    private final PipelineReplica[][] pipelines;

    public Region ( final RegionExecutionPlan executionPlan, final PipelineReplica[][] pipelines )
    {
        this.executionPlan = executionPlan;
        this.pipelines = Arrays.copyOf( pipelines, pipelines.length );
        for ( int i = 0; i < pipelines.length; i++ )
        {
            this.pipelines[ i ] = Arrays.copyOf( pipelines[ i ], pipelines[ i ].length );
        }
    }

    public int getRegionId ()
    {
        return executionPlan.getRegionId();
    }

    public RegionExecutionPlan getExecutionPlan ()
    {
        return executionPlan;
    }

    public RegionDef getRegionDef ()
    {
        return executionPlan.getRegionDef();
    }

    public PipelineReplica[] getReplicaPipelines ( final int replicaIndex )
    {
        final PipelineReplica[] p = new PipelineReplica[ executionPlan.getPipelineCount() ];
        for ( int i = 0; i < executionPlan.getPipelineCount(); i++ )
        {
            p[ i ] = pipelines[ i ][ replicaIndex ];
        }
        return p;
    }

    public PipelineReplica[] getPipelineReplicas ( final int pipelineIndex )
    {
        return pipelines[ pipelineIndex ];
    }

    public PipelineReplica[] getPipelineReplicasByPipelineId ( final PipelineId pipelineId )
    {
        for ( int i = 0; i < executionPlan.getPipelineCount(); i++ )
        {
            if ( pipelines[ i ][ 0 ].id().pipelineId.equals( pipelineId ) )
            {
                return Arrays.copyOf( pipelines[ i ], executionPlan.getReplicaCount() );
            }
        }

        throw new IllegalArgumentException( "Invalid pipeline id: " + pipelineId );
    }

}
