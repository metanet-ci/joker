package cs.bilkent.zanza.engine.region;

import java.util.Arrays;

import cs.bilkent.zanza.engine.pipeline.PipelineReplica;

public class Region
{

    private final RegionConfig config;

    // [pipelineIndex, replicaIndex]
    private final PipelineReplica[][] pipelines;

    public Region ( final RegionConfig config, final PipelineReplica[][] pipelines )
    {
        this.config = config;
        this.pipelines = Arrays.copyOf( pipelines, pipelines.length );
        for ( int i = 0; i < pipelines.length; i++ )
        {
            this.pipelines[ i ] = Arrays.copyOf( pipelines[ i ], pipelines[ i ].length );
        }
    }

    public int getRegionId ()
    {
        return config.getRegionId();
    }

    public RegionConfig getConfig ()
    {
        return config;
    }

    public PipelineReplica[] getReplicaPipelines ( final int replicaIndex )
    {
        final PipelineReplica[] p = new PipelineReplica[ config.getPipelineCount() ];
        for ( int i = 0; i < config.getPipelineCount(); i++ )
        {
            p[ i ] = pipelines[ i ][ replicaIndex ];
        }
        return p;
    }

    public PipelineReplica[] getPipelineReplicas ( final int pipelineId )
    {
        return pipelines[ pipelineId ];
    }

}
