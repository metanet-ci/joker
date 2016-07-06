package cs.bilkent.zanza.engine.region;

import cs.bilkent.zanza.engine.pipeline.PipelineReplica;

public class Region
{

    private final RegionRuntimeConfig config;

    // [pipelineIndex, replicaIndex]
    private final PipelineReplica[][] pipelines;

    public Region ( final RegionRuntimeConfig config, final PipelineReplica[][] pipelines )
    {
        this.config = config;
        this.pipelines = pipelines;
    }

    public RegionRuntimeConfig getConfig ()
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
