package cs.bilkent.zanza.engine.region;

import cs.bilkent.zanza.engine.pipeline.PipelineInstance;

public class RegionInstance
{

    private final RegionRuntimeConfig config;

    // [pipelineIndex, replicaIndex]
    private final PipelineInstance[][] pipelines;

    public RegionInstance ( final RegionRuntimeConfig config, final PipelineInstance[][] pipelines )
    {
        this.config = config;
        this.pipelines = pipelines;
    }

    public RegionRuntimeConfig getConfig ()
    {
        return config;
    }

    public PipelineInstance[] getReplicaPipelines ( final int replicaIndex )
    {
        final PipelineInstance[] p = new PipelineInstance[ config.getPipelineCount() ];
        for ( int i = 0; i < config.getPipelineCount(); i++ )
        {
            p[ i ] = pipelines[ i ][ replicaIndex ];
        }
        return p;
    }

    public PipelineInstance[] getPipelineReplicas ( final int pipelineId )
    {
        return pipelines[ pipelineId ];
    }

}
