package cs.bilkent.zanza.engine.region;

import cs.bilkent.zanza.engine.pipeline.PipelineInstance;

public class RegionInstance
{

    private final RegionRuntimeConfig config;

    // [replicaIndex, pipelineIndex]
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

    public PipelineInstance[] getPipelines ( final int replicaIndex )
    {
        return pipelines[ replicaIndex ];
    }

}
