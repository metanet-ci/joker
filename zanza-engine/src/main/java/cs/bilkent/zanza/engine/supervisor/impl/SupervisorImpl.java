package cs.bilkent.zanza.engine.supervisor.impl;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.concurrent.NotThreadSafe;

import com.google.inject.Inject;
import com.google.inject.Singleton;

import cs.bilkent.zanza.engine.config.ZanzaConfig;
import cs.bilkent.zanza.engine.pipeline.PipelineId;
import cs.bilkent.zanza.engine.pipeline.PipelineReplicaId;
import cs.bilkent.zanza.engine.pipeline.PipelineRuntimeManager;
import cs.bilkent.zanza.engine.pipeline.PipelineRuntimeState;
import cs.bilkent.zanza.engine.pipeline.UpstreamContext;
import cs.bilkent.zanza.engine.region.RegionManager;
import cs.bilkent.zanza.engine.region.RegionRuntimeConfig;
import cs.bilkent.zanza.engine.supervisor.Supervisor;
import cs.bilkent.zanza.flow.FlowDef;

@Singleton
@NotThreadSafe
public class SupervisorImpl implements Supervisor
{

    private final ZanzaConfig zanzaConfig;

    private final RegionManager regionManager;

    private final PipelineRuntimeManager pipelineRuntimeManager;

    private final Map<PipelineId, PipelineRuntimeState> pipelineRuntimeStates = new HashMap<>();

    @Inject
    public SupervisorImpl ( final ZanzaConfig zanzaConfig,
                            final RegionManager regionManager,
                            final PipelineRuntimeManager pipelineRuntimeManager )
    {
        this.zanzaConfig = zanzaConfig;
        this.regionManager = regionManager;
        this.pipelineRuntimeManager = pipelineRuntimeManager;
    }

    @Override
    public void deploy ( final FlowDef flow, final List<RegionRuntimeConfig> regionRuntimeConfigs )
    {
        final Collection<PipelineRuntimeState> pipelineRuntimeStates = pipelineRuntimeManager.createPipelineRuntimeStates( this,
                                                                                                                           flow,
                                                                                                                           regionRuntimeConfigs );
    }

    @Override
    public UpstreamContext getUpstreamContext ( final PipelineReplicaId id )
    {
        return null;
    }

    @Override
    public void notifyPipelineCompletedRunning ( final PipelineReplicaId id )
    {

    }

}
