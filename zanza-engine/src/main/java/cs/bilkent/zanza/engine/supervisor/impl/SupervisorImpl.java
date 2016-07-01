package cs.bilkent.zanza.engine.supervisor.impl;

import javax.annotation.concurrent.NotThreadSafe;

import com.google.inject.Inject;
import com.google.inject.Singleton;

import cs.bilkent.zanza.engine.pipeline.PipelineInstanceId;
import cs.bilkent.zanza.engine.pipeline.UpstreamContext;
import cs.bilkent.zanza.engine.region.RegionManager;
import cs.bilkent.zanza.engine.supervisor.Supervisor;

@Singleton
@NotThreadSafe
public class SupervisorImpl implements Supervisor
{

    private final RegionManager regionManager;

    @Inject
    public SupervisorImpl ( final RegionManager regionManager )
    {
        this.regionManager = regionManager;
    }

    @Override
    public UpstreamContext getUpstreamContext ( final PipelineInstanceId id )
    {
        return null;
    }

    @Override
    public void notifyPipelineCompletedRunning ( final PipelineInstanceId id )
    {

    }

}
