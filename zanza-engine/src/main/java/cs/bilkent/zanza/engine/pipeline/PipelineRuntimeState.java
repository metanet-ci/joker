package cs.bilkent.zanza.engine.pipeline;

import java.util.Arrays;

import static cs.bilkent.zanza.engine.pipeline.OperatorInstanceStatus.INITIAL;
import cs.bilkent.zanza.engine.region.RegionDefinition;
import cs.bilkent.zanza.flow.OperatorDefinition;
import cs.bilkent.zanza.operator.scheduling.SchedulingStrategy;

public class PipelineRuntimeState
{

    private final PipelineId id;

    private final RegionDefinition regionDefinition;

    private final OperatorDefinition[] operatorDefinitions;

    private final PipelineInstance[] replicas;

    private OperatorInstanceStatus pipelineStatus = INITIAL;

    private OperatorInstanceStatus[] replicaStatuses;

    private SchedulingStrategy initialSchedulingStrategy;

    private UpstreamContext upstreamContext;

    private DownstreamTupleSender downstreamTupleSender;

    public PipelineRuntimeState ( final PipelineId id,
                                  final RegionDefinition regionDefinition,
                                  final OperatorDefinition[] operatorDefinitions,
                                  final PipelineInstance[] replicas )
    {
        this.id = id;
        this.regionDefinition = regionDefinition;
        this.operatorDefinitions = operatorDefinitions;
        this.replicas = replicas;
        replicaStatuses = new OperatorInstanceStatus[ replicas.length ];
        Arrays.fill( replicaStatuses, INITIAL );
    }

    public PipelineId getId ()
    {
        return id;
    }

    public RegionDefinition getRegionDefinition ()
    {
        return regionDefinition;
    }

    public OperatorInstanceStatus getPipelineStatus ()
    {
        return pipelineStatus;
    }

    public void setPipelineStatus ( final OperatorInstanceStatus pipelineStatus )
    {
        this.pipelineStatus = pipelineStatus;
    }

    public SchedulingStrategy getInitialSchedulingStrategy ()
    {
        return initialSchedulingStrategy;
    }

    public void setInitialSchedulingStrategy ( final SchedulingStrategy initialSchedulingStrategy )
    {
        this.initialSchedulingStrategy = initialSchedulingStrategy;
    }

    public UpstreamContext getUpstreamContext ()
    {
        return upstreamContext;
    }

    public void setUpstreamContext ( final UpstreamContext upstreamContext )
    {
        this.upstreamContext = upstreamContext;
    }

    public DownstreamTupleSender getDownstreamTupleSender ()
    {
        return downstreamTupleSender;
    }

    public void setDownstreamTupleSender ( final DownstreamTupleSender downstreamTupleSender )
    {
        this.downstreamTupleSender = downstreamTupleSender;
    }

}
