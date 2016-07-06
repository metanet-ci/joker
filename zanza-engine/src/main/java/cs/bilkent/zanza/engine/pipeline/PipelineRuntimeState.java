package cs.bilkent.zanza.engine.pipeline;

import java.util.Arrays;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static cs.bilkent.zanza.engine.pipeline.OperatorReplicaStatus.INITIAL;
import cs.bilkent.zanza.engine.region.RegionDef;
import cs.bilkent.zanza.engine.region.RegionRuntimeConfig;
import cs.bilkent.zanza.flow.OperatorDef;
import cs.bilkent.zanza.operator.scheduling.SchedulingStrategy;

public class PipelineRuntimeState
{

    private final PipelineId id;

    private final RegionRuntimeConfig regionRuntimeConfig;

    private PipelineReplica[] replicas;

    private OperatorReplicaStatus pipelineStatus = INITIAL;

    private OperatorReplicaStatus[] replicaStatuses;

    private SchedulingStrategy initialSchedulingStrategy;

    private PipelineReplicaRunner[] runners;

    private Thread[] threads;

    private UpstreamContext upstreamContext;

    private DownstreamTupleSender[] downstreamTupleSenders;

    public PipelineRuntimeState ( final PipelineId id, final RegionRuntimeConfig regionRuntimeConfig )
    {
        this.id = id;
        this.regionRuntimeConfig = regionRuntimeConfig;
        final int replicaCount = regionRuntimeConfig.getReplicaCount();
        this.replicas = new PipelineReplica[ replicaCount ];
        replicaStatuses = new OperatorReplicaStatus[ replicaCount ];
        Arrays.fill( replicaStatuses, INITIAL );
        this.runners = new PipelineReplicaRunner[ replicaCount ];
        this.threads = new Thread[ replicaCount ];
        this.downstreamTupleSenders = new DownstreamTupleSender[ replicaCount ];
    }

    public PipelineId getId ()
    {
        return id;
    }

    public RegionDef getRegionDef ()
    {
        return regionRuntimeConfig.getRegionDef();
    }

    public OperatorDef[] getOperatorDefs ()
    {
        return regionRuntimeConfig.getOperatorDefs( id.pipelineId );
    }

    public int getOperatorCount ()
    {
        return regionRuntimeConfig.getOperatorDefs( id.pipelineId ).length;
    }

    public OperatorReplicaStatus getPipelineStatus ()
    {
        return pipelineStatus;
    }

    public void setPipelineStatus ( final OperatorReplicaStatus pipelineStatus )
    {
        checkNotNull( pipelineStatus );
        this.pipelineStatus = pipelineStatus;
    }

    public SchedulingStrategy getInitialSchedulingStrategy ()
    {
        return initialSchedulingStrategy;
    }

    public void setInitialSchedulingStrategy ( final SchedulingStrategy initialSchedulingStrategy )
    {
        checkNotNull( initialSchedulingStrategy );
        this.initialSchedulingStrategy = initialSchedulingStrategy;
    }

    public UpstreamContext getUpstreamContext ()
    {
        return upstreamContext;
    }

    public void setUpstreamContext ( final UpstreamContext upstreamContext )
    {
        checkNotNull( upstreamContext );
        this.upstreamContext = upstreamContext;
    }

    public int getOperatorIndex ( final OperatorDef operator )
    {
        final OperatorDef[] operatorDefs = getOperatorDefs();
        for ( int i = 0; i < operatorDefs.length; i++ )
        {
            if ( operatorDefs[ i ].equals( operator ) )
            {
                return i;
            }
        }

        return -1;
    }

    public int getReplicaCount ()
    {
        return replicas.length;
    }

    public void setPipelineReplica ( final int replicaIndex, final PipelineReplica pipelineReplica )
    {
        checkNotNull( pipelineReplica );
        checkState( replicas[ replicaIndex ] == null );
        replicas[ replicaIndex ] = pipelineReplica;
    }

    public PipelineReplica getPipelineReplica ( final int replicaIndex )
    {
        return replicas[ replicaIndex ];
    }

    public void setPipelineReplicaRunner ( final int replicaIndex, final PipelineReplicaRunner pipelineReplicaRunner, final Thread thread )
    {
        checkNotNull( pipelineReplicaRunner );
        checkNotNull( thread );
        checkState( runners[ replicaIndex ] == null );
        checkState( threads[ replicaIndex ] == null );
        runners[ replicaIndex ] = pipelineReplicaRunner;
        threads[ replicaIndex ] = thread;
    }

    public PipelineReplicaRunner getPipelineReplicaRunner ( final int replicaIndex )
    {
        return runners[ replicaIndex ];
    }

    public Thread getPipelineReplicaRunnerThread ( final int replicaIndex )
    {
        return threads[ replicaIndex ];
    }

    public void setDownstreamTupleSender ( final int replicaIndex, final DownstreamTupleSender downstreamTupleSender )
    {
        checkNotNull( downstreamTupleSender );
        checkState( downstreamTupleSenders[ replicaIndex ] == null );
        downstreamTupleSenders[ replicaIndex ] = downstreamTupleSender;
    }

    public DownstreamTupleSender getDownstreamTupleSender ( final int replicaIndex )
    {
        return downstreamTupleSenders[ replicaIndex ];
    }

}
