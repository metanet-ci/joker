package cs.bilkent.zanza.engine.pipeline;

import java.util.Arrays;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static cs.bilkent.zanza.engine.pipeline.OperatorReplicaStatus.INITIAL;
import cs.bilkent.zanza.engine.region.RegionDefinition;
import cs.bilkent.zanza.engine.region.RegionRuntimeConfig;
import cs.bilkent.zanza.flow.OperatorDefinition;
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

    public RegionDefinition getRegionDefinition ()
    {
        return regionRuntimeConfig.getRegion();
    }

    public OperatorDefinition[] getOperatorDefinitions ()
    {
        return regionRuntimeConfig.getOperatorDefinitions( id.pipelineId );
    }

    public int getOperatorCount ()
    {
        return regionRuntimeConfig.getOperatorDefinitions( id.pipelineId ).length;
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

    public int getOperatorIndex ( final OperatorDefinition operator )
    {
        final OperatorDefinition[] operatorDefinitions = getOperatorDefinitions();
        for ( int i = 0; i < operatorDefinitions.length; i++ )
        {
            if ( operatorDefinitions[ i ].equals( operator ) )
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

    public void setPipelineInstance ( final int replicaIndex, final PipelineReplica pipelineReplica )
    {
        checkNotNull( pipelineReplica );
        checkState( replicas[ replicaIndex ] == null );
        replicas[ replicaIndex ] = pipelineReplica;
    }

    public PipelineReplica getPipelineInstance ( final int replicaIndex )
    {
        return replicas[ replicaIndex ];
    }

    public void setPipelineInstanceRunner ( final int replicaIndex, final PipelineReplicaRunner pipelineReplicaRunner, final Thread thread )
    {
        checkNotNull( pipelineReplicaRunner );
        checkNotNull( thread );
        checkState( runners[ replicaIndex ] == null );
        checkState( threads[ replicaIndex ] == null );
        runners[ replicaIndex ] = pipelineReplicaRunner;
        threads[ replicaIndex ] = thread;
    }

    public PipelineReplicaRunner getPipelineInstanceRunner ( final int replicaIndex )
    {
        return runners[ replicaIndex ];
    }

    public Thread getPipelineInstanceRunnerThread ( final int replicaIndex )
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
