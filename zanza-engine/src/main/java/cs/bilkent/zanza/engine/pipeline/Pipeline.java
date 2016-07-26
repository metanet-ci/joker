package cs.bilkent.zanza.engine.pipeline;

import java.util.Arrays;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static cs.bilkent.zanza.engine.pipeline.OperatorReplicaStatus.COMPLETED;
import static cs.bilkent.zanza.engine.pipeline.OperatorReplicaStatus.COMPLETING;
import static cs.bilkent.zanza.engine.pipeline.OperatorReplicaStatus.RUNNING;
import cs.bilkent.zanza.engine.region.RegionConfig;
import cs.bilkent.zanza.engine.region.RegionDef;
import cs.bilkent.zanza.flow.OperatorDef;
import cs.bilkent.zanza.operator.scheduling.SchedulingStrategy;

public class Pipeline
{

    private final PipelineId id;

    private final RegionConfig regionConfig;

    private PipelineReplica[] replicas;

    private OperatorReplicaStatus pipelineStatus = RUNNING;

    private OperatorReplicaStatus[] replicaStatuses;

    private SchedulingStrategy initialSchedulingStrategy;

    private PipelineReplicaRunner[] runners;

    private DownstreamTupleSender[] downstreamTupleSenders;

    private volatile UpstreamContext upstreamContext;

    public Pipeline ( final PipelineId id, final RegionConfig regionConfig )
    {
        this.id = id;
        this.regionConfig = regionConfig;
        final int replicaCount = regionConfig.getReplicaCount();
        this.replicas = new PipelineReplica[ replicaCount ];
        replicaStatuses = new OperatorReplicaStatus[ replicaCount ];
        Arrays.fill( replicaStatuses, RUNNING );
        this.runners = new PipelineReplicaRunner[ replicaCount ];
        this.downstreamTupleSenders = new DownstreamTupleSender[ replicaCount ];
    }

    public PipelineId getId ()
    {
        return id;
    }

    public RegionDef getRegionDef ()
    {
        return regionConfig.getRegionDef();
    }

    public OperatorDef getOperatorDef ( int operatorIndex )
    {
        return regionConfig.getOperatorDefs( id.pipelineId )[ operatorIndex ];
    }

    public OperatorDef getFirstOperatorDef ()
    {
        return getOperatorDef( 0 );
    }

    public OperatorDef getLastOperatorDef ()
    {
        final OperatorDef[] operatorDefs = regionConfig.getOperatorDefs( id.pipelineId );
        return regionConfig.getOperatorDefs( id.pipelineId )[ operatorDefs.length - 1 ];
    }

    public int getOperatorCount ()
    {
        return regionConfig.getOperatorDefs( id.pipelineId ).length;
    }

    public OperatorReplicaStatus getPipelineStatus ()
    {
        return pipelineStatus;
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
        checkArgument( this.upstreamContext == null || this.upstreamContext.getVersion() < upstreamContext.getVersion() );
        this.upstreamContext = upstreamContext;
    }

    public int getOperatorIndex ( final OperatorDef operator )
    {
        final OperatorDef[] operatorDefs = regionConfig.getOperatorDefs( id.pipelineId );
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

    public void setPipelineReplicaRunner ( final int replicaIndex, final PipelineReplicaRunner pipelineReplicaRunner )
    {
        checkNotNull( pipelineReplicaRunner );
        checkState( runners[ replicaIndex ] == null );
        runners[ replicaIndex ] = pipelineReplicaRunner;
    }

    public PipelineReplicaRunner getPipelineReplicaRunner ( final int replicaIndex )
    {
        return runners[ replicaIndex ];
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

    public void setPipelineCompleting ()
    {
        checkState( pipelineStatus == RUNNING );
        for ( OperatorReplicaStatus replicaStatus : replicaStatuses )
        {
            checkState( replicaStatus == RUNNING );
        }

        pipelineStatus = COMPLETING;
        for ( int i = 0, j = getReplicaCount(); i < j; i++ )
        {
            replicaStatuses[ i ] = COMPLETING;
        }
    }

    public boolean setPipelineReplicaCompleted ( final int replicaIndex )
    {
        checkState( pipelineStatus == COMPLETING );
        checkState( replicaStatuses[ replicaIndex ] == COMPLETING );
        replicaStatuses[ replicaIndex ] = COMPLETED;
        for ( OperatorReplicaStatus replicaStatus : replicaStatuses )
        {
            if ( replicaStatus != COMPLETED )
            {
                return false;
            }
        }

        pipelineStatus = COMPLETED;
        return true;
    }

}
