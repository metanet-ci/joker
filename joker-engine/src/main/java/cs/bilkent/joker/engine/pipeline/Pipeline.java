package cs.bilkent.joker.engine.pipeline;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import cs.bilkent.joker.engine.config.JokerConfig;
import static cs.bilkent.joker.engine.pipeline.OperatorReplicaStatus.COMPLETED;
import static cs.bilkent.joker.engine.pipeline.OperatorReplicaStatus.COMPLETING;
import static cs.bilkent.joker.engine.pipeline.OperatorReplicaStatus.INITIAL;
import static cs.bilkent.joker.engine.pipeline.OperatorReplicaStatus.INITIALIZATION_FAILED;
import static cs.bilkent.joker.engine.pipeline.OperatorReplicaStatus.RUNNING;
import static cs.bilkent.joker.engine.pipeline.OperatorReplicaStatus.SHUT_DOWN;
import cs.bilkent.joker.engine.pipeline.PipelineReplicaRunner.PipelineReplicaRunnerStatus;
import cs.bilkent.joker.engine.region.RegionConfig;
import cs.bilkent.joker.engine.region.RegionDef;
import cs.bilkent.joker.engine.supervisor.Supervisor;
import cs.bilkent.joker.engine.tuplequeue.OperatorTupleQueue;
import cs.bilkent.joker.engine.tuplequeue.impl.context.DefaultOperatorTupleQueue;
import cs.bilkent.joker.operator.OperatorDef;
import cs.bilkent.joker.operator.scheduling.SchedulingStrategy;
import static java.lang.System.arraycopy;
import static java.util.Arrays.fill;

public class Pipeline
{

    private static final Logger LOGGER = LoggerFactory.getLogger( Pipeline.class );


    private final PipelineId id;

    private final RegionConfig regionConfig;

    private PipelineReplica[] replicas;

    private OperatorReplicaStatus pipelineStatus;

    private OperatorReplicaStatus[] replicaStatuses;

    private PipelineReplicaRunner[] runners;

    private Thread[] threads;

    private DownstreamTupleSender[] downstreamTupleSenders;

    private PipelineReplicaRunnerStatus runnerStatus;

    private SchedulingStrategy initialSchedulingStrategy;

    private volatile UpstreamContext upstreamContext;

    public Pipeline ( final PipelineId id, final RegionConfig regionConfig, final PipelineReplica[] pipelineReplicas )
    {
        this.id = id;
        this.regionConfig = regionConfig;
        final int replicaCount = regionConfig.getReplicaCount();
        this.replicas = new PipelineReplica[ replicaCount ];
        arraycopy( pipelineReplicas, 0, this.replicas, 0, replicaCount );
        this.threads = new Thread[ replicaCount ];
        this.pipelineStatus = INITIAL;
        this.replicaStatuses = new OperatorReplicaStatus[ replicaCount ];
        fill( this.replicaStatuses, INITIAL );
        this.runners = new PipelineReplicaRunner[ replicaCount ];
        this.downstreamTupleSenders = new DownstreamTupleSender[ replicaCount ];
    }

    public Pipeline ( final PipelineId id,
                      final RegionConfig regionConfig,
                      final PipelineReplica[] pipelineReplicas,
                      final SchedulingStrategy initialSchedulingStrategy,
                      final UpstreamContext upstreamContext )
    {
        this.id = id;
        this.regionConfig = regionConfig;
        final int replicaCount = regionConfig.getReplicaCount();
        this.replicas = new PipelineReplica[ replicaCount ];
        arraycopy( pipelineReplicas, 0, this.replicas, 0, replicaCount );
        this.threads = new Thread[ replicaCount ];
        this.pipelineStatus = RUNNING;
        replicaStatuses = new OperatorReplicaStatus[ replicaCount ];
        fill( this.replicaStatuses, RUNNING );
        this.runners = new PipelineReplicaRunner[ replicaCount ];
        this.downstreamTupleSenders = new DownstreamTupleSender[ replicaCount ];
        this.initialSchedulingStrategy = initialSchedulingStrategy;
        this.upstreamContext = upstreamContext;
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
        return regionConfig.getOperatorDefByPipelineId( id.pipelineId, operatorIndex );
    }

    public OperatorDef getFirstOperatorDef ()
    {
        return getOperatorDef( 0 );
    }

    public OperatorDef getLastOperatorDef ()
    {
        final int operatorCount = regionConfig.getOperatorCountByPipelineId( id.pipelineId );
        return regionConfig.getOperatorDefByPipelineId( id.pipelineId, operatorCount - 1 );
    }

    public int getOperatorCount ()
    {
        return regionConfig.getOperatorCountByPipelineId( id.pipelineId );
    }

    public OperatorReplicaStatus getPipelineStatus ()
    {
        return pipelineStatus;
    }

    public SchedulingStrategy getInitialSchedulingStrategy ()
    {
        return initialSchedulingStrategy;
    }

    public UpstreamContext getUpstreamContext ()
    {
        return upstreamContext;
    }

    public int getOperatorIndex ( final OperatorDef operator )
    {
        final OperatorDef[] operatorDefs = regionConfig.getOperatorDefsByPipelineId( id.pipelineId );
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

    public PipelineReplica getPipelineReplica ( final int replicaIndex )
    {
        return replicas[ replicaIndex ];
    }

    public void setUpstreamContext ( final UpstreamContext upstreamContext )
    {
        checkArgument( upstreamContext != null, "Cannot set null upstream context of Pipeline %s", id );
        checkArgument( this.upstreamContext == null || this.upstreamContext.getVersion() < upstreamContext.getVersion() );
        this.upstreamContext = upstreamContext;
    }

    public void setDownstreamTupleSender ( final int replicaIndex, final DownstreamTupleSender downstreamTupleSender )
    {
        checkArgument( downstreamTupleSender != null,
                       "Cannot set null DownstreamTupleSender for Pipeline %s replicaIndex=%s",
                       id,
                       replicaIndex );
        checkState( downstreamTupleSenders[ replicaIndex ] == null,
                    "DownstreamTupleSender %s already set for Pipeline %s replicaIndex=%s",
                    downstreamTupleSenders[ replicaIndex ],
                    id,
                    replicaIndex );
        downstreamTupleSenders[ replicaIndex ] = downstreamTupleSender;
    }

    public DownstreamTupleSender getDownstreamTupleSender ( final int replicaIndex )
    {
        return downstreamTupleSenders[ replicaIndex ];
    }

    public void init ()
    {
        checkState( pipelineStatus == INITIAL, "cannot initialize pipeline %s since in %s status", id, pipelineStatus );

        try
        {
            final SchedulingStrategy[][] schedulingStrategies = new SchedulingStrategy[ getReplicaCount() ][ getOperatorCount() ];
            for ( int replicaIndex = 0; replicaIndex < getReplicaCount(); replicaIndex++ )
            {
                final PipelineReplica pipelineReplica = replicas[ replicaIndex ];
                LOGGER.info( "Initializing Replica {} of Pipeline {} with {}", replicaIndex, id, upstreamContext );
                final SchedulingStrategy[] strategies = pipelineReplica.init( upstreamContext );
                arraycopy( strategies, 0, schedulingStrategies[ replicaIndex ], 0, getOperatorCount() );
            }

            for ( int replicaIndex = 1; replicaIndex < getReplicaCount(); replicaIndex++ )
            {
                checkState( Arrays.equals( schedulingStrategies[ 0 ], schedulingStrategies[ replicaIndex ] ),
                            "Pipeline %s Replica O scheduling strategies: %s, Replica %s scheduling strategies: ",
                            id,
                            schedulingStrategies[ 0 ],
                            replicaIndex,
                            schedulingStrategies[ replicaIndex ] );
            }

            this.initialSchedulingStrategy = schedulingStrategies[ 0 ][ 0 ];
            pipelineStatus = RUNNING;
            fill( replicaStatuses, RUNNING );
        }
        catch ( Exception e )
        {
            if ( e.getCause() instanceof InterruptedException )
            {
                Thread.currentThread().interrupt();
            }
            pipelineStatus = INITIALIZATION_FAILED;
            throw e;
        }
    }

    public void handleUpstreamContextUpdated ( final UpstreamContext upstreamContext )
    {
        checkState( pipelineStatus == RUNNING || pipelineStatus == COMPLETING,
                    "Cannot handle updated pipeline upstream since Pipeline %s is in %s status",
                    id,
                    pipelineStatus );

        LOGGER.info( "Updating upstream context of Pipeline {} to {}", id, upstreamContext );

        setUpstreamContext( upstreamContext );
        if ( pipelineStatus == RUNNING && !upstreamContext.isInvokable( getFirstOperatorDef(), initialSchedulingStrategy ) )
        {
            LOGGER.info( "Pipeline {} is not invokable anymore. Setting pipeline status to {}", id, COMPLETING );
            setPipelineCompleting();
        }

        notifyPipelineReplicaRunners( upstreamContext );
    }

    private void setPipelineCompleting ()
    {
        checkState( pipelineStatus == RUNNING,
                    "Pipeline %s cannot move to %s state since it is in %s state",
                    id,
                    COMPLETING,
                    pipelineStatus );

        for ( int i = 0, j = getReplicaCount(); i < j; i++ )
        {
            checkState( replicaStatuses[ i ] == RUNNING,
                        "Pipeline %s cannot move to %s state since replica index %s in %s state",
                        id,
                        COMPLETING,
                        i,
                        replicaStatuses[ i ] );
        }

        fill( replicaStatuses, COMPLETING );
        pipelineStatus = COMPLETING;
    }

    private void notifyPipelineReplicaRunners ( final UpstreamContext upstreamContext )
    {
        LOGGER.info( "Notifying runners about new {} of Pipeline {}", upstreamContext, id );
        for ( int i = 0; i < getReplicaCount(); i++ )
        {
            final PipelineReplicaRunner runner = runners[ i ];
            runner.updatePipelineUpstreamContext();
        }
    }

    public boolean handlePipelineReplicaCompleted ( final int replicaIndex )
    {
        checkState( pipelineStatus == COMPLETING,
                    "Cannot set pipeline replica completed for replicaIndex=%s of Pipeline %s as it is in %s status",
                    replicaIndex,
                    id,
                    pipelineStatus );
        checkState( replicaStatuses[ replicaIndex ] == COMPLETING,
                    "Cannot set pipeline replica completed for replicaIndex=%s as replica is in %s status",
                    replicaIndex,
                    replicaStatuses[ replicaIndex ] );

        replicaStatuses[ replicaIndex ] = COMPLETED;

        LOGGER.info( "Replica {} of pipeline {} is completed.", replicaIndex, id );

        for ( OperatorReplicaStatus replicaStatus : replicaStatuses )
        {
            if ( replicaStatus != COMPLETED )
            {
                return false;
            }
        }

        LOGGER.info( "All replicas of pipeline {} are completed.", id );
        pipelineStatus = COMPLETED;
        return true;
    }

    public void startPipelineReplicaRunners ( final JokerConfig jokerConfig, final Supervisor supervisor, final ThreadGroup threadGroup )
    {
        checkArgument( jokerConfig != null, "cannot start pipeline %s replica runners since config is null", id );
        checkArgument( supervisor != null, "cannot start pipeline %s replica runners since supervisor is null", id );
        checkArgument( threadGroup != null, "cannot start pipeline %s replica runners since threadGroup is null", id );
        checkState( pipelineStatus == RUNNING, "cannot create pipeline %s replica runners since in %s status", id, pipelineStatus );
        checkState( runnerStatus == null, "cannot create pipeline %s replica runners since runner status is %s", id, runnerStatus );

        createPipelineReplicaRunners( jokerConfig, supervisor, threadGroup );
        startPipelineReplicaRunnerThreads();

        runnerStatus = PipelineReplicaRunnerStatus.RUNNING;
    }

    private void createPipelineReplicaRunners ( final JokerConfig jokerConfig, final Supervisor supervisor, final ThreadGroup threadGroup )
    {
        for ( int replicaIndex = 0; replicaIndex < getReplicaCount(); replicaIndex++ )
        {
            final PipelineReplica replica = replicas[ replicaIndex ];
            final DownstreamTupleSender downstreamTupleSender = downstreamTupleSenders[ replicaIndex ];
            final PipelineReplicaRunner runner = new PipelineReplicaRunner( jokerConfig, replica, supervisor, downstreamTupleSender );
            final String threadName = getThreadName( threadGroup, replica );
            final Thread thread = new Thread( threadGroup, runner, threadName );
            setPipelineReplicaRunner( replicaIndex, runner, thread );
        }

        LOGGER.info( "Created runners and threads for pipeline: {}", id );
    }

    private String getThreadName ( final ThreadGroup threadGroup, final PipelineReplica replica )
    {
        return threadGroup.getName() + "-" + replica.id();
    }

    private void setPipelineReplicaRunner ( final int replicaIndex, final PipelineReplicaRunner pipelineReplicaRunner, final Thread thread )
    {
        checkArgument( pipelineReplicaRunner != null,
                       "Cannot set null pipeline replica runner for replicaIndex=%s of Pipeline %s",
                       replicaIndex,
                       id );
        checkState( runners[ replicaIndex ] == null,
                    "Cannot set pipeline replica runner for replicaIndex=%s for Pipeline %s",
                    replicaIndex,
                    id );
        checkState( threads[ replicaIndex ] == null,
                    "Cannot set pipeline replica runner thread for replicaIndex=%s for Pipeline %s",
                    replicaIndex,
                    id );

        runners[ replicaIndex ] = pipelineReplicaRunner;
        threads[ replicaIndex ] = thread;
    }

    private void startPipelineReplicaRunnerThreads ()
    {
        for ( Thread thread : threads )
        {
            thread.start();
        }

        LOGGER.info( "Pipeline {} threads are started", id );
    }

    public List<Exception> stopPipelineReplicaRunners ( final boolean disableCapacityCheck, final long timeoutInMillis )
    {
        checkState( pipelineStatus == RUNNING || pipelineStatus == COMPLETING || pipelineStatus == COMPLETED,
                    "cannot create pipeline %s replica runners since in %s status",
                    id,
                    pipelineStatus );
        checkState( runnerStatus == PipelineReplicaRunnerStatus.RUNNING,
                    "cannot create pipeline %s replica runners since runner status is %s",
                    id,
                    runnerStatus );

        final List<Future<Boolean>> futures = new ArrayList<>();
        final List<Exception> failures = new ArrayList<>();

        for ( int replicaIndex = 0; replicaIndex < getReplicaCount(); replicaIndex++ )
        {
            if ( disableCapacityCheck )
            {
                final PipelineReplica pipelineReplica = replicas[ replicaIndex ];
                final OperatorTupleQueue pipelineTupleQueue = pipelineReplica.getPipelineTupleQueue();
                if ( pipelineTupleQueue instanceof DefaultOperatorTupleQueue )
                {
                    final DefaultOperatorTupleQueue d = (DefaultOperatorTupleQueue) pipelineTupleQueue;
                    for ( int portIndex = 0; portIndex < d.getInputPortCount(); portIndex++ )
                    {
                        LOGGER.warn( "Pipeline {} upstream tuple queue capacity check is disabled", pipelineReplica.id() );
                        d.disableCapacityCheck( portIndex );
                    }
                }
            }

            final PipelineReplicaRunner runner = runners[ replicaIndex ];
            runners[ replicaIndex ] = null;
            futures.add( runner.stop() );
        }

        for ( int replicaIndex = 0; replicaIndex < getReplicaCount(); replicaIndex++ )
        {
            final Future<Boolean> future = futures.get( replicaIndex );
            try
            {
                future.get( timeoutInMillis, TimeUnit.MILLISECONDS );
            }
            catch ( InterruptedException e )
            {
                Thread.currentThread().interrupt();
                LOGGER.error( "Interrupted during waiting for pipeline " + id + " replicaIndex=" + replicaIndex + " runner to stop", e );
                failures.add( e );
            }
            catch ( ExecutionException | TimeoutException e )
            {
                LOGGER.error( "Failed during waiting for pipeline " + id + " replicaIndex=" + replicaIndex + " runner to stop", e );
                failures.add( e );
            }
        }

        LOGGER.info( "Replica runners of Pipeline {} are stopped...", id );

        for ( int replicaIndex = 0; replicaIndex < getReplicaCount(); replicaIndex++ )
        {
            final Thread thread = threads[ replicaIndex ];
            threads[ replicaIndex ] = null;
            try
            {
                thread.join( timeoutInMillis );
            }
            catch ( InterruptedException e )
            {
                Thread.currentThread().interrupt();
                LOGGER.error( "Interrupted during waiting for pipeline " + id + " replicaIndex=" + replicaIndex + " runner to stop", e );
                failures.add( e );
            }
        }

        runnerStatus = PipelineReplicaRunnerStatus.COMPLETED;

        LOGGER.info( "Replica runner threads of Pipeline {} are stopped...", id );

        return failures;
    }

    public void shutdown ()
    {
        checkState( pipelineStatus != INITIAL, "cannot shutdown Pipeline %s since in %s status", id, INITIAL );
        checkState( runnerStatus == null || runnerStatus == PipelineReplicaRunnerStatus.COMPLETED,
                    "cannot shutdown Pipeline %s since runner status is %s",
                    runnerStatus );

        if ( pipelineStatus == SHUT_DOWN )
        {
            LOGGER.warn( "Pipeline {} is already shut down", id );
            return;
        }

        for ( int replicaIndex = 0; replicaIndex < getReplicaCount(); replicaIndex++ )
        {
            final PipelineReplica replica = replicas[ replicaIndex ];
            try
            {
                if ( replica.getStatus() != INITIAL )
                {
                    replica.shutdown();
                }
                else
                {
                    LOGGER.warn( "Did not shutdown Pipeline Replica {} since in {} status", replica.id(), INITIAL );
                }
            }
            catch ( Exception e )
            {
                LOGGER.error( "PipelineReplica " + replica.id() + " releaseRegions failed", e );
            }
        }
    }

}
