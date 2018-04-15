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
import cs.bilkent.joker.engine.exception.InitializationException;
import cs.bilkent.joker.engine.flow.PipelineId;
import cs.bilkent.joker.engine.flow.RegionDef;
import cs.bilkent.joker.engine.flow.RegionExecPlan;
import cs.bilkent.joker.engine.metric.PipelineMeter;
import cs.bilkent.joker.engine.metric.PipelineReplicaMeter;
import static cs.bilkent.joker.engine.pipeline.OperatorReplicaStatus.COMPLETED;
import static cs.bilkent.joker.engine.pipeline.OperatorReplicaStatus.COMPLETING;
import static cs.bilkent.joker.engine.pipeline.OperatorReplicaStatus.INITIAL;
import static cs.bilkent.joker.engine.pipeline.OperatorReplicaStatus.RUNNING;
import static cs.bilkent.joker.engine.pipeline.OperatorReplicaStatus.SHUT_DOWN;
import cs.bilkent.joker.engine.pipeline.PipelineReplicaRunner.PipelineReplicaRunnerStatus;
import cs.bilkent.joker.engine.region.Region;
import cs.bilkent.joker.engine.supervisor.Supervisor;
import static cs.bilkent.joker.engine.util.ExceptionUtils.checkInterruption;
import cs.bilkent.joker.operator.OperatorDef;
import cs.bilkent.joker.operator.scheduling.SchedulingStrategy;
import static java.util.Arrays.fill;

public class Pipeline
{

    private static final Logger LOGGER = LoggerFactory.getLogger( Pipeline.class );


    private final PipelineId id;

    private final RegionExecPlan regionExecPlan;

    private final SchedulingStrategy[] operatorSchedulingStrategies;

    private final SchedulingStrategy[][] fusedOperatorSchedulingStrategies;

    private final UpstreamCtx[] operatorUpstreamCtxes;

    private final UpstreamCtx[][] fusedOperatorUpstreamCtxes;

    private final PipelineReplica[] replicas;

    private OperatorReplicaStatus pipelineStatus;

    private OperatorReplicaStatus[] replicaStatuses;

    private PipelineReplicaRunner[] runners;

    private Thread[] threads;

    private volatile DownstreamCollector[] downstreamCollectors;

    private PipelineReplicaRunnerStatus runnerStatus;

    private volatile UpstreamCtx upstreamCtx;

    public Pipeline ( final PipelineId id, final Region region )
    {
        this.id = id;
        this.regionExecPlan = region.getExecPlan();
        this.operatorSchedulingStrategies = region.getSchedulingStrategies( id );
        this.fusedOperatorSchedulingStrategies = region.getFusedSchedulingStrategies( id );
        this.operatorUpstreamCtxes = region.getUpstreamCtxes( id );
        this.fusedOperatorUpstreamCtxes = region.getFusedUpstreamCtxes( id );
        this.replicas = region.getPipelineReplicas( id );
        final int replicaCount = regionExecPlan.getReplicaCount();
        this.threads = new Thread[ replicaCount ];
        this.pipelineStatus = INITIAL;
        this.replicaStatuses = new OperatorReplicaStatus[ replicaCount ];
        fill( this.replicaStatuses, INITIAL );
        this.runners = new PipelineReplicaRunner[ replicaCount ];
        this.downstreamCollectors = new DownstreamCollector[ replicaCount ];
    }

    public PipelineId getId ()
    {
        return id;
    }

    public RegionDef getRegionDef ()
    {
        return regionExecPlan.getRegionDef();
    }

    public OperatorDef getOperatorDef ( final int operatorIndex )
    {
        final OperatorDef[] operatorDefs = regionExecPlan.getOperatorDefsByPipelineStartIndex( id.getPipelineStartIndex() );
        return operatorDefs[ operatorIndex ];
    }

    public OperatorDef getFirstOperatorDef ()
    {
        return getOperatorDef( 0 );
    }

    public OperatorDef getLastOperatorDef ()
    {
        final int count = regionExecPlan.getOperatorCountByPipelineStartIndex( id.getPipelineStartIndex() );
        return getOperatorDef( count - 1 );
    }

    public int getOperatorCount ()
    {
        return regionExecPlan.getOperatorCountByPipelineStartIndex( id.getPipelineStartIndex() );
    }

    public OperatorReplicaStatus getPipelineStatus ()
    {
        return pipelineStatus;
    }

    public UpstreamCtx getUpstreamCtx ()
    {
        return upstreamCtx;
    }

    public int getOperatorIndex ( final OperatorDef operator )
    {
        final OperatorDef[] operatorDefs = regionExecPlan.getOperatorDefsByPipelineStartIndex( id.getPipelineStartIndex() );
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

    private void setUpstreamCtx ( final UpstreamCtx upstreamCtx )
    {
        checkArgument( upstreamCtx != null, "Cannot set null upstream context of Pipeline: %s", id );
        checkArgument( this.upstreamCtx == null || this.upstreamCtx.getVersion() < upstreamCtx.getVersion(),
                       "Cannot set new: %s for current : of Pipeline: %s",
                       upstreamCtx,
                       this.upstreamCtx,
                       id );
        this.upstreamCtx = upstreamCtx;
    }

    public void setDownstreamCollectors ( final DownstreamCollector[] downstreamCollectors )
    {
        checkArgument( downstreamCollectors != null, "Cannot set null downstream collectors for Pipeline %s", id );
        checkArgument( downstreamCollectors.length == getReplicaCount(),
                       "Cannot set downstream collectors with %s replicas for Pipeline %s with %s replicas",
                       id,
                       downstreamCollectors.length,
                       getReplicaCount() );

        for ( int replicaIndex = 0; replicaIndex < getReplicaCount(); replicaIndex++ )
        {
            checkArgument( downstreamCollectors[ replicaIndex ] != null, "downstream collector null for Pipeline %s replicaIndex=%s",
                           id,
                           replicaIndex );
        }

        this.downstreamCollectors = Arrays.copyOf( downstreamCollectors, downstreamCollectors.length );
    }

    public DownstreamCollector getDownstreamCollector ( final int replicaIndex )
    {
        return downstreamCollectors[ replicaIndex ];
    }

    public void init ()
    {
        final int replicaIndexToInit = getReplicaIndexToInit();
        try
        {
            for ( int replicaIndex = replicaIndexToInit; replicaIndex < getReplicaCount(); replicaIndex++ )
            {
                checkState( replicaStatuses[ replicaIndex ] == INITIAL );
                final PipelineReplica pipelineReplica = replicas[ replicaIndex ];
                LOGGER.debug( "Initializing Replica {} of Pipeline {} with {}", replicaIndex, id, upstreamCtx );
                pipelineReplica.init( fusedOperatorSchedulingStrategies, fusedOperatorUpstreamCtxes );
            }

            setUpstreamCtx( operatorUpstreamCtxes[ 0 ] );
            pipelineStatus = RUNNING;
            fill( replicaStatuses, RUNNING );
        }
        catch ( Exception e1 )
        {
            checkInterruption( e1 );
            for ( int replicaIndex = 0; replicaIndex < getReplicaCount(); replicaIndex++ )
            {
                final PipelineReplica pipelineReplica = replicas[ replicaIndex ];
                try
                {
                    LOGGER.warn( "Shutting down Replica {} of Pipeline {}", replicaIndex, id );
                    pipelineReplica.shutdown();
                }
                catch ( Exception e2 )
                {
                    checkInterruption( e2 );

                    LOGGER.error( "Shutdown of PipelineReplica=" + pipelineReplica.id() + "  failed!", e2 );
                }
            }

            pipelineStatus = SHUT_DOWN;

            throw new InitializationException( "Initialization of Pipeline " + id + " failed!", e1 );
        }
    }

    private int getReplicaIndexToInit ()
    {
        int replicaIndex = 0;
        for ( ; replicaIndex < getReplicaCount(); replicaIndex++ )
        {
            final OperatorReplicaStatus status = replicas[ replicaIndex ].getStatus();
            if ( status == INITIAL )
            {
                break;
            }

            checkState( status == RUNNING,
                        "invalid pipeline initialization status! Pipeline: %s replicaIndex is in %s status",
                        id,
                        replicaIndex,
                        status );
        }

        for ( int i = replicaIndex; i < getReplicaCount(); i++ )
        {
            final OperatorReplicaStatus status = replicas[ i ].getStatus();
            checkState( status == INITIAL,
                        "invalid pipeline status! Pipeline: %s replica index: %s should be %s but it is %s",
                        id,
                        i,
                        INITIAL,
                        status );
        }

        return replicaIndex;
    }

    public void handleUpstreamCtxUpdated ( final UpstreamCtx upstreamCtx )
    {
        checkState( pipelineStatus == RUNNING || pipelineStatus == COMPLETING,
                    "Cannot handle updated pipeline upstream since Pipeline %s is in %s status",
                    id,
                    pipelineStatus );

        LOGGER.info( "Updating upstream context of Pipeline {} to {}", id, upstreamCtx );

        setUpstreamCtx( upstreamCtx );
        if ( pipelineStatus == RUNNING && !upstreamCtx.isInvokable( getFirstOperatorDef(), operatorSchedulingStrategies[ 0 ] ) )
        {
            LOGGER.info( "Pipeline {} is not invokable anymore. Setting pipeline status to {}", id, COMPLETING );
            setPipelineCompleting();
        }

        notifyPipelineReplicaRunners();
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

    public void notifyPipelineReplicaRunners ()
    {
        for ( int i = 0; i < getReplicaCount(); i++ )
        {
            final PipelineReplicaRunner runner = runners[ i ];
            runner.refresh();
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
            final DownstreamCollector downstreamCollector = downstreamCollectors[ replicaIndex ];
            final PipelineReplicaRunner runner = new PipelineReplicaRunner( jokerConfig, replica, supervisor, downstreamCollector );
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

    public List<Exception> pausePipelineReplicaRunners ( final long timeoutInMillis )
    {
        checkState( pipelineStatus == RUNNING, "cannot pause pipeline %s replica runners since in %s status", id, pipelineStatus );
        checkState( runnerStatus == PipelineReplicaRunnerStatus.RUNNING || runnerStatus == PipelineReplicaRunnerStatus.PAUSED,
                    "cannot pause pipeline %s replica runners since runner status is %s",
                    id,
                    runnerStatus );

        final List<Future<Boolean>> futures = new ArrayList<>();

        for ( int replicaIndex = 0; replicaIndex < getReplicaCount(); replicaIndex++ )
        {
            final PipelineReplicaRunner runner = runners[ replicaIndex ];
            futures.add( runner.pause() );
        }

        final List<Exception> failures = waitForPipelineReplicaCommands( futures, timeoutInMillis, "pause" );

        LOGGER.info( "Replica runners of Pipeline {} are paused...", id );

        runnerStatus = PipelineReplicaRunnerStatus.PAUSED;

        return failures;
    }

    public List<Exception> resumePipelineReplicaRunners ( final long timeoutInMillis )
    {
        checkState( pipelineStatus == RUNNING, "cannot resume pipeline %s replica runners since in %s status", id, pipelineStatus );
        checkState( runnerStatus == PipelineReplicaRunnerStatus.RUNNING || runnerStatus == PipelineReplicaRunnerStatus.PAUSED,
                    "cannot resume pipeline %s replica runners since runner status is %s",
                    id,
                    runnerStatus );

        final List<Future<Boolean>> futures = new ArrayList<>();

        for ( int replicaIndex = 0; replicaIndex < getReplicaCount(); replicaIndex++ )
        {
            final PipelineReplicaRunner runner = runners[ replicaIndex ];
            futures.add( runner.resume() );
        }

        final List<Exception> failures = waitForPipelineReplicaCommands( futures, timeoutInMillis, "resume" );

        LOGGER.info( "Replica runners of Pipeline {} are resumed...", id );

        runnerStatus = PipelineReplicaRunnerStatus.RUNNING;

        return failures;
    }

    public PipelineMeter getPipelineMeter ()
    {
        final int replicaCount = getReplicaCount();
        final OperatorDef[] operatorDefs = regionExecPlan.getOperatorDefsByPipelineStartIndex( id.getPipelineStartIndex() );
        final long[] threadIds = new long[ replicaCount ];
        final PipelineReplicaMeter[] replicaMeters = new PipelineReplicaMeter[ replicaCount ];

        for ( int replicaIndex = 0; replicaIndex < replicaCount; replicaIndex++ )
        {
            threadIds[ replicaIndex ] = threads[ replicaIndex ].getId();
            replicaMeters[ replicaIndex ] = replicas[ replicaIndex ].getMeter();
        }

        return new PipelineMeter( id, operatorDefs, replicaCount, threadIds, replicaMeters );
    }

    public List<Exception> stopPipelineReplicaRunners ( final long timeoutInMillis )
    {
        checkState( pipelineStatus == RUNNING || pipelineStatus == COMPLETING || pipelineStatus == COMPLETED,
                    "cannot stop pipeline %s replica runners since in %s status",
                    id,
                    pipelineStatus );
        checkState( runnerStatus == PipelineReplicaRunnerStatus.RUNNING,
                    "cannot stop pipeline %s replica runners since runner status is %s",
                    id,
                    runnerStatus );

        final List<Future<Boolean>> futures = new ArrayList<>();

        for ( int replicaIndex = 0; replicaIndex < getReplicaCount(); replicaIndex++ )
        {
            final PipelineReplicaRunner runner = runners[ replicaIndex ];
            runners[ replicaIndex ] = null;
            futures.add( runner.stop() );
        }

        final List<Exception> failures = waitForPipelineReplicaCommands( futures, timeoutInMillis, "stop" );

        LOGGER.info( "Replica runners of Pipeline {} are stopped...", id );

        joinPipelineReplicaRunnerThreads( timeoutInMillis, failures );

        runnerStatus = PipelineReplicaRunnerStatus.COMPLETED;

        if ( failures.isEmpty() )
        {
            LOGGER.info( "Replica runner threads of Pipeline {} are stopped...", id );
        }
        else
        {
            for ( Exception failure : failures )
            {
                LOGGER.error( "Replica runner threads of Pipeline " + id + "  are stopped...", failure );
            }
        }

        return failures;
    }

    private List<Exception> waitForPipelineReplicaCommands ( final List<Future<Boolean>> futures,
                                                             final long timeoutInMillis,
                                                             final String command )
    {
        final List<Exception> failures = new ArrayList<>();

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
                LOGGER.error( "Interrupted during waiting for pipeline " + id + " replicaIndex=" + replicaIndex + " runner to " + command,
                              e );
                failures.add( e );
            }
            catch ( ExecutionException | TimeoutException e )
            {
                LOGGER.error( "Failed during waiting for pipeline " + id + " replicaIndex=" + replicaIndex + " runner to " + command, e );
                failures.add( e );
            }
        }

        return failures;
    }

    private void joinPipelineReplicaRunnerThreads ( final long timeoutInMillis, final List<Exception> failures )
    {
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
