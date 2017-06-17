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
import cs.bilkent.joker.engine.flow.RegionExecutionPlan;
import cs.bilkent.joker.engine.metric.PipelineMeter;
import cs.bilkent.joker.engine.metric.PipelineReplicaMeter;
import static cs.bilkent.joker.engine.pipeline.OperatorReplicaStatus.COMPLETED;
import static cs.bilkent.joker.engine.pipeline.OperatorReplicaStatus.COMPLETING;
import static cs.bilkent.joker.engine.pipeline.OperatorReplicaStatus.INITIAL;
import static cs.bilkent.joker.engine.pipeline.OperatorReplicaStatus.RUNNING;
import static cs.bilkent.joker.engine.pipeline.OperatorReplicaStatus.SHUT_DOWN;
import cs.bilkent.joker.engine.pipeline.PipelineReplicaRunner.PipelineReplicaRunnerStatus;
import cs.bilkent.joker.engine.supervisor.Supervisor;
import static cs.bilkent.joker.engine.util.ExceptionUtils.checkInterruption;
import cs.bilkent.joker.operator.OperatorDef;
import cs.bilkent.joker.operator.scheduling.SchedulingStrategy;
import static java.lang.System.arraycopy;
import static java.util.Arrays.fill;

public class Pipeline
{

    private static final Logger LOGGER = LoggerFactory.getLogger( Pipeline.class );


    private final PipelineId id;

    private final RegionExecutionPlan regionExecutionPlan;

    private PipelineReplica[] replicas;

    private OperatorReplicaStatus pipelineStatus;

    private OperatorReplicaStatus[] replicaStatuses;

    private PipelineReplicaRunner[] runners;

    private Thread[] threads;

    private volatile DownstreamTupleSender[] downstreamTupleSenders;

    private PipelineReplicaRunnerStatus runnerStatus;

    private SchedulingStrategy initialSchedulingStrategy;

    private volatile UpstreamContext upstreamContext;

    public Pipeline ( final PipelineId id, final RegionExecutionPlan regionExecutionPlan, final PipelineReplica[] pipelineReplicas )
    {
        this.id = id;
        this.regionExecutionPlan = regionExecutionPlan;
        final int replicaCount = regionExecutionPlan.getReplicaCount();
        this.replicas = new PipelineReplica[ replicaCount ];
        arraycopy( pipelineReplicas, 0, this.replicas, 0, replicaCount );
        this.threads = new Thread[ replicaCount ];
        this.pipelineStatus = INITIAL;
        this.replicaStatuses = new OperatorReplicaStatus[ replicaCount ];
        fill( this.replicaStatuses, INITIAL );
        this.runners = new PipelineReplicaRunner[ replicaCount ];
        this.downstreamTupleSenders = new DownstreamTupleSender[ replicaCount ];
    }

    private Pipeline ( final PipelineId id,
                       final RegionExecutionPlan regionExecutionPlan,
                       final PipelineReplica[] pipelineReplicas,
                       final SchedulingStrategy initialSchedulingStrategy,
                       final UpstreamContext upstreamContext )
    {
        this.id = id;
        this.regionExecutionPlan = regionExecutionPlan;
        final int replicaCount = regionExecutionPlan.getReplicaCount();
        this.replicas = new PipelineReplica[ replicaCount ];
        arraycopy( pipelineReplicas, 0, this.replicas, 0, replicaCount );
        this.threads = new Thread[ replicaCount ];
        this.replicaStatuses = new OperatorReplicaStatus[ replicaCount ];
        this.runners = new PipelineReplicaRunner[ replicaCount ];
        this.downstreamTupleSenders = new DownstreamTupleSender[ replicaCount ];
        this.initialSchedulingStrategy = initialSchedulingStrategy;
        this.upstreamContext = upstreamContext;

        final int highestRunningStatusReplicaIndex = getHighestRunningStatusReplicaIndex();
        if ( highestRunningStatusReplicaIndex == replicas.length - 1 )
        {
            this.pipelineStatus = RUNNING;
            fill( this.replicaStatuses, RUNNING );
        }
        else
        {
            final SchedulingStrategy[] schedulingStrategies = this.replicas[ 0 ].getSchedulingStrategies();
            init( schedulingStrategies, highestRunningStatusReplicaIndex + 1 );
        }
    }

    public static Pipeline createAlreadyRunningPipeline ( final PipelineId id,
                                                          final RegionExecutionPlan regionExecutionPlan,
                                                          final PipelineReplica[] pipelineReplicas,
                                                          final SchedulingStrategy initialSchedulingStrategy,
                                                          final UpstreamContext upstreamContext )
    {
        return new Pipeline( id, regionExecutionPlan, pipelineReplicas, initialSchedulingStrategy, upstreamContext );
    }

    private int getHighestRunningStatusReplicaIndex ()
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

        checkState( replicaIndex > 0, "invalid pipeline status! No running replicas! Pipeline: %s", id );
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

        return replicaIndex - 1;
    }

    public PipelineId getId ()
    {
        return id;
    }

    public RegionDef getRegionDef ()
    {
        return regionExecutionPlan.getRegionDef();
    }

    public OperatorDef getOperatorDef ( final int operatorIndex )
    {
        final OperatorDef[] operatorDefs = regionExecutionPlan.getOperatorDefsByPipelineStartIndex( id.getPipelineStartIndex() );
        return operatorDefs[ operatorIndex ];
    }

    public OperatorDef getFirstOperatorDef ()
    {
        return getOperatorDef( 0 );
    }

    public OperatorDef getLastOperatorDef ()
    {
        final int count = regionExecutionPlan.getOperatorCountByPipelineStartIndex( id.getPipelineStartIndex() );
        return getOperatorDef( count - 1 );
    }

    public int getOperatorCount ()
    {
        return regionExecutionPlan.getOperatorCountByPipelineStartIndex( id.getPipelineStartIndex() );
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
        final OperatorDef[] operatorDefs = regionExecutionPlan.getOperatorDefsByPipelineStartIndex( id.getPipelineStartIndex() );
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

    public void setDownstreamTupleSenders ( final DownstreamTupleSender[] downstreamTupleSenders )
    {
        checkArgument( downstreamTupleSenders != null, "Cannot set null DownstreamTupleSenders for Pipeline %s", id );
        checkArgument( downstreamTupleSenders.length == getReplicaCount(),
                       "Cannot set DownstreamTupleSenders with %s replicas for Pipeline %s with %s replicas",
                       id,
                       downstreamTupleSenders.length,
                       getReplicaCount() );

        for ( int replicaIndex = 0; replicaIndex < getReplicaCount(); replicaIndex++ )
        {
            checkArgument( downstreamTupleSenders[ replicaIndex ] != null,
                           "argument DownstreamTupleSender null for Pipeline %s replicaIndex=%s",
                           id,
                           replicaIndex );
        }

        this.downstreamTupleSenders = Arrays.copyOf( downstreamTupleSenders, downstreamTupleSenders.length );
    }

    public DownstreamTupleSender getDownstreamTupleSender ( final int replicaIndex )
    {
        return downstreamTupleSenders[ replicaIndex ];
    }

    private void init ( final SchedulingStrategy[] initialSchedulingStrategies, final int replicaIndexToInit )
    {
        try
        {
            final SchedulingStrategy[][] schedulingStrategies = new SchedulingStrategy[ getReplicaCount() ][ getOperatorCount() ];
            for ( int replicaIndex = replicaIndexToInit; replicaIndex < getReplicaCount(); replicaIndex++ )
            {
                final PipelineReplica pipelineReplica = replicas[ replicaIndex ];
                LOGGER.debug( "Initializing Replica {} of Pipeline {} with {}", replicaIndex, id, upstreamContext );
                final SchedulingStrategy[] strategies = pipelineReplica.init( upstreamContext );
                arraycopy( strategies, 0, schedulingStrategies[ replicaIndex ], 0, getOperatorCount() );
            }

            for ( int replicaIndex = replicaIndexToInit; replicaIndex < getReplicaCount(); replicaIndex++ )
            {
                checkState( Arrays.equals( initialSchedulingStrategies, schedulingStrategies[ replicaIndex ] ),
                            "Pipeline %s Replica O scheduling strategies: %s, Replica %s scheduling strategies: ",
                            id,
                            schedulingStrategies[ 0 ],
                            replicaIndex,
                            schedulingStrategies[ replicaIndex ] );
            }

            this.initialSchedulingStrategy = initialSchedulingStrategies[ 0 ];
            pipelineStatus = RUNNING;
            fill( replicaStatuses, RUNNING );
        }
        catch ( Exception e )
        {
            checkInterruption( e );
            pipelineStatus = SHUT_DOWN;
            throw e;
        }
    }

    public void init ()
    {
        checkState( pipelineStatus == INITIAL, "cannot initialize pipeline %s since in %s status", id, pipelineStatus );
        checkState( upstreamContext != null, "cannot initialize pipeline %s since upstream context not set", id );

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
            LOGGER.error( "Initialization of Pipeline " + id + " failed!", e1 );
            pipelineStatus = SHUT_DOWN;

            throw new InitializationException( "Initialization of Pipeline " + id + " failed!", e1 );
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
        final OperatorDef[] operatorDefs = regionExecutionPlan.getOperatorDefsByPipelineStartIndex( id.getPipelineStartIndex() );
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
