package cs.bilkent.joker.engine.pipeline.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import javax.annotation.concurrent.NotThreadSafe;
import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;

import org.agrona.concurrent.OneToOneConcurrentArrayQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static cs.bilkent.joker.JokerModule.DOWNSTREAM_FAILURE_FLAG_NAME;
import cs.bilkent.joker.engine.FlowStatus;
import static cs.bilkent.joker.engine.FlowStatus.RUNNING;
import cs.bilkent.joker.engine.config.JokerConfig;
import static cs.bilkent.joker.engine.config.JokerConfig.JOKER_THREAD_GROUP_NAME;
import cs.bilkent.joker.engine.exception.InitializationException;
import cs.bilkent.joker.engine.exception.JokerException;
import cs.bilkent.joker.engine.flow.FlowExecPlan;
import cs.bilkent.joker.engine.flow.PipelineId;
import cs.bilkent.joker.engine.flow.RegionDef;
import cs.bilkent.joker.engine.flow.RegionExecPlan;
import cs.bilkent.joker.engine.metric.LatencyMeter;
import cs.bilkent.joker.engine.metric.MetricManager;
import cs.bilkent.joker.engine.metric.PipelineMeter;
import cs.bilkent.joker.engine.metric.PipelineReplicaMeter;
import cs.bilkent.joker.engine.partition.PartitionDistribution;
import cs.bilkent.joker.engine.partition.PartitionKeyExtractor;
import cs.bilkent.joker.engine.partition.PartitionKeyExtractorFactory;
import cs.bilkent.joker.engine.partition.PartitionService;
import cs.bilkent.joker.engine.pipeline.DownstreamCollector;
import cs.bilkent.joker.engine.pipeline.OperatorReplicaStatus;
import cs.bilkent.joker.engine.pipeline.Pipeline;
import cs.bilkent.joker.engine.pipeline.PipelineManager;
import cs.bilkent.joker.engine.pipeline.PipelineReplica;
import cs.bilkent.joker.engine.pipeline.PipelineReplicaId;
import cs.bilkent.joker.engine.pipeline.UpstreamCtx;
import cs.bilkent.joker.engine.pipeline.UpstreamCtx.ConnectionStatus;
import static cs.bilkent.joker.engine.pipeline.UpstreamCtx.ConnectionStatus.CLOSED;
import static cs.bilkent.joker.engine.pipeline.UpstreamCtx.ConnectionStatus.OPEN;
import static cs.bilkent.joker.engine.pipeline.UpstreamCtx.createShutdownSourceUpstreamCtx;
import cs.bilkent.joker.engine.pipeline.impl.downstreamcollector.CompositeDownstreamCollector;
import cs.bilkent.joker.engine.pipeline.impl.downstreamcollector.DownstreamCollector1;
import cs.bilkent.joker.engine.pipeline.impl.downstreamcollector.DownstreamCollectorN;
import cs.bilkent.joker.engine.pipeline.impl.downstreamcollector.PartitionedDownstreamCollector1;
import cs.bilkent.joker.engine.pipeline.impl.downstreamcollector.PartitionedDownstreamCollectorN;
import cs.bilkent.joker.engine.region.Region;
import cs.bilkent.joker.engine.region.RegionManager;
import cs.bilkent.joker.engine.supervisor.Supervisor;
import cs.bilkent.joker.engine.tuplequeue.OperatorQueue;
import static cs.bilkent.joker.engine.util.RegionUtils.getFirstOperator;
import cs.bilkent.joker.engine.util.concurrent.BackoffIdleStrategy;
import static cs.bilkent.joker.engine.util.concurrent.BackoffIdleStrategy.newDefaultInstance;
import cs.bilkent.joker.engine.util.concurrent.IdleStrategy;
import cs.bilkent.joker.flow.FlowDef;
import cs.bilkent.joker.flow.Port;
import cs.bilkent.joker.operator.OperatorDef;
import cs.bilkent.joker.operator.Tuple;
import cs.bilkent.joker.operator.Tuple.LatencyRecord;
import cs.bilkent.joker.operator.impl.TuplesImpl;
import static cs.bilkent.joker.operator.spec.OperatorType.PARTITIONED_STATEFUL;
import static cs.bilkent.joker.operator.spec.OperatorType.STATEFUL;
import static cs.bilkent.joker.operator.spec.OperatorType.STATELESS;
import cs.bilkent.joker.operator.utils.Pair;
import static java.lang.Math.min;
import static java.util.Collections.reverse;
import static java.util.Collections.singletonList;
import static java.util.Collections.sort;
import static java.util.Comparator.comparing;
import static java.util.Comparator.comparingInt;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

@Singleton
@NotThreadSafe
public class PipelineManagerImpl implements PipelineManager
{

    private static final Logger LOGGER = LoggerFactory.getLogger( PipelineManagerImpl.class );
    private static final int DOWNSTREAM_TUPLE_SENDER_CONSTRUCTOR_COUNT = 2;
    private static final int INITIAL_FLOW_VERSION = -1;


    private final JokerConfig jokerConfig;
    private final RegionManager regionManager;
    private final PartitionService partitionService;
    private final PartitionKeyExtractorFactory partitionKeyExtractorFactory;
    private final MetricManager metricManager;
    private final AtomicBoolean downstreamCollectorFailureFlag;
    private final ThreadGroup jokerThreadGroup;
    private final BiFunction<List<Pair<Integer, Integer>>, OperatorQueue, DownstreamCollector>[] defaultDownstreamCollectorCtors = new BiFunction[ 6 ];
    private final Function6<List<Pair<Integer, Integer>>, Integer, int[], OperatorQueue[], PartitionKeyExtractor, DownstreamCollector>[]
            partitionedDownstreamCollectorCtors = new Function6[ 6 ];
    private final int latencyRecorderPoolSize;
    private final ExecutorService latencyRecorderPool;
    private final OneToOneConcurrentArrayQueue[] latencyRecorderQueues;

    private Supervisor supervisor;
    private int flowVersion = INITIAL_FLOW_VERSION;
    private FlowDef flow;
    private final Map<Integer, RegionExecPlan> regionExecPlans = new HashMap<>();
    private final Map<PipelineId, Pipeline> pipelines = new ConcurrentHashMap<>();

    private volatile FlowStatus status = FlowStatus.INITIAL;

    @Inject
    public PipelineManagerImpl ( final JokerConfig jokerConfig,
                                 final RegionManager regionManager,
                                 final PartitionService partitionService,
                                 final PartitionKeyExtractorFactory partitionKeyExtractorFactory,
                                 final MetricManager metricManager,
                                 @Named( DOWNSTREAM_FAILURE_FLAG_NAME ) final AtomicBoolean downstreamCollectorFailureFlag,
                                 @Named( JOKER_THREAD_GROUP_NAME ) final ThreadGroup jokerThreadGroup )
    {
        this.jokerConfig = jokerConfig;
        this.regionManager = regionManager;
        this.partitionService = partitionService;
        this.partitionKeyExtractorFactory = partitionKeyExtractorFactory;
        this.metricManager = metricManager;
        this.downstreamCollectorFailureFlag = downstreamCollectorFailureFlag;
        this.jokerThreadGroup = jokerThreadGroup;
        this.latencyRecorderPoolSize = jokerConfig.getPipelineManagerConfig().getLatencyRecorderPoolSize();
        this.latencyRecorderPool = newFixedThreadPool( latencyRecorderPoolSize );
        this.latencyRecorderQueues = new OneToOneConcurrentArrayQueue[ latencyRecorderPoolSize ];
        IntStream.range( 0, latencyRecorderPoolSize ).forEach( i -> latencyRecorderQueues[ i ] = new OneToOneConcurrentArrayQueue( 4096 ) );
        createDownstreamCollectorFactories();
    }

    @Inject
    public void setSupervisor ( final Supervisor supervisor )
    {
        this.supervisor = supervisor;
    }

    private void createDownstreamCollectorFactories ()
    {
        defaultDownstreamCollectorCtors[ 1 ] = ( pairs, tupleQueue ) -> {
            final Pair<Integer, Integer> pair1 = pairs.get( 0 );
            return new DownstreamCollector1( downstreamCollectorFailureFlag, pair1._1, pair1._2, tupleQueue );
        };
        defaultDownstreamCollectorCtors[ 2 ] = ( pairs, tupleQueue ) -> {
            final int[] sourcePorts = new int[ pairs.size() ];
            final int[] destinationPorts = new int[ pairs.size() ];
            copyPorts( pairs, sourcePorts, destinationPorts );
            return new DownstreamCollectorN( downstreamCollectorFailureFlag, sourcePorts, destinationPorts, tupleQueue );
        };
        partitionedDownstreamCollectorCtors[ 1 ] = ( pairs, partitionCount, partitionDistribution, tupleQueues, partitionKeyFunction ) -> {
            final Pair<Integer, Integer> pair1 = pairs.get( 0 );
            return new PartitionedDownstreamCollector1( downstreamCollectorFailureFlag,
                                                        pair1._1,
                                                        pair1._2,
                                                        partitionCount,
                                                        partitionDistribution,
                                                        tupleQueues,
                                                        partitionKeyFunction );
        };
        partitionedDownstreamCollectorCtors[ 2 ] = ( pairs, partitionCount, partitionDistribution, tupleQueues, partitionKeyFunction ) -> {
            final int[] sourcePorts = new int[ pairs.size() ];
            final int[] destinationPorts = new int[ pairs.size() ];
            copyPorts( pairs, sourcePorts, destinationPorts );
            return new PartitionedDownstreamCollectorN( downstreamCollectorFailureFlag,
                                                        sourcePorts,
                                                        destinationPorts,
                                                        partitionCount,
                                                        partitionDistribution,
                                                        tupleQueues,
                                                        partitionKeyFunction );
        };
    }

    public List<Pipeline> getPipelines ()
    {
        return new ArrayList<>( pipelines.values() );
    }

    List<Pipeline> createPipelines ( final FlowDef flow, final List<RegionExecPlan> regionExecPlans )
    {
        final List<Pipeline> p = new ArrayList<>();
        for ( RegionExecPlan regionExecPlan : regionExecPlans )
        {
            LOGGER.info( "Initializing regionId={} with operators: {} and {} pipelines ( {} ) and {} replicas",
                         regionExecPlan.getRegionId(),
                         regionExecPlan.getRegionDef().getOperators().stream().map( OperatorDef::getId ).collect( toList() ),
                         regionExecPlan.getPipelineCount(),
                         regionExecPlan.getPipelineStartIndices(),
                         regionExecPlan.getReplicaCount() );

            final Region region = regionManager.createRegion( flow, regionExecPlan );
            this.regionExecPlans.put( region.getRegionId(), regionExecPlan );
            for ( int pipelineIndex = 0; pipelineIndex < regionExecPlan.getPipelineCount(); pipelineIndex++ )
            {
                final PipelineReplica[] pipelineReplicas = region.getPipelineReplicas( pipelineIndex );
                final PipelineId pipelineId = pipelineReplicas[ 0 ].id().pipelineId;
                final Pipeline pipeline = new Pipeline( pipelineId, region );
                addPipeline( pipeline );
                p.add( pipeline );
            }

            LOGGER.info( "regionId={} is created", regionExecPlan.getRegionId() );
        }

        for ( Pipeline pipeline : pipelines.values() )
        {
            createDownstreamCollectors( flow, pipeline );
        }

        return p;
    }

    @Override
    public void start ( final FlowDef flow, final List<RegionExecPlan> regionExecPlans )
    {
        try
        {
            checkState( status == FlowStatus.INITIAL, "cannot start pipeline replica runner threads since status is %s", status );
            this.flow = flow;
            createPipelines( flow, regionExecPlans );
            initPipelines();
            startPipelineReplicaRunners( supervisor );
            status = RUNNING;
            IntStream.range( 0, latencyRecorderPoolSize ).forEach( i -> {
                latencyRecorderPool.submit( () -> recordLatencies( latencyRecorderQueues[ i ], newDefaultInstance() ) );
            } );
            incrementFlowVersion();
        }
        catch ( Exception e )
        {
            status = FlowStatus.INITIALIZATION_FAILED;
            throw new InitializationException( "Flow start failed", e );
        }
    }

    @Override
    public FlowExecPlan getFlowExecPlan ()
    {
        final FlowStatus status = this.status;
        checkState( status == RUNNING || status == FlowStatus.SHUTTING_DOWN, "cannot get flow execution plan since status is %s", status );
        return new FlowExecPlan( flowVersion, flow, regionExecPlans.values() );
    }

    @Override
    public List<PipelineMeter> getAllPipelineMetersOrFail ()
    {
        final List<PipelineMeter> pipelineMeters = pipelines.values().stream().map( Pipeline::getPipelineMeter ).collect( toList() );
        checkState( pipelineMeters.size() > 0, "there is no pipeline meters!" );
        return pipelineMeters;
    }

    @Override
    public List<PipelineMeter> getRegionPipelineMetersOrFail ( final int regionId )
    {
        final List<PipelineMeter> meters = pipelines.values()
                                                    .stream()
                                                    .filter( p -> p.getRegionDef().getRegionId() == regionId )
                                                    .map( Pipeline::getPipelineMeter )
                                                    .collect( toList() );
        checkArgument( meters.size() > 0, "No pipelines found for regionId=%s", regionId );
        return meters;
    }

    @Override
    public PipelineMeter getPipelineMeterOrFail ( final PipelineId pipelineId )
    {
        final Pipeline pipeline = pipelines.get( pipelineId );
        checkArgument( pipeline != null, "Pipeline %s not found", pipelineId );

        return pipeline.getPipelineMeter();
    }

    private void initPipelines ()
    {
        final List<Pipeline> initialized = new ArrayList<>();
        try
        {
            for ( Pipeline pipeline : pipelines.values() )
            {
                initialized.add( pipeline );
                pipeline.init();
            }
        }
        catch ( Exception e1 )
        {
            LOGGER.error( "Pipeline initialization failed.", e1 );
            reverse( initialized );
            LOGGER.error( "Shutting down {} pipelines.", initialized.size() );
            for ( Pipeline pipeline : initialized )
            {
                try
                {
                    LOGGER.info( "Shutting down Pipeline {}", pipeline.getId() );
                    pipeline.shutdown();
                }
                catch ( Exception e2 )
                {
                    LOGGER.error( "Shutdown of Pipeline " + pipeline.getId() + " failed", e2 );
                }
            }

            releaseRegions();

            throw e1;
        }
    }

    @Override
    public UpstreamCtx getUpstreamCtx ( final PipelineReplicaId id )
    {
        final Pipeline pipeline = getPipelineOrFail( id.pipelineId );
        return pipeline.getUpstreamCtx();
    }

    @Override
    public DownstreamCollector getDownstreamCollector ( final PipelineReplicaId id )
    {
        final Pipeline pipeline = getPipelineOrFail( id.pipelineId );
        return pipeline.getDownstreamCollector( id.replicaIndex );
    }

    @Override
    public boolean handlePipelineReplicaCompleted ( final PipelineReplicaId id )
    {
        checkState( status == FlowStatus.SHUTTING_DOWN, "cannot notify Pipeline Replica %s completion since status is %s", id, status );

        final Pipeline pipeline = getPipelineOrFail( id.pipelineId );

        if ( pipeline.handlePipelineReplicaCompleted( id.replicaIndex ) )
        {
            for ( Pipeline downstreamPipeline : getDownstreamPipelines( pipeline ) )
            {
                final UpstreamCtx updatedUpstreamCtx = getUpdatedUpstreamCtx( downstreamPipeline );
                if ( updatedUpstreamCtx != null )
                {
                    downstreamPipeline.handleUpstreamCtxUpdated( updatedUpstreamCtx );
                }
                else
                {
                    LOGGER.info( "Upstream Pipeline {} is completed but {} of Downstream Pipeline {} is same.",
                                 id,
                                 downstreamPipeline.getUpstreamCtx(),
                                 downstreamPipeline.getId() );
                }
            }
        }

        return gracefullyShutdownIfAllPipelinesCompleted();
    }

    private Collection<Pipeline> getDownstreamPipelines ( final Pipeline upstreamPipeline )
    {
        return flow.getOutboundConnections( upstreamPipeline.getLastOperatorDef().getId() )
                   .values()
                   .stream()
                   .flatMap( ports -> ports.stream().map( Port::getOperatorId ) )
                   .distinct()
                   .map( flow::getOperator )
                   .map( this::getPipelineByFirstOperatorOrFail )
                   .collect( toList() );
    }

    private UpstreamCtx getUpdatedUpstreamCtx ( final Pipeline pipeline )
    {
        final ConnectionStatus[] connectionStatuses = getUpstreamConnectionStatuses( pipeline );
        final UpstreamCtx currentCtx = pipeline.getUpstreamCtx();
        UpstreamCtx newCtx = currentCtx;
        for ( int i = 0; i < connectionStatuses.length; i++ )
        {
            if ( connectionStatuses[ i ] == CLOSED )
            {
                newCtx = newCtx.withConnectionClosed( i );
            }
        }

        return newCtx != currentCtx ? newCtx : null;
    }

    private ConnectionStatus[] getUpstreamConnectionStatuses ( final Pipeline pipeline )
    {
        final OperatorDef firstOperator = pipeline.getFirstOperatorDef();
        final ConnectionStatus[] statuses = new ConnectionStatus[ firstOperator.getInputPortCount() ];
        for ( Entry<Port, Set<Port>> entry : flow.getInboundConnections( firstOperator.getId() ).entrySet() )
        {
            final Collection<Port> upstreamPorts = entry.getValue();
            final ConnectionStatus status;
            if ( upstreamPorts.isEmpty() )
            {
                status = CLOSED;
            }
            else
            {
                final List<Pair<OperatorDef, Pipeline>> upstream = upstreamPorts.stream()
                                                                                .map( p -> flow.getOperator( p.getOperatorId() ) )
                                                                                .map( o -> Pair.of( o, getPipelineOrFail( o ) ) )
                                                                                .collect( toList() );

                upstream.forEach( p -> checkState( p._2.getOperatorIndex( p._1 ) == p._2.getOperatorCount() - 1 ) );

                final boolean aliveConnPresent = upstream.stream().anyMatch( p -> {
                    final OperatorReplicaStatus s = p._2.getPipelineStatus();
                    return s == OperatorReplicaStatus.INITIAL || s == OperatorReplicaStatus.RUNNING
                           || s == OperatorReplicaStatus.COMPLETING;
                } );
                status = aliveConnPresent ? OPEN : CLOSED;
            }

            statuses[ entry.getKey().getPortIndex() ] = status;
        }

        return statuses;
    }

    private boolean gracefullyShutdownIfAllPipelinesCompleted ()
    {
        for ( Pipeline pipeline : pipelines.values() )
        {
            if ( pipeline.getPipelineStatus() != OperatorReplicaStatus.COMPLETED )
            {
                return false;
            }
        }

        LOGGER.info( "All pipelines completed." );
        shutdownGracefully( null );

        return true;
    }

    @Override
    public void triggerShutdown ()
    {
        checkState( status == RUNNING, "cannot trigger shutdown since status is %s", status );
        LOGGER.info( "Shutdown request is being handled." );

        status = FlowStatus.SHUTTING_DOWN;

        for ( Pipeline pipeline : pipelines.values() )
        {
            final OperatorDef operatorDef = pipeline.getFirstOperatorDef();
            if ( flow.isSourceOperator( operatorDef.getId() ) )
            {
                pipeline.handleUpstreamCtxUpdated( createShutdownSourceUpstreamCtx() );
            }
        }
    }

    @Override
    public void mergePipelines ( final int flowVersion, final List<PipelineId> pipelineIds )
    {
        checkArgument( flowVersion == this.flowVersion,
                       "cannot merge pipelines %s since given flow version %s is not equal to the flow version: %s",
                       pipelineIds,
                       flowVersion,
                       this.flowVersion );
        checkState( status == RUNNING,
                    "cannot merge pipelines %s with flow version %s since status is %s",
                    pipelineIds,
                    flowVersion,
                    status );

        LOGGER.info( "Will try to merge pipelines {} with flow version: {}", pipelineIds, flowVersion );

        regionManager.validatePipelineMergeParameters( pipelineIds );

        final PipelineId firstPipelineId = pipelineIds.get( 0 );

        stopAndReleasePipelines( pipelineIds );

        try
        {
            final Region region = regionManager.mergePipelines( pipelineIds );
            regionExecPlans.put( region.getRegionId(), region.getExecPlan() );
            final Pipeline pipeline = new Pipeline( firstPipelineId, region );
            pipeline.init();
            addPipeline( pipeline );
            createDownstreamCollectors( flow, pipeline );
            recreateSinkDownstreamCollectors();
            pipeline.startPipelineReplicaRunners( jokerConfig, supervisor, jokerThreadGroup );
            incrementFlowVersion();
        }
        catch ( Exception e )
        {
            throw new JokerException( "Failed during merging pipelines: " + pipelineIds + " with flow version: " + flowVersion, e );
        }
    }

    @Override
    public void splitPipeline ( final int flowVersion, final PipelineId pipelineIdToSplit, final List<Integer> pipelineOperatorIndices )
    {
        checkArgument( flowVersion == this.flowVersion,
                       "cannot split pipeline %s to %s since given flow version %s is not equal to the flow version: %s",
                       pipelineIdToSplit,
                       pipelineOperatorIndices,
                       flowVersion,
                       this.flowVersion );
        checkState( status == RUNNING,
                    "cannot split pipeline %s into %s with flow version %s since status is %s",
                    pipelineIdToSplit,
                    pipelineOperatorIndices,
                    flowVersion,
                    status );

        LOGGER.info( "Will try to split pipeline: {} into: {} with flow version {}",
                     pipelineIdToSplit,
                     pipelineOperatorIndices,
                     flowVersion );

        regionManager.validatePipelineSplitParameters( pipelineIdToSplit, pipelineOperatorIndices );

        stopAndReleasePipelines( singletonList( pipelineIdToSplit ) );

        try
        {
            final Region region = regionManager.splitPipeline( pipelineIdToSplit, pipelineOperatorIndices );
            regionExecPlans.put( region.getRegionId(), region.getExecPlan() );
            final List<Pipeline> newPipelines = new ArrayList<>();
            for ( int pipelineIndex = 0; pipelineIndex < region.getExecPlan().getPipelineCount(); pipelineIndex++ )
            {
                final PipelineReplica[] pipelineReplicas = region.getPipelineReplicas( pipelineIndex );
                final PipelineReplica pipelineReplica = pipelineReplicas[ 0 ];
                final PipelineId pipelineId = pipelineReplica.id().pipelineId;
                if ( !pipelines.containsKey( pipelineId ) )
                {
                    final Pipeline pipeline = new Pipeline( pipelineId, region );
                    pipeline.init();
                    addPipeline( pipeline );
                    newPipelines.add( pipeline );
                }
            }

            for ( Pipeline pipeline : newPipelines )
            {
                createDownstreamCollectors( flow, pipeline );
            }

            recreateSinkDownstreamCollectors();

            for ( Pipeline pipeline : newPipelines )
            {
                LOGGER.info( "Starting new pipeline {}", pipeline.getId() );
                pipeline.startPipelineReplicaRunners( jokerConfig, supervisor, jokerThreadGroup );
            }

            incrementFlowVersion();
        }
        catch ( Exception e )
        {
            throw new JokerException(
                    "Failed during splitting pipeline: " + pipelineIdToSplit + " into: " + pipelineOperatorIndices + " with flow version: "
                    + flowVersion, e );
        }
    }

    private void incrementFlowVersion ()
    {
        flowVersion++;
        LOGGER.info( "Flow version is updated to {}", flowVersion );
    }

    @Override
    public void rebalanceRegion ( final int flowVersion, final int regionId, final int newReplicaCount )
    {
        checkArgument( flowVersion == this.flowVersion,
                       "cannot rebalance region %s to %s replicas since given flow version %s is not equal to the flow version: %s",
                       regionId,
                       newReplicaCount,
                       flowVersion,
                       this.flowVersion );
        checkState( status == RUNNING,
                    "cannot rebalance region %s to %s replicas with flow version %s since status is %s",
                    regionId,
                    newReplicaCount,
                    flowVersion,
                    status );
        final RegionDef regionDef = getRegionDefOrFail( regionId );
        checkArgument( newReplicaCount > 0,
                       "cannot rebalance region %s with flow version %s since new replica count is %s",
                       regionId,
                       flowVersion,
                       newReplicaCount );
        checkArgument( regionDef.getRegionType() == PARTITIONED_STATEFUL,
                       "cannot rebalance region %s to new replica count %s with flow version %s since region is %s",
                       regionId,
                       newReplicaCount,
                       flowVersion,
                       regionDef.getRegionType() );

        LOGGER.info( "Will try to rebalance region: {} new replica count: {} with flow version {}",
                     regionId,
                     newReplicaCount,
                     flowVersion );

        final Collection<Pipeline> upstreamPipelines = pauseUpstreamPipelines( regionDef );
        stopAndReleasePipelines( regionDef );

        try
        {
            final List<Pipeline> newPipelines = new ArrayList<>();
            final Region region = regionManager.rebalanceRegion( flow, regionDef.getRegionId(), newReplicaCount );
            regionExecPlans.put( region.getRegionId(), region.getExecPlan() );

            for ( int pipelineIndex = 0; pipelineIndex < region.getExecPlan().getPipelineCount(); pipelineIndex++ )
            {
                final PipelineReplica[] pipelineReplicas = region.getPipelineReplicas( pipelineIndex );
                final PipelineReplica pipelineReplica = pipelineReplicas[ 0 ];
                final PipelineId pipelineId = pipelineReplica.id().pipelineId;
                final Pipeline pipeline = new Pipeline( pipelineId, region );
                pipeline.init();
                addPipeline( pipeline );
                newPipelines.add( pipeline );
            }

            for ( Pipeline pipeline : newPipelines )
            {
                createDownstreamCollectors( flow, pipeline );
            }

            for ( Pipeline pipeline : newPipelines )
            {
                LOGGER.info( "Starting new pipeline {}", pipeline.getId() );
                pipeline.startPipelineReplicaRunners( jokerConfig, supervisor, jokerThreadGroup );
            }

            for ( Pipeline pausedPipeline : upstreamPipelines )
            {
                createDownstreamCollectors( flow, pausedPipeline );
            }

            recreateSinkDownstreamCollectors();

            incrementFlowVersion();

            resumePipelines( upstreamPipelines );
        }
        catch ( Exception e )
        {
            throw new JokerException(
                    "Failed during rebalancing region: " + regionId + " to new replica count: " + newReplicaCount + " with flow version: "
                    + flowVersion, e );
        }
    }

    private RegionDef getRegionDefOrFail ( final int regionId )
    {
        final RegionExecPlan regionExecPlan = regionExecPlans.get( regionId );
        checkArgument( regionExecPlan != null, " region: %s not found!", regionId );
        return regionExecPlan.getRegionDef();
    }

    private void addPipeline ( final Pipeline pipeline )
    {
        checkState( this.pipelines.put( pipeline.getId(), pipeline ) == null,
                    "there are multiple pipelines with same id: %s",
                    pipeline.getId() );
    }

    private void stopAndReleasePipelines ( final List<PipelineId> pipelineIds )
    {
        for ( PipelineId pipelineId : pipelineIds )
        {
            checkArgument( pipelines.containsKey( pipelineId ), "pipeline not found for pipeline id %s to merge", pipelineId );
        }

        for ( final PipelineId pipelineId : pipelineIds )
        {
            LOGGER.info( "Stopping pipeline {}...", pipelineId );

            final Pipeline pipeline = pipelines.get( pipelineId );

            final long runnerStopTimeoutInMillis = jokerConfig.getPipelineManagerConfig().getRunnerCommandTimeoutInMillis();
            final List<Exception> failures = pipeline.stopPipelineReplicaRunners( runnerStopTimeoutInMillis );
            pipelines.remove( pipelineId );
            LOGGER.info( "Pipeline {} is released...", pipelineId );
            if ( !failures.isEmpty() )
            {
                throw new JokerException( "Failed during stopping pipeline " + pipelineId + " replica runners!" );
            }
        }
    }

    private void stopAndReleasePipelines ( final RegionDef regionDef )
    {
        final List<PipelineId> pipelineIds = new ArrayList<>();
        for ( Pipeline pipeline : this.pipelines.values() )
        {
            if ( regionDef.getRegionId() == pipeline.getId().getRegionId() )
            {
                pipelineIds.add( pipeline.getId() );
            }
        }

        pipelineIds.sort( PipelineId::compareTo );
        stopAndReleasePipelines( pipelineIds );
    }

    private Collection<Pipeline> pauseUpstreamPipelines ( final RegionDef regionDef )
    {
        final Collection<Pipeline> upstream = new ArrayList<>();
        final Set<String> upstreamOperatorIds = new HashSet<>();
        for ( Collection<Port> c : flow.getInboundConnections( getFirstOperator( regionDef ).getId() ).values() )
        {
            for ( Port p : c )
            {
                upstreamOperatorIds.add( p.getOperatorId() );
            }
        }

        for ( Pipeline pipeline : pipelines.values() )
        {
            if ( upstreamOperatorIds.contains( pipeline.getLastOperatorDef().getId() ) )
            {
                final long runnerStopTimeoutInMillis = jokerConfig.getPipelineManagerConfig().getRunnerCommandTimeoutInMillis();
                final List<Exception> failures = pipeline.pausePipelineReplicaRunners( runnerStopTimeoutInMillis );
                if ( failures.isEmpty() )
                {
                    upstream.add( pipeline );
                }
                else
                {
                    throw new JokerException( "Failed during pausing pipeline " + pipeline.getId() + " replica runners!" );
                }
            }
        }

        return upstream;
    }

    private void resumePipelines ( final Collection<Pipeline> pipelines )
    {
        for ( Pipeline pipeline : pipelines )
        {
            final long runnerStopTimeoutInMillis = jokerConfig.getPipelineManagerConfig().getRunnerCommandTimeoutInMillis();
            final List<Exception> failures = pipeline.resumePipelineReplicaRunners( runnerStopTimeoutInMillis );
            if ( !failures.isEmpty() )
            {
                throw new JokerException( "Failed during resuming pipeline " + pipeline.getId() + " replica runners!" );
            }
        }
    }

    private void releaseRegions ()
    {
        final Multimap<Integer, PipelineId> regionIds = HashMultimap.create();
        for ( PipelineId id : pipelines.keySet() )
        {
            regionIds.put( id.getRegionId(), id );
        }

        for ( Integer regionId : new ArrayList<>( regionIds.keySet() ) )
        {
            for ( PipelineId pipelineId : regionIds.removeAll( regionId ) )
            {
                LOGGER.info( "Removing Pipeline {}", pipelineId );
                pipelines.remove( pipelineId );
            }

            regionManager.releaseRegion( regionId );
        }
    }

    private void recreateSinkDownstreamCollectors ()
    {
        for ( Pipeline pipeline : pipelines.values() )
        {
            if ( flow.getOutboundConnections( pipeline.getLastOperatorDef().getId() ).isEmpty() )
            {
                createDownstreamCollectors( flow, pipeline );
                pipeline.notifyPipelineReplicaRunners();
            }
        }
    }

    private void createDownstreamCollectors ( final FlowDef flow, final Pipeline pipeline )
    {
        final OperatorDef lastOperator = pipeline.getLastOperatorDef();
        final Map<String, List<Pair<Integer, Integer>>> connectionsByOperatorId = getDownstreamConnectionsByOperatorId( flow,
                                                                                                                        lastOperator );
        LOGGER.info( "Pipeline {} with last operator {} has following downstream connectionsByOperatorId: {}",
                     pipeline.getId(),
                     lastOperator.getId(),
                     connectionsByOperatorId );

        final DownstreamCollector[] collectors = new DownstreamCollector[ pipeline.getReplicaCount() ];
        for ( int replicaIndex = 0; replicaIndex < pipeline.getReplicaCount(); replicaIndex++ )
        {
            final DownstreamCollector[] collectorsToDownstreamOperators = new DownstreamCollector[ connectionsByOperatorId.size() ];
            int i = 0;
            for ( Entry<String, List<Pair<Integer, Integer>>> e : connectionsByOperatorId.entrySet() )
            {
                final String downstreamOperatorId = e.getKey();
                final List<Pair<Integer, Integer>> pairs = e.getValue();
                final OperatorDef downstreamOperator = flow.getOperator( downstreamOperatorId );
                final Pipeline downstreamPipeline = getPipeline( downstreamOperator, 0 );
                final RegionDef downstreamRegionDef = downstreamPipeline.getRegionDef();
                final OperatorQueue[] pipelineQueues = getPipelineQueues( downstreamOperator );
                final int j = min( pairs.size(), DOWNSTREAM_TUPLE_SENDER_CONSTRUCTOR_COUNT );

                if ( pipeline.getId().getRegionId() == downstreamPipeline.getId().getRegionId() )
                {
                    final OperatorQueue pipelineQueue = pipelineQueues[ replicaIndex ];
                    collectorsToDownstreamOperators[ i ] = defaultDownstreamCollectorCtors[ j ].apply( pairs, pipelineQueue );
                }
                else if ( downstreamRegionDef.getRegionType() == PARTITIONED_STATEFUL )
                {
                    final int[] partitionDistribution = getPartitionDistribution( downstreamOperator );
                    final PartitionKeyExtractor partitionKeyExtractor = partitionKeyExtractorFactory.createPartitionKeyExtractor(
                            downstreamRegionDef.getPartitionFieldNames() );
                    collectorsToDownstreamOperators[ i ] = partitionedDownstreamCollectorCtors[ j ].apply( pairs,
                                                                                                           partitionService
                                                                                                                   .getPartitionCount(),
                                                                                                           partitionDistribution,
                                                                                                           pipelineQueues,
                                                                                                           partitionKeyExtractor );
                }
                else if ( downstreamRegionDef.getRegionType() == STATELESS )
                {
                    OperatorQueue pipelineQueue = null;
                    if ( pipeline.getReplicaCount() == downstreamPipeline.getReplicaCount() )
                    {
                        pipelineQueue = pipelineQueues[ replicaIndex ];
                    }
                    else if ( downstreamPipeline.getReplicaCount() == 1 )
                    {
                        pipelineQueue = pipelineQueues[ 0 ];
                    }

                    if ( pipelineQueue != null )
                    {
                        collectorsToDownstreamOperators[ i ] = defaultDownstreamCollectorCtors[ j ].apply( pairs, pipelineQueue );
                    }
                    else
                    {
                        throw new IllegalStateException( "incompatible replica counts! pipeline: " + pipeline.getId() + " replica count: "
                                                         + pipeline.getReplicaCount() + " downstream pipeline: "
                                                         + downstreamPipeline.getId() + " downstream pipeline replica count: "
                                                         + downstreamPipeline.getReplicaCount() );
                    }
                }
                else if ( downstreamRegionDef.getRegionType() == STATEFUL )
                {
                    final int l = pipelineQueues.length;
                    checkState( l == 1, "Operator %s can not have %s replicas", downstreamOperatorId, l );
                    final OperatorQueue pipelineQueue = pipelineQueues[ 0 ];
                    collectorsToDownstreamOperators[ i ] = defaultDownstreamCollectorCtors[ j ].apply( pairs, pipelineQueue );
                }
                else
                {
                    throw new IllegalStateException( "invalid region type: " + downstreamRegionDef.getRegionType() );
                }

                i++;
            }

            DownstreamCollector collector = null;
            if ( i == 1 )
            {
                collector = collectorsToDownstreamOperators[ 0 ];
            }
            else if ( i > 1 )
            {
                collector = new CompositeDownstreamCollector( collectorsToDownstreamOperators );
            }

            if ( pipeline.getRegionDef().isSource() )
            {
                if ( i > 0 )
                {
                    final PipelineReplica pipelineReplica = pipeline.getPipelineReplica( replicaIndex );
                    final PipelineReplicaMeter meter = pipelineReplica.getMeter();
                    collector = new IngestionTimeInjector( meter, collector );
                }
                else
                {
                    collector = new NopDownstreamCollector();
                }
            }
            else if ( i == 0 )
            {
                final String tailOperatorId = pipeline.getLastOperatorDef().getId();
                final LatencyMeter latencyMeter = metricManager.createLatencyMeter( flow, tailOperatorId, replicaIndex );
                collector = new LatencyRecorder( latencyMeter );
            }

            checkState( collector != null );

            LOGGER.info( "Created {} for Pipeline {} replica index {}",
                         collector.getClass().getSimpleName(),
                         pipeline.getId(),
                         replicaIndex );

            collectors[ replicaIndex ] = collector;
        }

        pipeline.setDownstreamCollectors( collectors );
    }

    private Map<String, List<Pair<Integer, Integer>>> getDownstreamConnectionsByOperatorId ( final FlowDef flow,
                                                                                             final OperatorDef operator )
    {
        final Map<String, List<Pair<Integer, Integer>>> result = new LinkedHashMap<>();
        final Map<Port, Set<Port>> connections = flow.getOutboundConnections( operator.getId() );
        final Set<String> downstreamOperatorIds = new HashSet<>();
        for ( Collection<Port> c : connections.values() )
        {
            downstreamOperatorIds.addAll( c.stream().map( Port::getOperatorId ).collect( toSet() ) );
        }

        final List<String> downstreamOperatorIdsSorted = new ArrayList<>( downstreamOperatorIds );
        sort( downstreamOperatorIdsSorted );

        for ( String downstreamOperatorId : downstreamOperatorIdsSorted )
        {
            for ( Entry<Port, Set<Port>> e : connections.entrySet() )
            {
                final Port upstreamPort = e.getKey();
                e.getValue()
                 .stream()
                 .filter( downstreamPort -> downstreamPort.getOperatorId().equals( downstreamOperatorId ) )
                 .forEach( downstreamPort -> result.computeIfAbsent( downstreamOperatorId, s -> new ArrayList<>() )
                                                   .add( Pair.of( upstreamPort.getPortIndex(), downstreamPort.getPortIndex() ) ) );
            }
        }

        for ( List<Pair<Integer, Integer>> pairs : result.values() )
        {
            pairs.sort( comparingInt( ( Pair<Integer, Integer> p ) -> p._1 ).thenComparingInt( p -> p._2 ) );
        }

        return result;
    }

    private int[] getPartitionDistribution ( final OperatorDef operator )
    {
        final Pipeline pipeline = getPipeline( operator, 0 );
        final PartitionDistribution partitionDistribution = partitionService.getPartitionDistributionOrFail( pipeline.getId()
                                                                                                                     .getRegionId() );
        return partitionDistribution.getDistribution();
    }

    private OperatorQueue[] getPipelineQueues ( final OperatorDef operator )
    {
        final Pipeline pipeline = getPipeline( operator, 0 );
        final int replicaCount = pipeline.getReplicaCount();
        final OperatorQueue[] pipelineQueues = new OperatorQueue[ replicaCount ];
        for ( int i = 0; i < replicaCount; i++ )
        {
            pipelineQueues[ i ] = pipeline.getPipelineReplica( i ).getEffectiveQueue();
        }

        return pipelineQueues;
    }

    private void copyPorts ( final List<Pair<Integer, Integer>> pairs, final int[] sourcePorts, final int[] destinationPorts )
    {
        for ( int i = 0; i < pairs.size(); i++ )
        {
            sourcePorts[ i ] = pairs.get( i )._1;
            destinationPorts[ i ] = pairs.get( i )._2;
        }
    }

    private void startPipelineReplicaRunners ( final Supervisor supervisor )
    {
        for ( Pipeline pipeline : pipelines.values() )
        {
            pipeline.startPipelineReplicaRunners( jokerConfig, supervisor, jokerThreadGroup );
        }
    }

    private Pipeline getPipeline ( final OperatorDef operator, final int operatorIndex )
    {
        for ( Pipeline pipeline : pipelines.values() )
        {
            final int i = pipeline.getOperatorIndex( operator );
            if ( i == -1 )
            {
                continue;
            }
            else if ( operatorIndex == -1 )
            {
                return pipeline;
            }

            checkArgument( operatorIndex == i,
                           "Operator %s is expected to be at %s'th index of pipeline %s but it is at %s'th index",
                           operator.getId(),
                           pipeline.getId(),
                           i );
            return pipeline;
        }

        throw new IllegalArgumentException( "Operator " + operator.getId() + " is not found in the pipelines" );
    }

    private Pipeline getPipelineOrFail ( final PipelineId id )
    {
        final Pipeline pipeline = pipelines.get( id );
        checkArgument( pipeline != null, "no pipeline found for pipeline id: %s", id );
        return pipeline;
    }

    private Pipeline getPipelineOrFail ( final OperatorDef operator )
    {
        return pipelines.values()
                        .stream()
                        .filter( p -> p.getOperatorIndex( operator ) != -1 )
                        .findFirst()
                        .orElseThrow( (Supplier<RuntimeException>) () -> new IllegalArgumentException(
                                "No pipeline found for operator " + operator.getId() ) );
    }

    private Pipeline getPipelineByFirstOperatorOrFail ( final OperatorDef operator )
    {
        final int expectedOperatorIndex = 0;
        final Pipeline pipeline = getPipelineOrFail( operator );
        final int operatorIndex = pipeline.getOperatorIndex( operator );
        checkArgument( operatorIndex == expectedOperatorIndex,
                       "Pipeline %s has operator %s with index %s but expected index is %s",
                       pipeline.getId(),
                       operator.getId(),
                       operatorIndex,
                       expectedOperatorIndex );
        return pipeline;
    }

    private void stopPipelineReplicaRunners ()
    {
        for ( Pipeline pipeline : getPipelinesTopologicallySorted() )
        {
            final long runnerStopTimeoutInMillis = jokerConfig.getPipelineManagerConfig().getRunnerCommandTimeoutInMillis();
            pipeline.stopPipelineReplicaRunners( runnerStopTimeoutInMillis );
        }
    }

    private List<Pipeline> getPipelinesTopologicallySorted ()
    {
        final List<Pipeline> pipelines = new ArrayList<>( this.pipelines.values() );
        pipelines.sort( comparing( Pipeline::getId ) );

        return pipelines;
    }

    private void shutdownPipelines ()
    {
        for ( Pipeline pipeline : pipelines.values() )
        {
            pipeline.shutdown();
        }
    }

    @Override
    public void handlePipelineReplicaFailed ( final PipelineReplicaId id, final Throwable failure )
    {
        LOGGER.error( "Pipeline Replica " + id + " failed.", failure );
        shutdownGracefully( failure );
    }

    @Override
    public FlowStatus getFlowStatus ()
    {
        return status;
    }

    private void shutdownGracefully ( final Throwable reason )
    {
        try
        {
            if ( reason != null )
            {
                LOGGER.error( "Shutting down flow", reason );
                downstreamCollectorFailureFlag.set( true );
            }
            else
            {
                LOGGER.info( "Shutting down flow..." );
            }

            status = FlowStatus.SHUT_DOWN;
            latencyRecorderPool.shutdown();
            if ( !latencyRecorderPool.awaitTermination( 10, TimeUnit.SECONDS ) )
            {
                LOGGER.error( "Latency recorder pool could not be terminated..." );
            }

            stopPipelineReplicaRunners();
            shutdownPipelines();
            releaseRegions();
        }
        catch ( Exception e )
        {
            LOGGER.error( "Shutdown failed", e );
        }
    }

    private void recordLatencies ( final OneToOneConcurrentArrayQueue<Tuple> queue, final IdleStrategy idleStrategy )
    {
        while ( status == RUNNING )
        {
            final Tuple tuple = queue.poll();
            if ( tuple == null )
            {
                idleStrategy.idle();
                continue;
            }

            idleStrategy.reset();

            final LatencyMeter latencyMeter = tuple.getLatencyRecorder();
            if ( tuple.isIngestionTimeNA() )
            {
                continue;
            }

            latencyMeter.recordTuple( ( tuple.getIngestionTime() ) );

            final List<LatencyRecord> recs = tuple.getLatencyRecs();
            if ( recs == null )
            {
                continue;
            }

            for ( int i = 0; i < recs.size(); i++ )
            {
                final LatencyRecord record = recs.get( i );
                final String operatorId = record.getOperatorId();
                final long latency = record.getLatency();
                if ( record.isOperator() )
                {
                    latencyMeter.recordInvocation( operatorId, latency );
                }
                else
                {
                    latencyMeter.recordQueue( operatorId, latency );
                }
            }
        }
    }

    @FunctionalInterface
    private interface Function6<T1, T2, T3, T4, T5, T6>
    {
        T6 apply ( T1 t1, T2 t2, T3 t3, T4 t4, T5 t5 );
    }


    static class NopDownstreamCollector implements DownstreamCollector
    {
        @Override
        public void accept ( final TuplesImpl tuples )
        {
        }
    }


    static class IngestionTimeInjector implements DownstreamCollector
    {
        private final PipelineReplicaMeter meter;
        private final DownstreamCollector downstream;

        IngestionTimeInjector ( final PipelineReplicaMeter meter, final DownstreamCollector downstream )
        {
            this.meter = meter;
            this.downstream = downstream;
        }

        @Override
        public void accept ( final TuplesImpl tuples )
        {
            final long ingestionTime = System.nanoTime();
            final boolean trackLatencyRecords = meter.isTicked();

            for ( int i = 0, p = tuples.getPortCount(); i < p; i++ )
            {
                final List<Tuple> l = tuples.getTuplesModifiable( i );
                for ( int j = 0, t = l.size(); j < t; j++ )
                {
                    l.get( j ).setIngestionTime( ingestionTime, trackLatencyRecords );
                }
            }

            downstream.accept( tuples );
        }

        public DownstreamCollector getDownstream ()
        {
            return downstream;
        }
    }


    class LatencyRecorder implements DownstreamCollector
    {
        private final LatencyMeter latencyMeter;
        private final BackoffIdleStrategy producerIdleStrategy = newDefaultInstance();
        private int next;

        LatencyRecorder ( final LatencyMeter latencyMeter )
        {
            this.latencyMeter = latencyMeter;
        }

        @Override
        public void accept ( final TuplesImpl tuples )
        {
            final long now = System.nanoTime();
            producerIdleStrategy.reset();

            for ( int i = 0; i < tuples.getPortCount(); i++ )
            {
                final List<Tuple> l = tuples.getTuplesModifiable( i );
                for ( int j = 0; j < l.size(); j++ )
                {
                    final Tuple tuple = l.get( j );

                    final LatencyMeter latencyMeter = tuple.getLatencyRecorder();
                    if ( tuple.isIngestionTimeNA() )
                    {
                        continue;
                    }

                    latencyMeter.recordTuple( ( now - tuple.getIngestionTime() ) );

                    if ( tuple.getLatencyRecs() == null )
                    {
                        continue;
                    }

                    while ( true )
                    {
                        final OneToOneConcurrentArrayQueue<Tuple> queue = latencyRecorderQueues[ next ];
                        next = ( next == ( latencyRecorderPoolSize - 1 ) ) ? 0 : next + 1;

                        if ( queue.offer( tuple ) )
                        {
                            break;
                        }
                    }

                }
            }
        }
    }

}
