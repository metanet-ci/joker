package cs.bilkent.joker.engine.pipeline.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import cs.bilkent.joker.engine.FlowStatus;
import cs.bilkent.joker.engine.config.JokerConfig;
import static cs.bilkent.joker.engine.config.JokerConfig.JOKER_THREAD_GROUP_NAME;
import cs.bilkent.joker.engine.exception.InitializationException;
import cs.bilkent.joker.engine.exception.JokerException;
import cs.bilkent.joker.engine.partition.PartitionDistribution;
import cs.bilkent.joker.engine.partition.PartitionKeyExtractor;
import cs.bilkent.joker.engine.partition.PartitionKeyExtractorFactory;
import cs.bilkent.joker.engine.partition.PartitionService;
import cs.bilkent.joker.engine.pipeline.DownstreamTupleSender;
import cs.bilkent.joker.engine.pipeline.OperatorReplicaStatus;
import static cs.bilkent.joker.engine.pipeline.OperatorReplicaStatus.COMPLETING;
import static cs.bilkent.joker.engine.pipeline.OperatorReplicaStatus.INITIAL;
import static cs.bilkent.joker.engine.pipeline.OperatorReplicaStatus.RUNNING;
import cs.bilkent.joker.engine.pipeline.Pipeline;
import cs.bilkent.joker.engine.pipeline.PipelineId;
import cs.bilkent.joker.engine.pipeline.PipelineManager;
import cs.bilkent.joker.engine.pipeline.PipelineReplica;
import cs.bilkent.joker.engine.pipeline.PipelineReplicaId;
import cs.bilkent.joker.engine.pipeline.UpstreamConnectionStatus;
import static cs.bilkent.joker.engine.pipeline.UpstreamConnectionStatus.ACTIVE;
import static cs.bilkent.joker.engine.pipeline.UpstreamConnectionStatus.CLOSED;
import cs.bilkent.joker.engine.pipeline.UpstreamContext;
import cs.bilkent.joker.engine.pipeline.impl.downstreamtuplesender.CompositeDownstreamTupleSender;
import cs.bilkent.joker.engine.pipeline.impl.downstreamtuplesender.DownstreamTupleSender1;
import cs.bilkent.joker.engine.pipeline.impl.downstreamtuplesender.DownstreamTupleSender2;
import cs.bilkent.joker.engine.pipeline.impl.downstreamtuplesender.DownstreamTupleSender3;
import cs.bilkent.joker.engine.pipeline.impl.downstreamtuplesender.DownstreamTupleSender4;
import cs.bilkent.joker.engine.pipeline.impl.downstreamtuplesender.DownstreamTupleSenderN;
import cs.bilkent.joker.engine.pipeline.impl.downstreamtuplesender.PartitionedDownstreamTupleSender1;
import cs.bilkent.joker.engine.pipeline.impl.downstreamtuplesender.PartitionedDownstreamTupleSender2;
import cs.bilkent.joker.engine.pipeline.impl.downstreamtuplesender.PartitionedDownstreamTupleSender3;
import cs.bilkent.joker.engine.pipeline.impl.downstreamtuplesender.PartitionedDownstreamTupleSender4;
import cs.bilkent.joker.engine.pipeline.impl.downstreamtuplesender.PartitionedDownstreamTupleSenderN;
import cs.bilkent.joker.engine.region.FlowDeploymentDef;
import cs.bilkent.joker.engine.region.Region;
import cs.bilkent.joker.engine.region.RegionConfig;
import cs.bilkent.joker.engine.region.RegionDef;
import cs.bilkent.joker.engine.region.RegionManager;
import cs.bilkent.joker.engine.supervisor.Supervisor;
import cs.bilkent.joker.engine.tuplequeue.OperatorTupleQueue;
import cs.bilkent.joker.flow.FlowDef;
import cs.bilkent.joker.flow.Port;
import cs.bilkent.joker.operator.OperatorDef;
import cs.bilkent.joker.operator.impl.TuplesImpl;
import cs.bilkent.joker.operator.scheduling.SchedulingStrategy;
import static cs.bilkent.joker.operator.spec.OperatorType.PARTITIONED_STATEFUL;
import static cs.bilkent.joker.operator.spec.OperatorType.STATEFUL;
import static cs.bilkent.joker.operator.spec.OperatorType.STATELESS;
import cs.bilkent.joker.utils.Pair;
import static java.lang.Integer.compare;
import static java.lang.Math.min;
import static java.util.Collections.reverse;
import static java.util.Collections.singletonList;
import static java.util.Collections.sort;
import static java.util.stream.Collectors.toList;

@Singleton
public class PipelineManagerImpl implements PipelineManager
{

    private static final Logger LOGGER = LoggerFactory.getLogger( PipelineManagerImpl.class );

    private static final int DOWNSTREAM_TUPLE_SENDER_CONSTRUCTOR_COUNT = 5;


    private final JokerConfig jokerConfig;

    private final RegionManager regionManager;

    private final PartitionService partitionService;

    private final PartitionKeyExtractorFactory partitionKeyExtractorFactory;

    private final ThreadGroup jokerThreadGroup;

    private final BiFunction<List<Pair<Integer, Integer>>, OperatorTupleQueue, DownstreamTupleSender>[]
            defaultDownstreamTupleSenderConstructors = new BiFunction[ 6 ];

    private final Function6<List<Pair<Integer, Integer>>, Integer, int[], OperatorTupleQueue[], PartitionKeyExtractor,
                                   DownstreamTupleSender>[] partitionedDownstreamTupleSenderConstructors = new Function6[ 6 ];

    private FlowDeploymentDef flowDeployment;

    private final Map<PipelineId, Pipeline> pipelines = new ConcurrentHashMap<>();

    private volatile FlowStatus status = FlowStatus.INITIAL;

    @Inject
    public PipelineManagerImpl ( final JokerConfig jokerConfig,
                                 final RegionManager regionManager,
                                 final PartitionService partitionService,
                                 final PartitionKeyExtractorFactory partitionKeyExtractorFactory,
                                 @Named( JOKER_THREAD_GROUP_NAME ) final ThreadGroup jokerThreadGroup )
    {
        this.jokerConfig = jokerConfig;
        this.regionManager = regionManager;
        this.partitionService = partitionService;
        this.partitionKeyExtractorFactory = partitionKeyExtractorFactory;
        this.jokerThreadGroup = jokerThreadGroup;
        createDownstreamTupleSenderFactories( partitionService );
    }

    private void createDownstreamTupleSenderFactories ( final PartitionService partitionService )
    {
        defaultDownstreamTupleSenderConstructors[ 1 ] = ( pairs, tupleQueue ) ->
        {
            final Pair<Integer, Integer> pair1 = pairs.get( 0 );
            return new DownstreamTupleSender1( pair1._1, pair1._2, tupleQueue );
        };
        defaultDownstreamTupleSenderConstructors[ 2 ] = ( pairs, tupleQueue ) ->
        {
            final Pair<Integer, Integer> pair1 = pairs.get( 0 );
            final Pair<Integer, Integer> pair2 = pairs.get( 1 );
            return new DownstreamTupleSender2( pair1._1, pair1._2, pair2._1, pair2._2, tupleQueue );
        };
        defaultDownstreamTupleSenderConstructors[ 3 ] = ( pairs, tupleQueue ) ->
        {
            final Pair<Integer, Integer> pair1 = pairs.get( 0 );
            final Pair<Integer, Integer> pair2 = pairs.get( 1 );
            final Pair<Integer, Integer> pair3 = pairs.get( 2 );
            return new DownstreamTupleSender3( pair1._1, pair1._2, pair2._1, pair2._2, pair3._1, pair3._2, tupleQueue );
        };
        defaultDownstreamTupleSenderConstructors[ 4 ] = ( pairs, tupleQueue ) ->
        {
            final Pair<Integer, Integer> pair1 = pairs.get( 0 );
            final Pair<Integer, Integer> pair2 = pairs.get( 1 );
            final Pair<Integer, Integer> pair3 = pairs.get( 2 );
            final Pair<Integer, Integer> pair4 = pairs.get( 3 );
            return new DownstreamTupleSender4( pair1._1, pair1._2, pair2._1, pair2._2, pair3._1, pair3._2, pair4._1, pair4._2, tupleQueue );
        };
        defaultDownstreamTupleSenderConstructors[ 5 ] = ( pairs, tupleQueue ) ->
        {
            final int[] sourcePorts = new int[ pairs.size() ];
            final int[] destinationPorts = new int[ pairs.size() ];
            copyPorts( pairs, sourcePorts, destinationPorts );
            return new DownstreamTupleSenderN( sourcePorts, destinationPorts, tupleQueue );
        };
        partitionedDownstreamTupleSenderConstructors[ 1 ] = ( pairs, partitionCount, partitionDistribution, tupleQueues,
                                                              partitionKeyFunction ) ->
        {
            final Pair<Integer, Integer> pair1 = pairs.get( 0 );
            return new PartitionedDownstreamTupleSender1( pair1._1,
                                                          pair1._2,
                                                          partitionService.getPartitionCount(), partitionDistribution, tupleQueues,
                                                          partitionKeyFunction );
        };
        partitionedDownstreamTupleSenderConstructors[ 2 ] = ( pairs, partitionCount, partitionDistribution, tupleQueues,
                                                              partitionKeyFunction ) ->
        {
            final Pair<Integer, Integer> pair1 = pairs.get( 0 );
            final Pair<Integer, Integer> pair2 = pairs.get( 1 );
            return new PartitionedDownstreamTupleSender2( pair1._1,
                                                          pair1._2,
                                                          pair2._1,
                                                          pair2._2,
                                                          partitionService.getPartitionCount(), partitionDistribution, tupleQueues,
                                                          partitionKeyFunction );
        };
        partitionedDownstreamTupleSenderConstructors[ 3 ] = ( pairs, partitionCount, partitionDistribution, tupleQueues,
                                                              partitionKeyFunction ) ->
        {
            final Pair<Integer, Integer> pair1 = pairs.get( 0 );
            final Pair<Integer, Integer> pair2 = pairs.get( 1 );
            final Pair<Integer, Integer> pair3 = pairs.get( 2 );
            return new PartitionedDownstreamTupleSender3( pair1._1,
                                                          pair1._2,
                                                          pair2._1,
                                                          pair2._2,
                                                          pair3._1,
                                                          pair3._2,
                                                          partitionService.getPartitionCount(), partitionDistribution, tupleQueues,
                                                          partitionKeyFunction );
        };
        partitionedDownstreamTupleSenderConstructors[ 4 ] = ( pairs, partitionCount, partitionDistribution, tupleQueues,
                                                              partitionKeyFunction ) ->
        {
            final Pair<Integer, Integer> pair1 = pairs.get( 0 );
            final Pair<Integer, Integer> pair2 = pairs.get( 1 );
            final Pair<Integer, Integer> pair3 = pairs.get( 2 );
            final Pair<Integer, Integer> pair4 = pairs.get( 3 );
            return new PartitionedDownstreamTupleSender4( pair1._1,
                                                          pair1._2,
                                                          pair2._1,
                                                          pair2._2,
                                                          pair3._1,
                                                          pair3._2,
                                                          pair4._1,
                                                          pair4._2,
                                                          partitionService.getPartitionCount(), partitionDistribution, tupleQueues,
                                                          partitionKeyFunction );
        };
        partitionedDownstreamTupleSenderConstructors[ 5 ] = ( pairs, partitionCount, partitionDistribution, tupleQueues,
                                                              partitionKeyFunction ) ->
        {
            final int[] sourcePorts = new int[ pairs.size() ];
            final int[] destinationPorts = new int[ pairs.size() ];
            copyPorts( pairs, sourcePorts, destinationPorts );
            return new PartitionedDownstreamTupleSenderN( sourcePorts,
                                                          destinationPorts,
                                                          partitionService.getPartitionCount(), partitionDistribution, tupleQueues,
                                                          partitionKeyFunction );
        };
    }

    public List<Pipeline> getPipelines ()
    {
        return new ArrayList<>( pipelines.values() );
    }

    List<Pipeline> createPipelines ( final FlowDef flow, final List<RegionConfig> regionConfigs )
    {
        final List<Pipeline> pipelines = createRegions( flow, regionConfigs );
        createDownstreamTupleSenders( flow );
        createUpstreamContexts( flow );

        return pipelines;
    }

    @Override
    public void start ( final Supervisor supervisor, final FlowDeploymentDef flowDeployment, final List<RegionConfig> regionConfigs )
    {
        try
        {
            checkState( status == FlowStatus.INITIAL, "cannot startPipelineReplicaRunnerThreads since status is %s", status );
            checkState( this.flowDeployment == null, "Flow deployment is already done!" );
            this.flowDeployment = flowDeployment;
            createPipelines( flowDeployment.getFlow(), regionConfigs );
            initPipelines();
            createPipelineReplicaRunners( supervisor );
            status = FlowStatus.RUNNING;
        }
        catch ( Exception e )
        {
            status = FlowStatus.INITIALIZATION_FAILED;
            throw new InitializationException( "Flow start failed", e );
        }
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
    public UpstreamContext getUpstreamContext ( final PipelineReplicaId id )
    {
        final Pipeline pipeline = pipelines.get( id.pipelineId );
        checkArgument( pipeline != null, "PipelineReplica %s not found", id );
        return pipeline.getUpstreamContext();
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
                final UpstreamContext updatedUpstreamContext = getUpdatedUpstreamContext( downstreamPipeline );
                if ( updatedUpstreamContext != null )
                {
                    downstreamPipeline.handleUpstreamContextUpdated( updatedUpstreamContext );
                }
                else
                {
                    LOGGER.info( "Upstream Pipeline {} is completed but {} of Downstream Pipeline {} is same.",
                                 id,
                                 downstreamPipeline.getUpstreamContext(),
                                 downstreamPipeline.getId() );
                }
            }
        }

        return gracefullyShutdownIfAllPipelinesCompleted();
    }

    private Collection<Pipeline> getDownstreamPipelines ( final Pipeline upstreamPipeline )
    {
        return flowDeployment.getFlow()
                             .getDownstreamConnections( upstreamPipeline.getLastOperatorDef().id() )
                             .values()
                             .stream()
                             .flatMap( ports -> ports.stream().map( port -> port.operatorId ) )
                             .distinct()
                             .map( flowDeployment.getFlow()::getOperator )
                             .map( operatorDef -> getPipelineOrFail( operatorDef, 0 ) )
                             .collect( toList() );
    }

    private UpstreamContext getUpdatedUpstreamContext ( final Pipeline pipeline )
    {
        final UpstreamConnectionStatus[] upstreamConnectionStatuses = getUpstreamConnectionStatuses( pipeline );
        final UpstreamContext current = pipeline.getUpstreamContext();
        boolean update = false;
        for ( int i = 0; i < upstreamConnectionStatuses.length; i++ )
        {
            if ( upstreamConnectionStatuses[ i ] != current.getUpstreamConnectionStatus( i ) )
            {
                update = true;
                break;
            }
        }

        UpstreamContext upstreamContext = null;
        if ( update )
        {
            upstreamContext = new UpstreamContext( current.getVersion() + 1, upstreamConnectionStatuses );
        }

        return upstreamContext;
    }

    private UpstreamConnectionStatus[] getUpstreamConnectionStatuses ( final Pipeline pipeline )
    {
        final OperatorDef firstOperator = pipeline.getFirstOperatorDef();
        final UpstreamConnectionStatus[] statuses = new UpstreamConnectionStatus[ firstOperator.inputPortCount() ];
        for ( Entry<Port, Collection<Port>> entry : flowDeployment.getFlow().getUpstreamConnections( firstOperator.id() ).entrySet() )
        {
            final Collection<Port> upstreamPorts = entry.getValue();
            final UpstreamConnectionStatus status;
            if ( upstreamPorts.isEmpty() )
            {
                status = CLOSED;
            }
            else
            {
                final List<Pair<OperatorDef, Pipeline>> upstream = upstreamPorts.stream()
                                                                                .map( p -> flowDeployment.getFlow()
                                                                                                         .getOperator( p.operatorId ) )
                                                                                .map( o -> Pair.of( o, getPipelineOrFail( o ) ) )
                                                                                .collect( toList() );

                upstream.forEach( p -> checkState( p._2.getOperatorIndex( p._1 ) == p._2.getOperatorCount() - 1 ) );

                final boolean aliveConnPresent = upstream.stream().filter( p ->
                                                                           {
                                                                               final OperatorReplicaStatus s = p._2.getPipelineStatus();
                                                                               return s == OperatorReplicaStatus.INITIAL
                                                                                      || s == OperatorReplicaStatus.RUNNING
                                                                                      || s == OperatorReplicaStatus.COMPLETING;
                                                                           } ).findFirst().isPresent();
                status = aliveConnPresent ? UpstreamConnectionStatus.ACTIVE : UpstreamConnectionStatus.CLOSED;
            }

            statuses[ entry.getKey().portIndex ] = status;
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
        checkState( status == FlowStatus.RUNNING, "cannot trigger shutdown since status is %s", status );
        LOGGER.info( "Shutdown request is being handled." );

        status = FlowStatus.SHUTTING_DOWN;

        for ( Pipeline pipeline : pipelines.values() )
        {
            final OperatorDef operatorDef = pipeline.getFirstOperatorDef();
            if ( flowDeployment.getFlow().getUpstreamConnections( operatorDef.id() ).isEmpty() )
            {
                pipeline.handleUpstreamContextUpdated( new UpstreamContext( 1, new UpstreamConnectionStatus[] {} ) );
            }
        }
    }

    @Override
    public void mergePipelines ( final Supervisor supervisor, final List<PipelineId> pipelineIds )
    {
        checkState( status == FlowStatus.RUNNING, "cannot merge pipelines %s since status is %s", pipelineIds, status );
        LOGGER.info( "Will try to merge pipelines: {}", pipelineIds );

        regionManager.validatePipelineMergeParameters( pipelineIds );

        final PipelineId firstPipelineId = pipelineIds.get( 0 );
        final Pipeline firstPipeline = pipelines.get( firstPipelineId );
        final SchedulingStrategy initialSchedulingStrategy = firstPipeline.getInitialSchedulingStrategy();
        final UpstreamContext upstreamContext = firstPipeline.getUpstreamContext();

        releasePipelines( pipelineIds );

        try
        {
            final Region region = regionManager.mergePipelines( pipelineIds );
            final PipelineReplica[] pipelineReplicas = region.getPipelineReplicasByPipelineId( firstPipelineId );

            final Pipeline pipeline = new Pipeline( firstPipelineId,
                                                    region.getConfig(),
                                                    pipelineReplicas,
                                                    initialSchedulingStrategy,
                                                    upstreamContext );
            addPipeline( pipeline );
            createDownstreamTupleSenders( flowDeployment.getFlow(), pipeline );
            pipeline.startPipelineReplicaRunners( jokerConfig, supervisor, jokerThreadGroup );
        }
        catch ( Exception e )
        {
            throw new JokerException( "Failed during merging pipelines: " + pipelineIds, e );
        }
    }

    @Override
    public void splitPipeline ( final Supervisor supervisor,
                                final PipelineId pipelineIdToSplit,
                                final List<Integer> pipelineOperatorIndices )
    {
        checkState( status == FlowStatus.RUNNING,
                    "cannot split pipeline %s into %s since status is %s",
                    pipelineIdToSplit,
                    pipelineOperatorIndices,
                    status );
        LOGGER.info( "Will try to split pipeline: {} into: {}", pipelineIdToSplit, pipelineOperatorIndices );

        regionManager.validatePipelineSplitParameters( pipelineIdToSplit, pipelineOperatorIndices );

        releasePipelines( singletonList( pipelineIdToSplit ) );

        try
        {
            final Region region = regionManager.splitPipeline( pipelineIdToSplit, pipelineOperatorIndices );
            final List<Pipeline> newPipelines = new ArrayList<>();
            for ( int pipelineIndex = 0; pipelineIndex < region.getConfig().getPipelineCount(); pipelineIndex++ )
            {
                final PipelineReplica[] pipelineReplicas = region.getPipelineReplicas( pipelineIndex );
                final PipelineReplica pipelineReplica = pipelineReplicas[ 0 ];
                final PipelineId pipelineId = pipelineReplica.id().pipelineId;
                if ( !pipelines.containsKey( pipelineId ) )
                {
                    final Pipeline pipeline = new Pipeline( pipelineId,
                                                            region.getConfig(),
                                                            pipelineReplicas,
                                                            pipelineReplica.getOperator( 0 ).getInitialSchedulingStrategy(),
                                                            pipelineReplica.getPipelineUpstreamContext() );
                    addPipeline( pipeline );
                    newPipelines.add( pipeline );
                }
            }

            for ( Pipeline pipeline : newPipelines )
            {
                createDownstreamTupleSenders( flowDeployment.getFlow(), pipeline );
            }

            for ( Pipeline pipeline : newPipelines )
            {
                LOGGER.info( "Starting new pipeline {}", pipeline.getId() );
                pipeline.startPipelineReplicaRunners( jokerConfig, supervisor, jokerThreadGroup );
            }
        }
        catch ( Exception e )
        {
            throw new JokerException( "Failed during splitting pipeline: " + pipelineIdToSplit + " into: " + pipelineOperatorIndices, e );
        }
    }

    private void addPipeline ( final Pipeline pipeline )
    {
        checkState( this.pipelines.put( pipeline.getId(), pipeline ) == null,
                    "there are multiple pipelines with same id: %s",
                    pipeline.getId() );
    }

    private void releasePipelines ( final List<PipelineId> pipelineIds )
    {
        for ( PipelineId pipelineId : pipelineIds )
        {
            checkArgument( pipelines.containsKey( pipelineId ), "pipeline not found for pipeline id %s to merge", pipelineId );
        }

        for ( int i = 0; i < pipelineIds.size(); i++ )
        {
            final PipelineId pipelineId = pipelineIds.get( i );
            LOGGER.info( "Stopping pipeline {}...", pipelineId );

            final Pipeline pipeline = pipelines.get( pipelineId );

            final long runnerStopTimeoutInMillis = jokerConfig.getPipelineManagerConfig().getRunnerStopTimeoutInMillis();
            final List<Exception> failures = pipeline.stopPipelineReplicaRunners( i > 0, runnerStopTimeoutInMillis );
            pipelines.remove( pipelineId );
            LOGGER.info( "Pipeline {} is released...", pipelineId );
            if ( !failures.isEmpty() )
            {
                throw new JokerException( "Failed during stopping pipeline " + pipelineId + " replica runners!" );
            }
        }
    }

    private void releaseRegions ()
    {
        final Multimap<Integer, PipelineId> regionIds = HashMultimap.create();
        for ( PipelineId id : pipelines.keySet() )
        {
            regionIds.put( id.regionId, id );
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

    private List<Pipeline> createRegions ( final FlowDef flow, final Collection<RegionConfig> regionConfigs )
    {
        final List<Pipeline> pipelines = new ArrayList<>();
        for ( RegionConfig regionConfig : regionConfigs )
        {
            LOGGER.info( "Initializing regionId={} with {} pipelines ( {} ) and {} replicas",
                         regionConfig.getRegionId(),
                         regionConfig.getPipelineCount(),
                         regionConfig.getPipelineStartIndices(),
                         regionConfig.getReplicaCount() );

            final Region region = regionManager.createRegion( flow, regionConfig );
            for ( int pipelineIndex = 0; pipelineIndex < regionConfig.getPipelineCount(); pipelineIndex++ )
            {
                final PipelineReplica[] pipelineReplicas = region.getPipelineReplicas( pipelineIndex );
                final PipelineId pipelineId = pipelineReplicas[ 0 ].id().pipelineId;
                final Pipeline pipeline = new Pipeline( pipelineId, regionConfig, pipelineReplicas );
                addPipeline( pipeline );
                pipelines.add( pipeline );
            }

            LOGGER.info( "regionId={} is created", regionConfig.getRegionId() );
        }

        return pipelines;
    }

    private void createDownstreamTupleSenders ( final FlowDef flow )
    {
        for ( Pipeline pipeline : pipelines.values() )
        {
            createDownstreamTupleSenders( flow, pipeline );
        }
    }

    private void createDownstreamTupleSenders ( final FlowDef flow, final Pipeline pipeline )
    {
        final OperatorDef lastOperator = pipeline.getLastOperatorDef();
        final Map<String, List<Pair<Integer, Integer>>> connectionsByOperatorId = getDownstreamConnectionsByOperatorId( flow,
                                                                                                                        lastOperator );
        LOGGER.info( "Pipeline {} with last operator {} has following downstream connectionsByOperatorId: {}",
                     pipeline.getId(),
                     lastOperator.id(),
                     connectionsByOperatorId );

        for ( int replicaIndex = 0; replicaIndex < pipeline.getReplicaCount(); replicaIndex++ )
        {
            final DownstreamTupleSender[] senders = new DownstreamTupleSender[ connectionsByOperatorId.size() ];
            int i = 0;
            for ( Entry<String, List<Pair<Integer, Integer>>> e : connectionsByOperatorId.entrySet() )
            {
                final String downstreamOperatorId = e.getKey();
                final List<Pair<Integer, Integer>> pairs = e.getValue();
                final OperatorDef downstreamOperator = flow.getOperator( downstreamOperatorId );
                final Pipeline downstreamPipeline = getPipeline( downstreamOperator, 0 );
                final RegionDef downstreamRegionDef = downstreamPipeline.getRegionDef();
                final OperatorTupleQueue[] pipelineTupleQueues = getPipelineTupleQueues( downstreamOperator );
                final int j = min( pairs.size(), DOWNSTREAM_TUPLE_SENDER_CONSTRUCTOR_COUNT );

                if ( pipeline.getId().regionId == downstreamPipeline.getId().regionId )
                {
                    final OperatorTupleQueue pipelineTupleQueue = pipelineTupleQueues[ replicaIndex ];
                    senders[ i ] = defaultDownstreamTupleSenderConstructors[ j ].apply( pairs, pipelineTupleQueue );
                }
                else if ( downstreamRegionDef.getRegionType() == PARTITIONED_STATEFUL )
                {
                    final int[] partitionDistribution = getPartitionDistribution( downstreamOperator );
                    final PartitionKeyExtractor partitionKeyExtractor = partitionKeyExtractorFactory.createPartitionKeyExtractor(
                            downstreamRegionDef.getPartitionFieldNames() );
                    senders[ i ] = partitionedDownstreamTupleSenderConstructors[ j ].apply( pairs,
                                                                                            partitionService.getPartitionCount(),
                                                                                            partitionDistribution,
                                                                                            pipelineTupleQueues,
                                                                                            partitionKeyExtractor );
                }
                else if ( downstreamRegionDef.getRegionType() == STATELESS )
                {
                    OperatorTupleQueue pipelineTupleQueue = null;
                    if ( pipeline.getReplicaCount() == downstreamPipeline.getReplicaCount() )
                    {
                        pipelineTupleQueue = pipelineTupleQueues[ replicaIndex ];
                    }
                    else if ( downstreamPipeline.getReplicaCount() == 1 )
                    {
                        pipelineTupleQueue = pipelineTupleQueues[ 0 ];
                    }

                    if ( pipelineTupleQueue != null )
                    {
                        senders[ i ] = defaultDownstreamTupleSenderConstructors[ j ].apply( pairs, pipelineTupleQueue );
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
                    final int l = pipelineTupleQueues.length;
                    checkState( l == 1, "Operator %s can not have %s replicas", downstreamOperatorId, l );
                    final OperatorTupleQueue pipelineTupleQueue = pipelineTupleQueues[ 0 ];
                    senders[ i ] = defaultDownstreamTupleSenderConstructors[ j ].apply( pairs, pipelineTupleQueue );
                }
                else
                {
                    throw new IllegalStateException( "invalid region type: " + downstreamRegionDef.getRegionType() );
                }

                i++;
            }

            DownstreamTupleSender sender;
            if ( i == 0 )
            {
                sender = new NopDownstreamTupleSender();
            }
            else if ( i == 1 )
            {
                sender = senders[ 0 ];
            }
            else
            {
                sender = new CompositeDownstreamTupleSender( senders );
            }

            LOGGER.info( "Created {} for Pipeline {} replica index {}", sender.getClass().getSimpleName(), pipeline.getId(), replicaIndex );

            pipeline.setDownstreamTupleSender( replicaIndex, sender );
        }
    }

    private Map<String, List<Pair<Integer, Integer>>> getDownstreamConnectionsByOperatorId ( final FlowDef flow,
                                                                                             final OperatorDef operator )
    {
        final Map<String, List<Pair<Integer, Integer>>> result = new LinkedHashMap<>();
        final Map<Port, Collection<Port>> connections = flow.getDownstreamConnections( operator.id() );
        final Set<String> downstreamOperatorIds = new HashSet<>();
        for ( Collection<Port> c : connections.values() )
        {
            downstreamOperatorIds.addAll( c.stream().map( p -> p.operatorId ).collect( Collectors.toSet() ) );
        }

        final List<String> downstreamOperatorIdsSorted = new ArrayList<>( downstreamOperatorIds );
        sort( downstreamOperatorIdsSorted );

        for ( String downstreamOperatorId : downstreamOperatorIdsSorted )
        {
            for ( Entry<Port, Collection<Port>> e : connections.entrySet() )
            {
                final Port upstreamPort = e.getKey();
                e.getValue()
                 .stream()
                 .filter( downstreamPort -> downstreamPort.operatorId.equals( downstreamOperatorId ) )
                 .forEach( downstreamPort -> result.computeIfAbsent( downstreamOperatorId, s -> new ArrayList<>() )
                                                   .add( Pair.of( upstreamPort.portIndex, downstreamPort.portIndex ) ) );
            }
        }

        for ( List<Pair<Integer, Integer>> pairs : result.values() )
        {
            sort( pairs, ( p1, p2 ) ->
            {
                final int r = compare( p1._1, p2._1 );
                return r == 0 ? compare( p1._2, p2._2 ) : r;
            } );
        }

        return result;
    }

    private int[] getPartitionDistribution ( final OperatorDef operator )
    {
        final Pipeline pipeline = getPipeline( operator, 0 );
        final PartitionDistribution partitionDistribution = partitionService.getPartitionDistributionOrFail( pipeline.getId().regionId );
        return partitionDistribution.getDistribution();
    }

    private OperatorTupleQueue[] getPipelineTupleQueues ( final OperatorDef operator )
    {
        final Pipeline pipeline = getPipeline( operator, 0 );
        final int replicaCount = pipeline.getReplicaCount();
        final OperatorTupleQueue[] pipelineTupleQueues = new OperatorTupleQueue[ replicaCount ];
        for ( int i = 0; i < replicaCount; i++ )
        {
            pipelineTupleQueues[ i ] = pipeline.getPipelineReplica( i ).getPipelineTupleQueue();
        }

        return pipelineTupleQueues;
    }

    private void copyPorts ( final List<Pair<Integer, Integer>> pairs, final int[] sourcePorts, final int[] destinationPorts )
    {
        for ( int i = 0; i < pairs.size(); i++ )
        {
            sourcePorts[ i ] = pairs.get( i )._1;
            destinationPorts[ i ] = pairs.get( i )._2;
        }
    }

    private void createUpstreamContexts ( final FlowDef flow )
    {
        for ( Pipeline pipeline : pipelines.values() )
        {
            UpstreamContext upstreamContext = createUpstreamContext( flow, pipeline );
            LOGGER.info( "Created {} for pipeline {}", upstreamContext, pipeline.getId() );
            pipeline.setUpstreamContext( upstreamContext );
        }
    }

    private UpstreamContext createUpstreamContext ( final FlowDef flow, final Pipeline pipeline )
    {
        UpstreamContext upstreamContext;
        final OperatorDef firstOperator = pipeline.getFirstOperatorDef();
        if ( firstOperator.inputPortCount() == 0 )
        {
            upstreamContext = new UpstreamContext( 0, new UpstreamConnectionStatus[] {} );
        }
        else
        {
            final UpstreamConnectionStatus[] statuses = new UpstreamConnectionStatus[ firstOperator.inputPortCount() ];
            final Map<Port, Collection<Port>> upstreamConnections = flow.getUpstreamConnections( firstOperator.id() );
            for ( int portIndex = 0; portIndex < firstOperator.inputPortCount(); portIndex++ )
            {
                UpstreamConnectionStatus status = CLOSED;
                final Collection<Port> upstreamPorts = upstreamConnections.get( new Port( firstOperator.id(), portIndex ) );
                if ( upstreamPorts == null || upstreamPorts.isEmpty() )
                {
                    status = CLOSED;
                }
                else
                {
                    for ( Port upstreamPort : upstreamPorts )
                    {
                        final OperatorDef upstreamOperator = flow.getOperator( upstreamPort.operatorId );
                        final Pipeline upstreamPipeline = getPipeline( upstreamOperator, -1 );
                        final int operatorIndex = upstreamPipeline.getOperatorIndex( upstreamOperator );
                        checkState( ( upstreamPipeline.getOperatorCount() - 1 ) == operatorIndex,
                                    "Operator %s is supposed to be last operator of pipeline %s but it is on index %s",
                                    upstreamOperator.id(),
                                    upstreamPipeline.getId(),
                                    operatorIndex );
                        final OperatorReplicaStatus pipelineStatus = upstreamPipeline.getPipelineStatus();
                        if ( pipelineStatus == INITIAL || pipelineStatus == RUNNING || pipelineStatus == COMPLETING )
                        {
                            status = ACTIVE;
                            break;
                        }
                    }
                }
                statuses[ portIndex ] = status;
            }
            upstreamContext = new UpstreamContext( 0, statuses );
        }
        return upstreamContext;
    }

    private void createPipelineReplicaRunners ( final Supervisor supervisor )
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
                           "Operator {} is expected to be at %s'th index of pipeline %s but it is at %s'th index",
                           operator.id(),
                           pipeline.getId(),
                           i );
            return pipeline;
        }

        throw new IllegalArgumentException( "Operator " + operator.id() + " is not found in the pipelines" );
    }

    private Pipeline getPipelineOrFail ( final PipelineId id )
    {
        final Pipeline pipeline = pipelines.get( id );
        checkArgument( pipeline != null, "no pipeline found for pipeline instance id: %s", id );
        return pipeline;
    }

    private Pipeline getPipelineOrFail ( final OperatorDef operator )
    {
        return pipelines.values()
                        .stream()
                        .filter( p -> p.getOperatorIndex( operator ) != -1 )
                        .findFirst()
                        .orElseThrow( (Supplier<RuntimeException>) () -> new IllegalArgumentException( "No pipeline found for operator "
                                                                                                       + operator.id() ) );
    }

    private Pipeline getPipelineOrFail ( final OperatorDef operator, final int expectedOperatorIndex )
    {
        final Pipeline pipeline = getPipelineOrFail( operator );
        final int operatorIndex = pipeline.getOperatorIndex( operator );
        checkArgument( operatorIndex == expectedOperatorIndex,
                       "Pipeline %s has operator %s with index %s but expected index is %s",
                       pipeline.getId(),
                       operator.id(),
                       operatorIndex,
                       expectedOperatorIndex );
        return pipeline;
    }


    private void stopPipelineReplicaRunners ()
    {
        for ( Pipeline pipeline : pipelines.values() )
        {
            final long runnerStopTimeoutInMillis = jokerConfig.getPipelineManagerConfig().getRunnerStopTimeoutInMillis();
            pipeline.stopPipelineReplicaRunners( true, runnerStopTimeoutInMillis );
        }
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
            }
            else
            {
                LOGGER.info( "Shutting down flow..." );
            }

            status = FlowStatus.SHUT_DOWN;

            stopPipelineReplicaRunners();
            shutdownPipelines();
            releaseRegions();
        }
        catch ( Exception e )
        {
            LOGGER.error( "Shutdown failed", e );
        }
    }

    @FunctionalInterface
    private interface Function6<T1, T2, T3, T4, T5, T6>
    {
        T6 apply ( T1 t1, T2 t2, T3 t3, T4 t4, T5 t5 );
    }


    static class NopDownstreamTupleSender implements DownstreamTupleSender
    {

        @Override
        public Future<Void> send ( final TuplesImpl tuples )
        {
            return null;
        }

    }

}
