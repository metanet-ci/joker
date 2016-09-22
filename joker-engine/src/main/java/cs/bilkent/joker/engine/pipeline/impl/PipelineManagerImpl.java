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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
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
import static cs.bilkent.joker.engine.FlowStatus.SHUTTING_DOWN;
import cs.bilkent.joker.engine.config.JokerConfig;
import cs.bilkent.joker.engine.exception.InitializationException;
import cs.bilkent.joker.engine.partition.PartitionKeyFunction;
import cs.bilkent.joker.engine.partition.PartitionKeyFunctionFactory;
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
import cs.bilkent.joker.engine.pipeline.PipelineReplicaCompletionTracker;
import cs.bilkent.joker.engine.pipeline.PipelineReplicaId;
import cs.bilkent.joker.engine.pipeline.PipelineReplicaRunner;
import cs.bilkent.joker.engine.pipeline.UpstreamConnectionStatus;
import static cs.bilkent.joker.engine.pipeline.UpstreamConnectionStatus.ACTIVE;
import static cs.bilkent.joker.engine.pipeline.UpstreamConnectionStatus.CLOSED;
import static cs.bilkent.joker.engine.pipeline.UpstreamConnectionStatus.NO_CONNECTION;
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
import cs.bilkent.joker.engine.tuplequeue.TupleQueueContext;
import cs.bilkent.joker.engine.tuplequeue.impl.context.DefaultTupleQueueContext;
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

    private final PartitionKeyFunctionFactory partitionKeyFunctionFactory;

    private final ThreadGroup jokerThreadGroup;

    private final BiFunction<List<Pair<Integer, Integer>>, TupleQueueContext, DownstreamTupleSender>[]
            defaultDownstreamTupleSenderConstructors = new BiFunction[ 6 ];

    private final Function6<List<Pair<Integer, Integer>>, Integer, int[], TupleQueueContext[], PartitionKeyFunction,
                                   DownstreamTupleSender>[] partitionedDownstreamTupleSenderConstructors = new Function6[ 6 ];

    private FlowDeploymentDef flowDeployment;

    private final Map<PipelineId, Pipeline> pipelines = new ConcurrentHashMap<>();

    private final Map<PipelineId, Thread[]> pipelineThreads = new HashMap<>();

    private volatile FlowStatus status = FlowStatus.INITIAL;

    @Inject
    public PipelineManagerImpl ( final JokerConfig jokerConfig,
                                 final RegionManager regionManager,
                                 final PartitionService partitionService,
                                 final PartitionKeyFunctionFactory partitionKeyFunctionFactory,
                                 @Named( "jokerThreadGroup" ) final ThreadGroup jokerThreadGroup )
    {
        this.jokerConfig = jokerConfig;
        this.regionManager = regionManager;
        this.partitionService = partitionService;
        this.partitionKeyFunctionFactory = partitionKeyFunctionFactory;
        this.jokerThreadGroup = jokerThreadGroup;
        createDownstreamTupleSenderFactories( partitionService );
    }

    private void createDownstreamTupleSenderFactories ( final PartitionService partitionService )
    {
        defaultDownstreamTupleSenderConstructors[ 1 ] = ( pairs, tupleQueueContext ) ->
        {
            final Pair<Integer, Integer> pair1 = pairs.get( 0 );
            return new DownstreamTupleSender1( pair1._1, pair1._2, tupleQueueContext );
        };
        defaultDownstreamTupleSenderConstructors[ 2 ] = ( pairs, tupleQueueContext ) ->
        {
            final Pair<Integer, Integer> pair1 = pairs.get( 0 );
            final Pair<Integer, Integer> pair2 = pairs.get( 1 );
            return new DownstreamTupleSender2( pair1._1, pair1._2, pair2._1, pair2._2, tupleQueueContext );
        };
        defaultDownstreamTupleSenderConstructors[ 3 ] = ( pairs, tupleQueueContext ) ->
        {
            final Pair<Integer, Integer> pair1 = pairs.get( 0 );
            final Pair<Integer, Integer> pair2 = pairs.get( 1 );
            final Pair<Integer, Integer> pair3 = pairs.get( 2 );
            return new DownstreamTupleSender3( pair1._1, pair1._2, pair2._1, pair2._2, pair3._1, pair3._2, tupleQueueContext );
        };
        defaultDownstreamTupleSenderConstructors[ 4 ] = ( pairs, tupleQueueContext ) ->
        {
            final Pair<Integer, Integer> pair1 = pairs.get( 0 );
            final Pair<Integer, Integer> pair2 = pairs.get( 1 );
            final Pair<Integer, Integer> pair3 = pairs.get( 2 );
            final Pair<Integer, Integer> pair4 = pairs.get( 3 );
            return new DownstreamTupleSender4( pair1._1,
                                               pair1._2,
                                               pair2._1,
                                               pair2._2,
                                               pair3._1,
                                               pair3._2,
                                               pair4._1,
                                               pair4._2,
                                               tupleQueueContext );
        };
        defaultDownstreamTupleSenderConstructors[ 5 ] = ( pairs, tupleQueueContext ) ->
        {
            final int[] sourcePorts = new int[ pairs.size() ];
            final int[] destinationPorts = new int[ pairs.size() ];
            copyPorts( pairs, sourcePorts, destinationPorts );
            return new DownstreamTupleSenderN( sourcePorts, destinationPorts, tupleQueueContext );
        };
        partitionedDownstreamTupleSenderConstructors[ 1 ] = ( pairs, partitionCount, partitionDistribution, tupleQueueContexts,
                                                              partitionKeyFunction ) ->
        {
            final Pair<Integer, Integer> pair1 = pairs.get( 0 );
            return new PartitionedDownstreamTupleSender1( pair1._1,
                                                          pair1._2,
                                                          partitionService.getPartitionCount(),
                                                          partitionDistribution,
                                                          tupleQueueContexts,
                                                          partitionKeyFunction );
        };
        partitionedDownstreamTupleSenderConstructors[ 2 ] = ( pairs, partitionCount, partitionDistribution, tupleQueueContexts,
                                                              partitionKeyFunction ) ->
        {
            final Pair<Integer, Integer> pair1 = pairs.get( 0 );
            final Pair<Integer, Integer> pair2 = pairs.get( 1 );
            return new PartitionedDownstreamTupleSender2( pair1._1,
                                                          pair1._2,
                                                          pair2._1,
                                                          pair2._2,
                                                          partitionService.getPartitionCount(),
                                                          partitionDistribution,
                                                          tupleQueueContexts,
                                                          partitionKeyFunction );
        };
        partitionedDownstreamTupleSenderConstructors[ 3 ] = ( pairs, partitionCount, partitionDistribution, tupleQueueContexts,
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
                                                          partitionService.getPartitionCount(),
                                                          partitionDistribution,
                                                          tupleQueueContexts,
                                                          partitionKeyFunction );
        };
        partitionedDownstreamTupleSenderConstructors[ 4 ] = ( pairs, partitionCount, partitionDistribution, tupleQueueContexts,
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
                                                          partitionService.getPartitionCount(),
                                                          partitionDistribution,
                                                          tupleQueueContexts,
                                                          partitionKeyFunction );
        };
        partitionedDownstreamTupleSenderConstructors[ 5 ] = ( pairs, partitionCount, partitionDistribution, tupleQueueContexts,
                                                              partitionKeyFunction ) ->
        {
            final int[] sourcePorts = new int[ pairs.size() ];
            final int[] destinationPorts = new int[ pairs.size() ];
            copyPorts( pairs, sourcePorts, destinationPorts );
            return new PartitionedDownstreamTupleSenderN( sourcePorts,
                                                          destinationPorts,
                                                          partitionService.getPartitionCount(),
                                                          partitionDistribution,
                                                          tupleQueueContexts,
                                                          partitionKeyFunction );
        };
    }

    public List<Pipeline> getPipelines ()
    {
        return new ArrayList<>( pipelines.values() );
    }

    List<Pipeline> createPipelines ( final Supervisor supervisor, final FlowDef flow, final List<RegionConfig> regionConfigs )
    {
        final List<Pipeline> pipelines = createPipelineReplicas( flow, regionConfigs );
        createDownstreamTupleSenders( flow );
        createUpstreamContexts( flow );
        createPipelineReplicaRunners( supervisor );
        return pipelines;
    }

    @Override
    public void start ( final Supervisor supervisor, final FlowDeploymentDef flowDeployment, final List<RegionConfig> regionConfigs )
    {
        try
        {
            checkState( status == FlowStatus.INITIAL, "cannot start since status is %s", status );
            checkState( this.flowDeployment == null, "Flow deployment is already done!" );
            this.flowDeployment = flowDeployment;
            createPipelines( supervisor, flowDeployment.getFlow(), regionConfigs );
            initPipelineReplicas();
            startPipelineReplicaRunnerThreads();
            status = FlowStatus.RUNNING;
        }
        catch ( Exception e )
        {
            status = FlowStatus.INITIALIZATION_FAILED;
            throw new InitializationException( "Flow start failed", e );
        }
    }

    private void initPipelineReplicas ()
    {
        final List<PipelineReplica> pipelineReplicas = new ArrayList<>();
        try
        {
            for ( Pipeline pipeline : pipelines.values() )
            {
                final UpstreamContext upstreamContext = pipeline.getUpstreamContext();
                for ( int replicaIndex = 0; replicaIndex < pipeline.getReplicaCount(); replicaIndex++ )
                {
                    final PipelineReplica pipelineReplica = pipeline.getPipelineReplica( replicaIndex );
                    final PipelineReplicaCompletionTracker tracker = pipeline.getPipelineReplicaRunner( replicaIndex )
                                                                             .getPipelineReplicaCompletionTracker();
                    LOGGER.info( "Initializing Replica {} of Pipeline {}", replicaIndex, pipeline.getId() );
                    pipelineReplica.init( upstreamContext, tracker );
                    pipelineReplicas.add( pipelineReplica );
                }

                final SchedulingStrategy initialSchedulingStrategy = pipeline.getPipelineReplica( 0 )
                                                                             .getOperator( 0 )
                                                                             .getInitialSchedulingStrategy();
                for ( int replicaIndex = 1; replicaIndex < pipeline.getReplicaCount(); replicaIndex++ )
                {
                    checkState( initialSchedulingStrategy.equals( pipeline.getPipelineReplica( replicaIndex )
                                                                          .getOperator( 0 )
                                                                          .getInitialSchedulingStrategy() ),
                                "Pipeline %s Replica %s has different Scheduling Strategy: %s than 0th Replica's Scheduling Strategy: ",
                                pipeline.getId(),
                                replicaIndex,
                                initialSchedulingStrategy );
                }
                pipeline.setInitialSchedulingStrategy( initialSchedulingStrategy );
            }
        }
        catch ( Exception e1 )
        {
            LOGGER.error( "Pipeline initialization failed.", e1 );
            reverse( pipelineReplicas );
            LOGGER.error( "Shutting down {} pipeline replicas.", pipelineReplicas.size() );
            for ( PipelineReplica p : pipelineReplicas )
            {
                try
                {
                    LOGGER.info( "Shutting down Pipeline Replica {}", p.id() );
                    p.shutdown();
                }
                catch ( Exception e2 )
                {
                    LOGGER.error( "Shutdown of Pipeline " + p.id() + " failed", e2 );
                }
            }

            releaseRegions();

            throw e1;
        }
    }

    private void startPipelineReplicaRunnerThreads ()
    {
        for ( Pipeline pipeline : pipelines.values() )
        {
            final int replicaCount = pipeline.getReplicaCount();
            final Thread[] threads = pipelineThreads.computeIfAbsent( pipeline.getId(), ( pipelineId ) -> new Thread[ replicaCount ] );
            for ( int replicaIndex = 0; replicaIndex < replicaCount; replicaIndex++ )
            {
                LOGGER.info( "Starting thread for Pipeline {} Replica {}", pipeline.getId(), replicaIndex );
                final String threadName = jokerThreadGroup.getName() + "-" + pipeline.getPipelineReplica( replicaIndex ).id();
                threads[ replicaIndex ] = new Thread( jokerThreadGroup, pipeline.getPipelineReplicaRunner( replicaIndex ), threadName );
            }
        }

        for ( Thread[] threads : pipelineThreads.values() )
        {
            for ( Thread thread : threads )
            {
                thread.start();
            }
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
    public boolean notifyPipelineReplicaCompleted ( final PipelineReplicaId id )
    {
        checkState( status == FlowStatus.SHUTTING_DOWN, "cannot notify Pipeline Replica %s completion since status is %s", id, status );

        final Pipeline pipeline = getPipelineOrFail( id.pipelineId );
        final OperatorReplicaStatus pipelineStatus = pipeline.getPipelineStatus();
        checkState( pipelineStatus == COMPLETING,
                    "Pipeline %s can not receive completion notification in %s status",
                    id.pipelineId,
                    pipelineStatus );

        LOGGER.info( "Pipeline replica {} is completed.", id );

        if ( pipeline.setPipelineReplicaCompleted( id.replicaIndex ) )
        {
            for ( Pipeline downstreamPipeline : getDownstreamPipelines( pipeline ) )
            {
                final UpstreamContext updatedUpstreamContext = getUpdatedUpstreamContext( downstreamPipeline );
                if ( updatedUpstreamContext != null )
                {
                    handlePipelineUpstreamUpdated( downstreamPipeline, updatedUpstreamContext );
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
                status = NO_CONNECTION;
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
                                                                                      || s == COMPLETING;
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

        status = SHUTTING_DOWN;

        for ( Pipeline pipeline : pipelines.values() )
        {
            final OperatorDef operatorDef = pipeline.getFirstOperatorDef();
            if ( flowDeployment.getFlow().getUpstreamConnections( operatorDef.id() ).isEmpty() )
            {
                handlePipelineUpstreamUpdated( pipeline, new UpstreamContext( 1, new UpstreamConnectionStatus[] {} ) );
            }
        }
    }

    private void handlePipelineUpstreamUpdated ( final Pipeline pipeline, final UpstreamContext upstreamContext )
    {
        LOGGER.info( "Updating upstream context of Pipeline {} to {}", pipeline.getId(), upstreamContext );

        final OperatorReplicaStatus pipelineStatus = pipeline.getPipelineStatus();
        checkState( pipelineStatus == OperatorReplicaStatus.RUNNING || pipelineStatus == OperatorReplicaStatus.COMPLETING,
                    "Cannot handle updated pipeline upstream since Pipeline %s is in %s status",
                    pipeline.getId(),
                    pipelineStatus );

        pipeline.setUpstreamContext( upstreamContext );
        if ( pipelineStatus == OperatorReplicaStatus.RUNNING && !upstreamContext.isInvokable( pipeline.getFirstOperatorDef(),
                                                                                              pipeline.getInitialSchedulingStrategy() ) )
        {
            LOGGER.info( "Pipeline {} is not invokable anymore. Setting pipeline status to {}", pipeline.getId(), COMPLETING );
            pipeline.setPipelineCompleting();
        }

        notifyPipelineReplicaRunners( pipeline, upstreamContext );
    }

    private void notifyPipelineReplicaRunners ( final Pipeline pipeline, final UpstreamContext upstreamContext )
    {
        LOGGER.info( "Notifying runners about new {} of Pipeline {}", upstreamContext, pipeline.getId() );
        for ( int i = 0; i < pipeline.getReplicaCount(); i++ )
        {
            final PipelineReplicaRunner runner = pipeline.getPipelineReplicaRunner( i );
            runner.updatePipelineUpstreamContext();
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

    private List<Pipeline> createPipelineReplicas ( final FlowDef flow, final Collection<RegionConfig> regionConfigs )
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
                final Pipeline pipeline = new Pipeline( pipelineId, regionConfig );

                checkState( this.pipelines.put( pipelineId, pipeline ) == null,
                            "there are multiple pipelines with same id: %s",
                            pipelineId );
                pipelines.add( pipeline );

                for ( int replicaIndex = 0; replicaIndex < pipeline.getReplicaCount(); replicaIndex++ )
                {
                    pipeline.setPipelineReplica( replicaIndex, pipelineReplicas[ replicaIndex ] );
                }
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
                final TupleQueueContext[] pipelineTupleQueueContexts = getPipelineTupleQueueContexts( downstreamOperator );
                final int j = min( pairs.size(), DOWNSTREAM_TUPLE_SENDER_CONSTRUCTOR_COUNT );

                if ( pipeline.getId().regionId == downstreamPipeline.getId().regionId )
                {
                    final TupleQueueContext pipelineTupleQueueContext = pipelineTupleQueueContexts[ replicaIndex ];
                    senders[ i ] = defaultDownstreamTupleSenderConstructors[ j ].apply( pairs, pipelineTupleQueueContext );
                }
                else if ( downstreamRegionDef.getRegionType() == PARTITIONED_STATEFUL )
                {
                    final int[] partitionDistribution = getPartitionDistribution( downstreamOperator );
                    final PartitionKeyFunction partitionKeyFunction = partitionKeyFunctionFactory.createPartitionKeyFunction(
                            downstreamRegionDef.getPartitionFieldNames() );
                    senders[ i ] = partitionedDownstreamTupleSenderConstructors[ j ].apply( pairs,
                                                                                            partitionService.getPartitionCount(),
                                                                                            partitionDistribution,
                                                                                            pipelineTupleQueueContexts,
                                                                                            partitionKeyFunction );
                }
                else if ( downstreamRegionDef.getRegionType() == STATELESS )
                {
                    TupleQueueContext pipelineTupleQueueContext = null;
                    if ( pipeline.getReplicaCount() == downstreamPipeline.getReplicaCount() )
                    {
                        pipelineTupleQueueContext = pipelineTupleQueueContexts[ replicaIndex ];
                    }
                    else if ( downstreamPipeline.getReplicaCount() == 1 )
                    {
                        pipelineTupleQueueContext = pipelineTupleQueueContexts[ 0 ];

                    }

                    if ( pipelineTupleQueueContext != null )
                    {
                        senders[ i ] = defaultDownstreamTupleSenderConstructors[ j ].apply( pairs, pipelineTupleQueueContext );
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
                    final int l = pipelineTupleQueueContexts.length;
                    checkState( l == 1, "Operator %s can not have %s replicas", downstreamOperatorId, l );
                    final TupleQueueContext pipelineTupleQueueContext = pipelineTupleQueueContexts[ 0 ];
                    senders[ i ] = defaultDownstreamTupleSenderConstructors[ j ].apply( pairs, pipelineTupleQueueContext );
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
        return partitionService.getOrCreatePartitionDistribution( pipeline.getId().regionId, pipeline.getReplicaCount() );
    }

    private TupleQueueContext[] getPipelineTupleQueueContexts ( final OperatorDef operator )
    {
        final Pipeline pipeline = getPipeline( operator, 0 );
        final int replicaCount = pipeline.getReplicaCount();
        final TupleQueueContext[] tupleQueueContexts = new TupleQueueContext[ replicaCount ];
        for ( int i = 0; i < replicaCount; i++ )
        {
            tupleQueueContexts[ i ] = pipeline.getPipelineReplica( i ).getUpstreamTupleQueueContext();
        }

        return tupleQueueContexts;
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
                        status = NO_CONNECTION;
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

            LOGGER.info( "Created {} for pipeline {}", upstreamContext, pipeline.getId() );
            pipeline.setUpstreamContext( upstreamContext );
        }
    }

    private void createPipelineReplicaRunners ( final Supervisor supervisor )
    {
        for ( Pipeline pipeline : pipelines.values() )
        {
            for ( int replicaIndex = 0; replicaIndex < pipeline.getReplicaCount(); replicaIndex++ )
            {
                final PipelineReplica pipelineReplica = pipeline.getPipelineReplica( replicaIndex );
                final PipelineReplicaCompletionTracker tracker = new PipelineReplicaCompletionTracker( pipelineReplica.id(),
                                                                                                       pipelineReplica.getOperatorCount() );
                final PipelineReplicaRunner runner = new PipelineReplicaRunner( jokerConfig,
                                                                                pipelineReplica, supervisor, tracker,
                                                                                pipeline.getDownstreamTupleSender( replicaIndex ) );
                pipeline.setPipelineReplicaRunner( replicaIndex, runner );
                LOGGER.info( "Created runner for pipeline replica: {}", pipelineReplica.id() );
            }
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
        checkArgument( pipeline != null, "no pipeline runtime context found for pipeline instance id: " + id );
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
        final List<Future<Boolean>> futures = new ArrayList<>();
        for ( Pipeline pipeline : pipelines.values() )
        {
            for ( int replicaIndex = 0; replicaIndex < pipeline.getReplicaCount(); replicaIndex++ )
            {
                final PipelineReplica pipelineReplica = pipeline.getPipelineReplica( replicaIndex );
                final TupleQueueContext upstreamTupleQueueContext = pipelineReplica.getUpstreamTupleQueueContext();
                if ( upstreamTupleQueueContext instanceof DefaultTupleQueueContext )
                {
                    final DefaultTupleQueueContext d = (DefaultTupleQueueContext) upstreamTupleQueueContext;
                    for ( int portIndex = 0; portIndex < d.getInputPortCount(); portIndex++ )
                    {
                        LOGGER.warn( "Pipeline {} upstream tuple queue capacity check is disabled", pipelineReplica.id() );
                        d.disableCapacityCheck( portIndex );
                    }
                }

                final PipelineReplicaRunner pipelineReplicaRunner = pipeline.getPipelineReplicaRunner( replicaIndex );
                futures.add( pipelineReplicaRunner.stop() );
            }
        }

        for ( Future<Boolean> future : futures )
        {
            try
            {
                future.get( 2, TimeUnit.MINUTES );
            }
            catch ( InterruptedException e )
            {
                LOGGER.error( "TaskRunner is interrupted while waiting for pipeline replica runner to stop", e );
                Thread.currentThread().interrupt();
            }
            catch ( ExecutionException e )
            {
                LOGGER.error( "Stop pipeline replica runner failed", e );
            }
            catch ( TimeoutException e )
            {
                LOGGER.error( "Stop pipeline replica runner timed out", e );
            }
        }
    }

    private void awaitPipelineThreads ()
    {
        LOGGER.warn( "Joining pipeline threads" );

        for ( Thread[] threads : pipelineThreads.values() )
        {
            for ( Thread thread : threads )
            {
                try
                {
                    thread.join( TimeUnit.MINUTES.toMillis( 2 ) );
                }
                catch ( InterruptedException e )
                {
                    LOGGER.error( "TaskRunner is interrupted while it is joined to pipeline runner thread", e );
                    Thread.currentThread().interrupt();
                }
            }
        }

        pipelineThreads.clear();

        LOGGER.warn( "All pipeline threads are finished." );
    }

    private void shutdownPipelines ()
    {
        for ( Pipeline pipeline : pipelines.values() )
        {
            for ( int replicaIndex = 0; replicaIndex < pipeline.getReplicaCount(); replicaIndex++ )
            {
                final PipelineReplica replica = pipeline.getPipelineReplica( replicaIndex );
                try
                {
                    replica.shutdown();
                }
                catch ( Exception e )
                {
                    LOGGER.error( "PipelineReplica " + replica.id() + " releaseRegions failed", e );
                }
            }
        }
    }

    @Override
    public void notifyPipelineReplicaFailed ( final PipelineReplicaId id, final Throwable failure )
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
            awaitPipelineThreads();
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
