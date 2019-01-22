package cs.bilkent.joker.engine.region.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.IntStream;
import javax.annotation.concurrent.NotThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.inject.Inject;
import com.google.inject.Singleton;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import cs.bilkent.joker.engine.config.JokerConfig;
import cs.bilkent.joker.engine.config.ThreadingPref;
import static cs.bilkent.joker.engine.config.ThreadingPref.MULTI_THREADED;
import static cs.bilkent.joker.engine.config.ThreadingPref.SINGLE_THREADED;
import cs.bilkent.joker.engine.exception.InitializationException;
import cs.bilkent.joker.engine.flow.PipelineId;
import cs.bilkent.joker.engine.flow.RegionDef;
import cs.bilkent.joker.engine.flow.RegionExecPlan;
import cs.bilkent.joker.engine.kvstore.OperatorKVStore;
import cs.bilkent.joker.engine.kvstore.OperatorKVStoreManager;
import cs.bilkent.joker.engine.kvstore.impl.DefaultOperatorKVStore;
import cs.bilkent.joker.engine.kvstore.impl.EmptyOperatorKVStore;
import cs.bilkent.joker.engine.kvstore.impl.PartitionedOperatorKVStore;
import cs.bilkent.joker.engine.metric.PipelineReplicaMeter;
import cs.bilkent.joker.engine.partition.PartitionDistribution;
import cs.bilkent.joker.engine.partition.PartitionKeyExtractor;
import cs.bilkent.joker.engine.partition.PartitionKeyExtractorFactory;
import cs.bilkent.joker.engine.partition.PartitionService;
import static cs.bilkent.joker.engine.partition.PartitionUtil.getPartitionId;
import cs.bilkent.joker.engine.pipeline.OperatorReplica;
import cs.bilkent.joker.engine.pipeline.PipelineReplica;
import cs.bilkent.joker.engine.pipeline.PipelineReplicaId;
import cs.bilkent.joker.engine.pipeline.UpstreamCtx;
import static cs.bilkent.joker.engine.pipeline.UpstreamCtx.createInitialUpstreamCtx;
import cs.bilkent.joker.engine.pipeline.impl.invocation.DefaultOutputCollector;
import cs.bilkent.joker.engine.pipeline.impl.invocation.FusedInvocationCtx;
import cs.bilkent.joker.engine.pipeline.impl.invocation.FusedPartitionedInvocationCtx;
import cs.bilkent.joker.engine.region.PipelineTransformer;
import cs.bilkent.joker.engine.region.Region;
import static cs.bilkent.joker.engine.region.Region.findFusionStartIndices;
import cs.bilkent.joker.engine.region.RegionManager;
import static cs.bilkent.joker.engine.region.impl.RegionExecPlanUtil.getMergeablePipelineStartIndices;
import static cs.bilkent.joker.engine.region.impl.RegionExecPlanUtil.getPipelineStartIndicesToSplit;
import cs.bilkent.joker.engine.tuplequeue.OperatorQueue;
import cs.bilkent.joker.engine.tuplequeue.OperatorQueueManager;
import cs.bilkent.joker.engine.tuplequeue.TupleQueueDrainer;
import cs.bilkent.joker.engine.tuplequeue.TupleQueueDrainerPool;
import static cs.bilkent.joker.engine.tuplequeue.impl.drainer.NonBlockingMultiPortDisjunctiveDrainer.newGreedyDrainer;
import cs.bilkent.joker.engine.tuplequeue.impl.drainer.pool.BlockingTupleQueueDrainerPool;
import cs.bilkent.joker.engine.tuplequeue.impl.drainer.pool.NonBlockingTupleQueueDrainerPool;
import cs.bilkent.joker.engine.tuplequeue.impl.operator.DefaultOperatorQueue;
import cs.bilkent.joker.engine.tuplequeue.impl.operator.EmptyOperatorQueue;
import cs.bilkent.joker.engine.tuplequeue.impl.operator.PartitionedOperatorQueue;
import static cs.bilkent.joker.engine.util.ExceptionUtils.checkInterruption;
import cs.bilkent.joker.flow.FlowDef;
import cs.bilkent.joker.operator.Operator;
import cs.bilkent.joker.operator.OperatorDef;
import cs.bilkent.joker.operator.Tuple;
import cs.bilkent.joker.operator.impl.DefaultInvocationCtx;
import cs.bilkent.joker.operator.impl.InitCtxImpl;
import cs.bilkent.joker.operator.impl.InternalInvocationCtx;
import cs.bilkent.joker.operator.impl.OutputCollector;
import cs.bilkent.joker.operator.impl.TuplesImpl;
import cs.bilkent.joker.operator.kvstore.KVStore;
import cs.bilkent.joker.operator.scheduling.ScheduleWhenTuplesAvailable;
import cs.bilkent.joker.operator.scheduling.SchedulingStrategy;
import static cs.bilkent.joker.operator.spec.OperatorType.PARTITIONED_STATEFUL;
import static cs.bilkent.joker.operator.spec.OperatorType.STATEFUL;
import static cs.bilkent.joker.operator.spec.OperatorType.STATELESS;
import cs.bilkent.joker.partition.impl.PartitionKey;
import static java.lang.Math.max;
import static java.lang.System.arraycopy;
import static java.util.stream.Collectors.toList;

@Singleton
@NotThreadSafe
public class RegionManagerImpl implements RegionManager
{

    private static final Logger LOGGER = LoggerFactory.getLogger( RegionManagerImpl.class );


    private final JokerConfig config;

    private final PartitionService partitionService;

    private final OperatorKVStoreManager operatorKvStoreManager;

    private final OperatorQueueManager operatorQueueManager;

    private final PipelineTransformer pipelineTransformer;

    private final PartitionKeyExtractorFactory partitionKeyExtractorFactory;

    private final Map<Integer, Region> regions = new HashMap<>();

    @Inject
    public RegionManagerImpl ( final JokerConfig config,
                               final PartitionService partitionService,
                               final OperatorKVStoreManager operatorKvStoreManager,
                               final OperatorQueueManager operatorQueueManager,
                               final PipelineTransformer pipelineTransformer,
                               final PartitionKeyExtractorFactory partitionKeyExtractorFactory )
    {
        this.config = config;
        this.partitionService = partitionService;
        this.operatorKvStoreManager = operatorKvStoreManager;
        this.operatorQueueManager = operatorQueueManager;
        this.pipelineTransformer = pipelineTransformer;
        this.partitionKeyExtractorFactory = partitionKeyExtractorFactory;
    }

    @Override
    public Region createRegion ( final FlowDef flow, final RegionExecPlan regionExecPlan )
    {
        checkArgument( flow != null, "flow is null" );
        checkState( !regions.containsKey( regionExecPlan.getRegionId() ), "Region %s is already created!", regionExecPlan.getRegionId() );

        final int regionId = regionExecPlan.getRegionId();
        final int replicaCount = regionExecPlan.getReplicaCount();
        final int pipelineCount = regionExecPlan.getPipelineCount();

        final RegionDef regionDef = regionExecPlan.getRegionDef();
        final int regionOperatorCount = regionDef.getOperatorCount();

        checkPipelineStartIndices( regionId, regionOperatorCount, regionExecPlan.getPipelineStartIndices() );

        LOGGER.debug( "Creating components for regionId={} pipelineCount={} replicaCount={}", regionId, pipelineCount, replicaCount );

        if ( regionDef.getRegionType() == PARTITIONED_STATEFUL )
        {
            partitionService.createPartitionDistribution( regionId, replicaCount );
        }

        final PipelineReplica[][] pipelineReplicas = new PipelineReplica[ pipelineCount ][ replicaCount ];
        final SchedulingStrategy[] schedulingStrategies = new SchedulingStrategy[ regionOperatorCount ];
        final UpstreamCtx[] upstreamCtxes = new UpstreamCtx[ regionOperatorCount ];
        preInitializeRegionOperators( flow, regionDef, schedulingStrategies, upstreamCtxes );

        for ( int pipelineIndex = 0; pipelineIndex < pipelineCount; pipelineIndex++ )
        {
            final int pipelineId = regionExecPlan.getPipelineStartIndex( pipelineIndex );
            final OperatorDef[] operatorDefs = regionExecPlan.getOperatorDefsByPipelineStartIndex( pipelineId );
            final int pipelineOperatorCount = operatorDefs.length;

            final SchedulingStrategy[] pipelineSchedulingStrategies = new SchedulingStrategy[ pipelineOperatorCount ];
            arraycopy( schedulingStrategies, pipelineId, pipelineSchedulingStrategies, 0, pipelineOperatorCount );
            final int[] fusionStartIndices = findFusionStartIndices( pipelineSchedulingStrategies );

            final int pipelineOperatorReplicaCount = fusionStartIndices.length;
            final OperatorReplica[][] operatorReplicas = new OperatorReplica[ replicaCount ][ pipelineOperatorReplicaCount ];

            LOGGER.debug( "Initializing pipeline replica for regionId={} pipelineIndex={} with {} fused operators",
                          regionId,
                          pipelineIndex,
                          pipelineOperatorReplicaCount );

            final PipelineReplicaId[] pipelineReplicaIds = createPipelineReplicaIds( regionId, replicaCount, pipelineId );
            final int forwardedKeySize = regionDef.getForwardedKeySize();

            final PipelineReplicaMeter[] replicaMeters = new PipelineReplicaMeter[ replicaCount ];
            for ( int replicaIndex = 0; replicaIndex < replicaCount; replicaIndex++ )
            {
                replicaMeters[ replicaIndex ] = new PipelineReplicaMeter( config.getMetricManagerConfig().getTickMask(),
                                                                          pipelineReplicaIds[ replicaIndex ],
                                                                          operatorDefs[ 0 ] );
            }

            for ( int i = 0; i < pipelineOperatorReplicaCount; i++ )
            {
                final int fromOperatorIndex = fusionStartIndices[ i ];
                final OperatorQueue[] operatorQueues = createOperatorQueues( flow,
                                                                             regionId,
                                                                             replicaCount,
                                                                             ( fromOperatorIndex == 0 ),
                                                                             operatorDefs[ fromOperatorIndex ],
                                                                             forwardedKeySize );

                final TupleQueueDrainerPool[] drainerPools = createTupleQueueDrainerPools( regionId,
                                                                                           replicaCount,
                                                                                           ( fromOperatorIndex == 0 ),
                                                                                           operatorDefs[ fromOperatorIndex ] );

                final int toOperatorIndex = ( i < fusionStartIndices.length - 1 ) ? fusionStartIndices[ i + 1 ] : pipelineOperatorCount;
                final int fusedOperatorCount = toOperatorIndex - fromOperatorIndex;

                final OperatorDef[][] fusedOperatorDefs = new OperatorDef[ replicaCount ][ fusedOperatorCount ];
                final InternalInvocationCtx[][] fusedInvocationCtxes = new InternalInvocationCtx[ replicaCount ][ fusedOperatorCount ];

                for ( int j = fusedOperatorCount - 1; j >= 0; j-- )
                {
                    final OperatorDef operatorDef = operatorDefs[ fromOperatorIndex + j ];
                    final int inputPortCount = operatorDef.getInputPortCount();
                    final OperatorKVStore[] operatorKvStores = createOperatorKVStores( regionId, replicaCount, operatorDef );

                    for ( int replicaIndex = 0; replicaIndex < replicaCount; replicaIndex++ )
                    {
                        fusedOperatorDefs[ replicaIndex ][ j ] = operatorDef;
                        final Function<PartitionKey, KVStore> kvStoreSupplier = operatorKvStores[ replicaIndex ]::getKVStore;

                        final OutputCollector outputCollector;
                        if ( j == fusedOperatorCount - 1 )
                        {
                            // TODO we should use the next operatorDef...
                            final TuplesImpl output = new TuplesImpl( operatorDef.getOutputPortCount() );
                            outputCollector = new DefaultOutputCollector( output );
                        }
                        else
                        {
                            outputCollector = (OutputCollector) fusedInvocationCtxes[ replicaIndex ][ j + 1 ];
                        }

                        final InternalInvocationCtx invocationCtx;
                        if ( j == 0 )
                        {
                            invocationCtx = new DefaultInvocationCtx( inputPortCount, kvStoreSupplier, outputCollector );
                        }
                        else if ( operatorDef.getOperatorType() == PARTITIONED_STATEFUL )
                        {
                            final List<String> partitionFieldNames = operatorDef.getPartitionFieldNames();
                            final PartitionKeyExtractor ext = partitionKeyExtractorFactory.createPartitionKeyExtractor( partitionFieldNames,
                                                                                                                        forwardedKeySize );
                            invocationCtx = new FusedPartitionedInvocationCtx( inputPortCount, kvStoreSupplier, ext, outputCollector );
                        }
                        else
                        {
                            invocationCtx = new FusedInvocationCtx( inputPortCount, kvStoreSupplier, outputCollector );
                        }

                        fusedInvocationCtxes[ replicaIndex ][ j ] = invocationCtx;
                    }
                }

                for ( int replicaIndex = 0; replicaIndex < replicaCount; replicaIndex++ )
                {
                    final DefaultInvocationCtx ctx = (DefaultInvocationCtx) fusedInvocationCtxes[ replicaIndex ][ 0 ];
                    operatorReplicas[ replicaIndex ][ i ] = new OperatorReplica( config, pipelineReplicaIds[ replicaIndex ],
                                                                                 operatorQueues[ replicaIndex ],
                                                                                 drainerPools[ replicaIndex ],
                                                                                 replicaMeters[ replicaIndex ],
                                                                                 ctx::createInputTuples,
                                                                                 fusedOperatorDefs[ replicaIndex ],
                                                                                 fusedInvocationCtxes[ replicaIndex ] );
                }
            }

            for ( int replicaIndex = 0; replicaIndex < replicaCount; replicaIndex++ )
            {
                LOGGER.debug( "Creating pipeline instance for regionId={} replicaIndex={} pipelineIndex={} pipelineStartIndex={}",
                              regionId,
                              replicaIndex,
                              pipelineIndex,
                              pipelineId );
                final OperatorReplica[] pipelineOperatorReplicas = operatorReplicas[ replicaIndex ];
                final OperatorQueue pipelineQueue = createPipelineQueue( flow, regionId, replicaIndex, pipelineOperatorReplicas );

                pipelineReplicas[ pipelineIndex ][ replicaIndex ] = new PipelineReplica( config, pipelineReplicaIds[ replicaIndex ],
                                                                                         pipelineOperatorReplicas,
                                                                                         pipelineQueue,
                                                                                         replicaMeters[ replicaIndex ] );
            }
        }

        final Region region = new Region( regionExecPlan, schedulingStrategies, upstreamCtxes, pipelineReplicas );
        regions.put( regionId, region );
        return region;
    }

    @Override
    public void validatePipelineMergeParameters ( final List<PipelineId> pipelineIds )
    {
        final Region region = regions.get( pipelineIds.get( 0 ).getRegionId() );
        checkArgument( region != null, "no region found for %s", pipelineIds );
        getMergeablePipelineStartIndices( region.getExecPlan(), pipelineIds );
    }

    @Override
    public Region mergePipelines ( final List<PipelineId> pipelineIdsToMerge )
    {
        final Region region = regions.get( pipelineIdsToMerge.get( 0 ).getRegionId() );
        final int regionId = region.getRegionId();

        final List<Integer> startIndicesToMerge = getMergeablePipelineStartIndices( region.getExecPlan(), pipelineIdsToMerge );

        regions.remove( regionId );

        final Region newRegion = pipelineTransformer.mergePipelines( region, startIndicesToMerge );
        regions.put( regionId, newRegion );
        return newRegion;
    }

    @Override
    public void validatePipelineSplitParameters ( final PipelineId pipelineId, final List<Integer> pipelineOperatorIndicesToSplit )
    {
        final Region region = regions.get( pipelineId.getRegionId() );
        checkArgument( region != null, "invalid Pipeline %s to split", pipelineId );
        getPipelineStartIndicesToSplit( region.getExecPlan(), pipelineId, pipelineOperatorIndicesToSplit );
    }

    @Override
    public Region splitPipeline ( final PipelineId pipelineId, final List<Integer> pipelineOperatorIndicesToSplit )
    {
        validatePipelineSplitParameters( pipelineId, pipelineOperatorIndicesToSplit );
        final int regionId = pipelineId.getRegionId();
        final Region region = regions.remove( regionId );
        final List<Integer> pipelineStartIndicesToSplit = getPipelineStartIndicesToSplit( region.getExecPlan(),
                                                                                          pipelineId,
                                                                                          pipelineOperatorIndicesToSplit );

        final Region newRegion = pipelineTransformer.splitPipeline( region, pipelineStartIndicesToSplit );
        regions.put( regionId, newRegion );

        return newRegion;
    }

    @Override
    public Region rebalanceRegion ( final FlowDef flow, final int regionId, final int newReplicaCount )
    {
        checkArgument( flow != null, "flow is null" );
        checkArgument( newReplicaCount > 0, "cannot rebalance regionId=%s since replica count is %s", regionId, newReplicaCount );

        final Region region = regions.remove( regionId );
        checkArgument( region != null, "invalid region %s to rebalance", region );

        final RegionExecPlan regionExecPlan = region.getExecPlan();
        final RegionDef regionDef = regionExecPlan.getRegionDef();
        checkState( regionDef.getRegionType() == PARTITIONED_STATEFUL,
                    "cannot rebalance %s regionId=%s",
                    regionDef.getRegionType(),
                    regionId );

        if ( newReplicaCount == regionExecPlan.getReplicaCount() )
        {
            regions.put( regionId, region );
            LOGGER.warn( "No rebalance since regionId={} already has the same replica count={}", regionId, newReplicaCount );
            return region;
        }

        LOGGER.info( "Rebalancing regionId={} to new replica count: {} from current replica count: {}",
                     regionId,
                     newReplicaCount,
                     regionExecPlan.getReplicaCount() );

        drainPipelineQueues( region );

        rebalanceRegion( region, newReplicaCount );

        final Region newRegion;
        if ( regionExecPlan.getReplicaCount() < newReplicaCount )
        {
            newRegion = expandRegionReplicas( flow, region, newReplicaCount );
        }
        else
        {
            newRegion = shrinkRegionReplicas( region, newReplicaCount );
        }

        regions.put( regionId, newRegion );

        return newRegion;
    }

    private void drainPipelineQueues ( final Region region )
    {
        final RegionExecPlan execPlan = region.getExecPlan();
        for ( int pipelineIndex = 0; pipelineIndex < execPlan.getPipelineCount(); pipelineIndex++ )
        {
            for ( PipelineReplica pipelineReplica : region.getPipelineReplicas( pipelineIndex ) )
            {
                final OperatorQueue pipelineQueue = pipelineReplica.getQueue();
                final OperatorReplica operator = pipelineReplica.getOperatorReplica( 0 );
                final OperatorQueue operatorQueue = operator.getQueue();
                final TuplesImpl result = new TuplesImpl( operatorQueue.getInputPortCount() );
                final TupleQueueDrainer drainer = newGreedyDrainer( operatorQueue.getOperatorId(),
                                                                    operatorQueue.getInputPortCount(),
                                                                    config.getTupleQueueManagerConfig().getTupleQueueCapacity() );
                pipelineQueue.drain( drainer, k -> result );
                if ( result.isNonEmpty() )
                {
                    LOGGER.debug( "Draining pipeline queue of {}", pipelineReplica.id() );
                    for ( int portIndex = 0; portIndex < result.getPortCount(); portIndex++ )
                    {
                        final List<Tuple> tuples = result.getTuples( portIndex );
                        if ( tuples.size() > 0 )
                        {
                            final int offered = operatorQueue.offer( portIndex, tuples );
                            checkState( offered == tuples.size() );
                        }
                    }
                }
            }
        }
    }

    private void rebalanceRegion ( final Region region, final int newReplicaCount )
    {
        final int regionId = region.getRegionId();

        final PartitionDistribution currentPartitionDistribution = partitionService.getPartitionDistributionOrFail( regionId );
        final PartitionDistribution newPartitionDistribution = partitionService.rebalancePartitionDistribution( regionId, newReplicaCount );

        final RegionExecPlan regionExecPlan = region.getExecPlan();

        final int currentReplicaCount = currentPartitionDistribution.getReplicaCount();

        for ( int pipelineIndex = 0; pipelineIndex < regionExecPlan.getPipelineCount(); pipelineIndex++ )
        {
            final PipelineReplica[] pipelineReplicas = region.getPipelineReplicas( pipelineIndex );
            final OperatorDef[] operatorDefs = regionExecPlan.getOperatorDefsByPipelineIndex( pipelineIndex );
            rebalancePartitionedStatefulOperators( regionId, currentPartitionDistribution, newPartitionDistribution, operatorDefs );
            if ( operatorDefs[ 0 ].getOperatorType() == STATELESS )
            {
                final OperatorQueue[] queues = Arrays.stream( pipelineReplicas )
                                                     .map( p -> p.getOperatorReplica( 0 ).getQueue() )
                                                     .toArray( OperatorQueue[]::new );
                final PartitionKeyExtractor e = partitionKeyExtractorFactory.createPartitionKeyExtractor( region.getRegionDef()
                                                                                                                .getPartitionFieldNames() );
                rebalanceNonFusedStatelessOperator( region.getRegionId(),
                                                    e,
                                                    newPartitionDistribution,
                                                    currentReplicaCount,
                                                    regionExecPlan.getPipelineId( pipelineIndex ),
                                                    operatorDefs[ 0 ],
                                                    queues );
            }
        }
    }

    private void rebalancePartitionedStatefulOperators ( final int regionId,
                                                         final PartitionDistribution currentPartitionDistribution,
                                                         final PartitionDistribution newPartitionDistribution,
                                                         final OperatorDef[] operatorDefs )
    {
        for ( OperatorDef operatorDef : operatorDefs )
        {
            if ( operatorDef.getOperatorType() == PARTITIONED_STATEFUL )
            {
                final OperatorQueue[] queues = operatorQueueManager.getPartitionedQueues( regionId, operatorDef.getId() );
                if ( queues != null )
                {
                    operatorQueueManager.rebalancePartitionedQueues( regionId,
                                                                     operatorDef,
                                                                     currentPartitionDistribution,
                                                                     newPartitionDistribution );
                }
                else
                {
                    LOGGER.info( "Not rebalancing operator queues of fused operator: " + operatorDef.getId() );
                }

                operatorKvStoreManager.rebalancePartitionedKVStores( regionId,
                                                                     operatorDef.getId(),
                                                                     currentPartitionDistribution,
                                                                     newPartitionDistribution );
            }
            else
            {
                checkState( operatorDef.getOperatorType() == STATELESS );
            }
        }
    }

    private void rebalanceNonFusedStatelessOperator ( final int regionId,
                                                      final PartitionKeyExtractor partitionKeyExtractor,
                                                      final PartitionDistribution newPartitionDistribution,
                                                      final int currentReplicaCount,
                                                      final PipelineId pipelineId,
                                                      final OperatorDef operatorDef,
                                                      final OperatorQueue[] queues )
    {
        final int newReplicaCount = newPartitionDistribution.getReplicaCount();
        final List<List<Tuple>> buffer = IntStream.range( 0, newReplicaCount ).mapToObj( i -> new ArrayList<Tuple>() ).collect( toList() );

        for ( OperatorQueue queue : queues )
        {
            final TuplesImpl result = new TuplesImpl( 1 );
            final TupleQueueDrainer drainer = newGreedyDrainer( queue.getOperatorId(),
                                                                1,
                                                                config.getTupleQueueManagerConfig().getTupleQueueCapacity() );
            queue.drain( drainer, key -> result );

            result.getTuplesByDefaultPort().forEach( tuple -> {
                final int partitionId = getPartitionId( partitionKeyExtractor.getHash( tuple ),
                                                        config.getPartitionServiceConfig().getPartitionCount() );
                final int replicaIndex = newPartitionDistribution.getReplicaIndex( partitionId );
                buffer.get( replicaIndex ).add( tuple );
            } );
        }

        LOGGER.debug( "Rebalancing regionId={} {} operator: {} to {} replicas", regionId, STATELESS, operatorDef.getId(), newReplicaCount );

        if ( newReplicaCount > currentReplicaCount )
        {
            for ( int replicaIndex = currentReplicaCount; replicaIndex < newReplicaCount; replicaIndex++ )
            {
                LOGGER.debug( "Creating {} {} for regionId={} replicaIndex={} operatorId={}",
                              MULTI_THREADED,
                              DefaultOperatorQueue.class.getSimpleName(),
                              regionId,
                              replicaIndex,
                              operatorDef.getId() );
                operatorQueueManager.createDefaultQueue( regionId, operatorDef, replicaIndex, MULTI_THREADED );
            }
        }
        else
        {
            for ( int replicaIndex = newReplicaCount; replicaIndex < currentReplicaCount; replicaIndex++ )
            {
                LOGGER.debug( "Releasing operator queue of Pipeline {} Replica {} Operator {}",
                              pipelineId,
                              replicaIndex,
                              operatorDef.getId() );
                operatorQueueManager.releaseDefaultQueue( regionId, operatorDef.getId(), replicaIndex );
            }
        }

        final OperatorQueue[] newQueues = IntStream.range( 0, newReplicaCount )
                                                   .mapToObj( replicaIndex -> operatorQueueManager.getDefaultQueueOrFail( regionId,
                                                                                                                          operatorDef.getId(),
                                                                                                                          replicaIndex ) )
                                                   .toArray( OperatorQueue[]::new );

        int capacity = config.getTupleQueueManagerConfig().getTupleQueueCapacity();
        for ( final List tuples : buffer )
        {
            capacity = max( capacity, tuples.size() );
        }

        if ( capacity != config.getTupleQueueManagerConfig().getTupleQueueCapacity() )
        {
            LOGGER.debug( "Growing tuple queues of regionId={} operator {} to {}", regionId, operatorDef.getId(), capacity );
        }

        for ( int replicaIndex = 0; replicaIndex < newReplicaCount; replicaIndex++ )
        {
            final OperatorQueue operatorQueue = newQueues[ replicaIndex ];
            operatorQueue.ensureCapacity( capacity );
            operatorQueue.offer( 0, buffer.get( replicaIndex ) );
        }
    }

    private Region expandRegionReplicas ( final FlowDef flow, final Region region, final int newReplicaCount )
    {
        final int regionId = region.getRegionId();
        final RegionExecPlan currentRegionExecPlan = region.getExecPlan();
        final PipelineReplica[][] newPipelineReplicas = new PipelineReplica[ currentRegionExecPlan.getPipelineCount() ][ newReplicaCount ];

        for ( int pipelineIndex = 0; pipelineIndex < currentRegionExecPlan.getPipelineCount(); pipelineIndex++ )
        {
            arraycopy( region.getPipelineReplicas( pipelineIndex ),
                       0,
                       newPipelineReplicas[ pipelineIndex ],
                       0,
                       currentRegionExecPlan.getReplicaCount() );

            LOGGER.debug( "{} replicas of pipelineIndex={} of regionId={} are copied.",
                          currentRegionExecPlan.getReplicaCount(),
                          pipelineIndex,
                          regionId );

            final int pipelineId = currentRegionExecPlan.getPipelineStartIndex( pipelineIndex );
            final OperatorDef[] operatorDefs = currentRegionExecPlan.getOperatorDefsByPipelineIndex( pipelineIndex );
            final int pipelineOperatorCount = operatorDefs.length;
            final SchedulingStrategy[] pipelineSchedulingStrategies = new SchedulingStrategy[ pipelineOperatorCount ];
            arraycopy( region.getSchedulingStrategies(), pipelineId, pipelineSchedulingStrategies, 0, pipelineOperatorCount );
            final int[] fusionStartIndices = findFusionStartIndices( pipelineSchedulingStrategies );
            final int pipelineOperatorReplicaCount = fusionStartIndices.length;
            final int forwardedKeySize = currentRegionExecPlan.getRegionDef().getForwardedKeySize();

            for ( int replicaIndex = currentRegionExecPlan.getReplicaCount(); replicaIndex < newReplicaCount; replicaIndex++ )
            {
                final PipelineReplicaId pipelineReplicaId = new PipelineReplicaId( regionId, pipelineId, replicaIndex );

                LOGGER.debug( "Initializing pipeline replica {} with {} operators and {} fused operators",
                              pipelineReplicaId,
                              pipelineOperatorCount,
                              pipelineOperatorReplicaCount );

                final OperatorReplica[] operatorReplicas = new OperatorReplica[ pipelineOperatorReplicaCount ];
                final PipelineReplicaMeter meter = new PipelineReplicaMeter( config.getMetricManagerConfig().getTickMask(),
                                                                             pipelineReplicaId,
                                                                             operatorDefs[ 0 ] );

                for ( int i = 0; i < pipelineOperatorReplicaCount; i++ )
                {
                    final int fromOperatorIndex = fusionStartIndices[ i ];
                    final OperatorDef firstOperatorDef = operatorDefs[ fromOperatorIndex ];
                    final String firstOperatorId = firstOperatorDef.getId();

                    final OperatorQueue operatorQueue;
                    if ( flow.isSourceOperator( firstOperatorId ) )
                    {
                        LOGGER.debug( "Creating {} for regionId={} replicaIndex={} operatorId={}", EmptyOperatorQueue.class.getSimpleName(),
                                      regionId,
                                      replicaIndex,
                                      firstOperatorId );
                        operatorQueue = new EmptyOperatorQueue( firstOperatorId, firstOperatorDef.getInputPortCount() );
                    }
                    else if ( firstOperatorDef.getOperatorType() == PARTITIONED_STATEFUL )
                    {
                        final OperatorQueue[] operatorQueues = operatorQueueManager.getPartitionedQueuesOrFail( regionId,
                                                                                                                firstOperatorDef.getId() );
                        operatorQueue = operatorQueues[ replicaIndex ];
                    }
                    else
                    {
                        operatorQueue = operatorQueueManager.getDefaultQueueOrFail( regionId, firstOperatorDef.getId(), replicaIndex );
                    }

                    final TupleQueueDrainerPool drainerPool = createTupleQueueDrainerPool( firstOperatorDef, ( i == 0 ) );
                    LOGGER.debug( "Creating {} for regionId={} replicaIndex={} operatorId={}",
                                  drainerPool.getClass().getSimpleName(),
                                  regionId,
                                  replicaIndex,
                                  firstOperatorId );

                    final int toOperatorIndex = ( i < fusionStartIndices.length - 1 ) ? fusionStartIndices[ i + 1 ] : pipelineOperatorCount;
                    final int fusedOperatorCount = toOperatorIndex - fromOperatorIndex;

                    final OperatorDef[] fusedOperatorDefs = new OperatorDef[ fusedOperatorCount ];
                    final InternalInvocationCtx[] fusedInvocationCtxes = new InternalInvocationCtx[ fusedOperatorCount ];

                    for ( int j = fusedOperatorCount - 1; j >= 0; j-- )
                    {
                        final OperatorDef operatorDef = operatorDefs[ fromOperatorIndex + j ];
                        final String operatorId = operatorDef.getId();

                        final OperatorKVStore kvStore;
                        if ( operatorDef.getOperatorType() == STATELESS )
                        {
                            LOGGER.debug( "Creating {} for regionId={} replicaIndex={} operatorId={}",
                                          EmptyOperatorKVStore.class.getSimpleName(),
                                          regionId,
                                          replicaIndex,
                                          operatorId );
                            kvStore = new EmptyOperatorKVStore( operatorId );
                        }
                        else
                        {
                            checkState( operatorDef.getOperatorType() == PARTITIONED_STATEFUL );
                            LOGGER.debug( "Creating {} for regionId={} replicaIndex={} operatorId={}",
                                          PartitionedOperatorKVStore.class.getSimpleName(),
                                          regionId,
                                          replicaIndex,
                                          operatorId );
                            kvStore = operatorKvStoreManager.getPartitionedKVStore( regionId, operatorId, replicaIndex );
                        }

                        fusedOperatorDefs[ j ] = operatorDef;
                        final Function<PartitionKey, KVStore> fusedKVStoreSupplier = kvStore::getKVStore;

                        final OutputCollector outputCollector;
                        if ( j == fusedOperatorCount - 1 )
                        {
                            outputCollector = new DefaultOutputCollector( new TuplesImpl( operatorDef.getOutputPortCount() ) );
                        }
                        else
                        {
                            outputCollector = (OutputCollector) fusedInvocationCtxes[ j + 1 ];
                        }

                        final InternalInvocationCtx invocationCtx;
                        if ( j == 0 )
                        {
                            invocationCtx = new DefaultInvocationCtx( operatorDef.getInputPortCount(),
                                                                      fusedKVStoreSupplier,
                                                                      outputCollector );
                        }
                        else if ( operatorDef.getOperatorType() == PARTITIONED_STATEFUL )
                        {
                            final List<String> partitionFieldNames = operatorDef.getPartitionFieldNames();
                            final PartitionKeyExtractor ext = partitionKeyExtractorFactory.createPartitionKeyExtractor( partitionFieldNames,
                                                                                                                        forwardedKeySize );
                            invocationCtx = new FusedPartitionedInvocationCtx( operatorDef.getInputPortCount(),
                                                                               fusedKVStoreSupplier,
                                                                               ext,
                                                                               outputCollector );
                        }
                        else
                        {
                            invocationCtx = new FusedInvocationCtx( operatorDef.getInputPortCount(),
                                                                    fusedKVStoreSupplier,
                                                                    outputCollector );
                        }

                        fusedInvocationCtxes[ j ] = invocationCtx;
                    }

                    final DefaultInvocationCtx ctx = (DefaultInvocationCtx) fusedInvocationCtxes[ 0 ];
                    operatorReplicas[ i ] = new OperatorReplica( config,
                                                                 pipelineReplicaId,
                                                                 operatorQueue,
                                                                 drainerPool,
                                                                 meter,
                                                                 ctx::createInputTuples,
                                                                 fusedOperatorDefs,
                                                                 fusedInvocationCtxes );

                }

                final OperatorQueue pipelineQueue = createPipelineQueue( flow, regionId, replicaIndex, operatorReplicas );

                newPipelineReplicas[ pipelineIndex ][ replicaIndex ] = new PipelineReplica( config, pipelineReplicaId,
                                                                                            operatorReplicas,
                                                                                            pipelineQueue,
                                                                                            meter );
            }
        }

        final RegionExecPlan newRegionExecPlan = new RegionExecPlan( currentRegionExecPlan.getRegionDef(),
                                                                     currentRegionExecPlan.getPipelineStartIndices(),
                                                                     newReplicaCount );

        LOGGER.info( "regionId={} is expanded to {} replicas", regionId, newReplicaCount );

        return new Region( newRegionExecPlan, region.getSchedulingStrategies(), region.getUpstreamCtxes(), newPipelineReplicas );
    }

    private Region shrinkRegionReplicas ( final Region region, final int newReplicaCount )
    {
        final int regionId = region.getRegionId();
        final RegionExecPlan regionExecPlan = region.getExecPlan();
        final PipelineReplica[][] newPipelineReplicas = new PipelineReplica[ regionExecPlan.getPipelineCount() ][ newReplicaCount ];
        for ( int pipelineIndex = 0; pipelineIndex < regionExecPlan.getPipelineCount(); pipelineIndex++ )
        {
            final PipelineReplica[] currentPipelineReplicas = region.getPipelineReplicas( pipelineIndex );
            arraycopy( currentPipelineReplicas, 0, newPipelineReplicas[ pipelineIndex ], 0, newReplicaCount );

            for ( int replicaIndex = newReplicaCount; replicaIndex < regionExecPlan.getReplicaCount(); replicaIndex++ )
            {
                final PipelineReplica pipelineReplica = currentPipelineReplicas[ replicaIndex ];
                final OperatorReplica[] operatorReplicas = pipelineReplica.getOperators();
                final OperatorDef firstOperatorDef = operatorReplicas[ 0 ].getOperatorDef( 0 );
                if ( firstOperatorDef.getOperatorType() == PARTITIONED_STATEFUL )
                {
                    operatorQueueManager.releaseDefaultQueue( regionId, firstOperatorDef.getId(), replicaIndex );
                    LOGGER.debug( "Released pipeline queue of Pipeline {} Operator {}", pipelineReplica.id(), firstOperatorDef.getId() );
                }

                for ( OperatorReplica operatorReplica : operatorReplicas )
                {
                    try
                    {
                        operatorReplica.shutdown();
                    }
                    catch ( Exception e )
                    {
                        LOGGER.error( "Operator " + operatorReplica.getOperatorName() + " of regionId=" + regionId
                                      + " failed to shutdown while shrinking", e );
                    }
                }
            }
        }

        final RegionExecPlan newRegionExecPlan = new RegionExecPlan( regionExecPlan.getRegionDef(),
                                                                     regionExecPlan.getPipelineStartIndices(),
                                                                     newReplicaCount );
        LOGGER.info( "regionId={} is shrank to {} replicas", regionId, newReplicaCount );

        return new Region( newRegionExecPlan, region.getSchedulingStrategies(), region.getUpstreamCtxes(), newPipelineReplicas );
    }

    @Override
    public void releaseRegion ( final int regionId )
    {
        final Region region = regions.remove( regionId );
        checkArgument( region != null, "Region %s not found to release", regionId );

        final RegionExecPlan regionExecPlan = region.getExecPlan();
        final int replicaCount = regionExecPlan.getReplicaCount();
        final int pipelineCount = regionExecPlan.getPipelineCount();

        for ( int pipelineIndex = 0; pipelineIndex < pipelineCount; pipelineIndex++ )
        {
            final PipelineReplica[] pipelineReplicas = region.getPipelineReplicas( pipelineIndex );
            for ( int replicaIndex = 0; replicaIndex < replicaCount; replicaIndex++ )
            {
                final PipelineReplica pipelineReplica = pipelineReplicas[ replicaIndex ];

                final OperatorQueue pipelineQueue = pipelineReplica.getQueue();
                if ( pipelineQueue instanceof DefaultOperatorQueue )
                {
                    LOGGER.debug( "Releasing default operator queue of pipeline {}", pipelineReplica.id() );
                    final OperatorDef[] operatorDefs = regionExecPlan.getOperatorDefsByPipelineIndex( pipelineIndex );
                    final String operatorId = operatorDefs[ 0 ].getId();
                    operatorQueueManager.releaseDefaultQueue( regionId, operatorId, replicaIndex );
                }

                for ( int i = 0; i < pipelineReplica.getOperatorReplicaCount(); i++ )
                {
                    final OperatorReplica operator = pipelineReplica.getOperatorReplica( i );
                    for ( int j = 0; j < operator.getOperatorCount(); j++ )
                    {
                        final OperatorDef operatorDef = operator.getOperatorDef( j );
                        if ( j == 0 )
                        {
                            final OperatorQueue queue = operator.getQueue();
                            if ( queue instanceof DefaultOperatorQueue )
                            {
                                LOGGER.debug( "Releasing default operator queue of Operator {} in Pipeline {} replicaIndex {}",
                                              operatorDef.getId(),
                                              pipelineReplica.id(),
                                              replicaIndex );
                                operatorQueueManager.releaseDefaultQueue( regionId, operatorDef.getId(), replicaIndex );
                            }
                            else if ( queue instanceof PartitionedOperatorQueue && replicaIndex == 0 )
                            {
                                LOGGER.debug( "Releasing partitioned queue of Operator {} in Pipeline {}",
                                              operatorDef.getId(),
                                              pipelineReplica.id() );
                                operatorQueueManager.releasePartitionedQueues( regionId, operatorDef.getId() );
                            }
                        }

                        if ( replicaIndex == 0 )
                        {
                            if ( operatorDef.getOperatorType() == STATEFUL )
                            {
                                LOGGER.debug( "Releasing default operator kvStore of Operator {} in Pipeline {}",
                                              operatorDef.getId(),
                                              pipelineReplica.id() );
                                operatorKvStoreManager.releaseDefaultKVStore( regionId, operatorDef.getId() );
                            }
                            else if ( operatorDef.getOperatorType() == PARTITIONED_STATEFUL )
                            {
                                LOGGER.debug( "Releasing partitioned operator kvStore of Operator {} in Pipeline {}",
                                              operatorDef.getId(),
                                              pipelineReplica.id() );
                                operatorKvStoreManager.releasePartitionedKVStores( regionId, operatorDef.getId() );
                            }
                        }
                    }
                }
            }
        }
    }

    private void checkPipelineStartIndices ( final int regionId, final int operatorCount, final List<Integer> pipelineStartIndices )
    {
        int j = -1;
        for ( int i : pipelineStartIndices )
        {
            checkArgument( i > j, "invalid pipeline indices: %s for region %s", pipelineStartIndices, regionId );
            j = i;
            checkArgument( i < operatorCount, "invalid pipeline indices: %s for region %s", pipelineStartIndices, regionId );
        }
    }

    private PipelineReplicaId[] createPipelineReplicaIds ( final int regionId, final int replicaCount, final int pipelineId )
    {
        final PipelineReplicaId[] pipelineReplicaIds = new PipelineReplicaId[ replicaCount ];
        for ( int replicaIndex = 0; replicaIndex < replicaCount; replicaIndex++ )
        {
            pipelineReplicaIds[ replicaIndex ] = new PipelineReplicaId( regionId, pipelineId, replicaIndex );
        }
        return pipelineReplicaIds;
    }

    private OperatorQueue[] createOperatorQueues ( final FlowDef flow,
                                                   final int regionId,
                                                   final int replicaCount,
                                                   final boolean isFirstOperator,
                                                   final OperatorDef operatorDef,
                                                   final int forwardedKeySize )
    {
        final String operatorId = operatorDef.getId();
        final OperatorQueue[] operatorQueues;

        if ( flow.isSourceOperator( operatorId ) )
        {
            LOGGER.debug( "Creating {} for regionId={} operatorId={}", EmptyOperatorQueue.class.getSimpleName(), regionId, operatorId );
            operatorQueues = new OperatorQueue[ replicaCount ];
            for ( int replicaIndex = 0; replicaIndex < replicaCount; replicaIndex++ )
            {
                operatorQueues[ replicaIndex ] = new EmptyOperatorQueue( operatorId, operatorDef.getInputPortCount() );
            }
        }
        else if ( operatorDef.getOperatorType() == PARTITIONED_STATEFUL )
        {
            LOGGER.debug( "Creating {} for regionId={} operatorId={}", PartitionedOperatorQueue.class.getSimpleName(),
                          regionId,
                          operatorId );
            final PartitionDistribution partitionDistribution = partitionService.getPartitionDistributionOrFail( regionId );
            operatorQueues = operatorQueueManager.createPartitionedQueues( regionId, operatorDef, partitionDistribution, forwardedKeySize );
        }
        else
        {
            final ThreadingPref threadingPref = getThreadingPref( isFirstOperator );
            LOGGER.debug( "Creating {} {} for regionId={} operatorId={}", threadingPref, DefaultOperatorQueue.class.getSimpleName(),
                          regionId,
                          operatorId );
            operatorQueues = new OperatorQueue[ replicaCount ];
            for ( int replicaIndex = 0; replicaIndex < replicaCount; replicaIndex++ )
            {
                operatorQueues[ replicaIndex ] = operatorQueueManager.createDefaultQueue( regionId,
                                                                                          operatorDef,
                                                                                          replicaIndex,
                                                                                          threadingPref );
            }
        }

        return operatorQueues;
    }

    private ThreadingPref getThreadingPref ( final boolean isFirstOperator )
    {
        return isFirstOperator ? MULTI_THREADED : SINGLE_THREADED;
    }

    private TupleQueueDrainerPool[] createTupleQueueDrainerPools ( final int regionId,
                                                                   final int replicaCount,
                                                                   final boolean isFirstOperator,
                                                                   final OperatorDef operatorDef )
    {
        final String operatorId = operatorDef.getId();

        if ( isFirstOperator )
        {
            LOGGER.debug( "Creating {} for regionId={} operatorId={}",
                          BlockingTupleQueueDrainerPool.class.getSimpleName(),
                          regionId,
                          operatorId );
        }
        else
        {
            LOGGER.debug( "Creating {} for regionId={} operatorId={}",
                          NonBlockingTupleQueueDrainerPool.class.getSimpleName(),
                          regionId,
                          operatorId );
        }

        final TupleQueueDrainerPool[] drainerPools = new TupleQueueDrainerPool[ replicaCount ];
        for ( int replicaIndex = 0; replicaIndex < replicaCount; replicaIndex++ )
        {
            drainerPools[ replicaIndex ] = createTupleQueueDrainerPool( operatorDef, isFirstOperator );
        }

        return drainerPools;
    }

    private TupleQueueDrainerPool createTupleQueueDrainerPool ( final OperatorDef operatorDef, final boolean isFirstOperator )
    {
        return ( isFirstOperator && operatorDef.getInputPortCount() > 0 && operatorDef.getOperatorType() != PARTITIONED_STATEFUL )
               ? new BlockingTupleQueueDrainerPool( config, operatorDef )
               : new NonBlockingTupleQueueDrainerPool( config, operatorDef );
    }

    private OperatorKVStore[] createOperatorKVStores ( final int regionId, final int replicaCount, final OperatorDef operatorDef )
    {
        final String operatorId = operatorDef.getId();
        final OperatorKVStore[] operatorKvStores;
        if ( operatorDef.getOperatorType() == STATELESS )
        {
            LOGGER.debug( "Creating {} for regionId={} operatorId={}", EmptyOperatorKVStore.class.getSimpleName(), regionId, operatorId );
            operatorKvStores = new OperatorKVStore[ replicaCount ];
            for ( int replicaIndex = 0; replicaIndex < replicaCount; replicaIndex++ )
            {

                operatorKvStores[ replicaIndex ] = new EmptyOperatorKVStore( operatorId );
            }
        }
        else if ( operatorDef.getOperatorType() == PARTITIONED_STATEFUL )
        {
            LOGGER.debug( "Creating {} for regionId={} operatorId={}", PartitionedOperatorKVStore.class.getSimpleName(),
                          regionId,
                          operatorId );
            final PartitionDistribution partitionDistribution = partitionService.getPartitionDistributionOrFail( regionId );
            operatorKvStores = operatorKvStoreManager.createPartitionedKVStores( regionId, operatorId, partitionDistribution );
        }
        else
        {
            LOGGER.debug( "Creating {} for regionId={} operatorId={}", DefaultOperatorKVStore.class.getSimpleName(), regionId, operatorId );
            checkArgument( replicaCount == 1, "invalid replica count for operator %s in region %s", operatorDef.getId(), regionId );
            operatorKvStores = new OperatorKVStore[ 1 ];
            operatorKvStores[ 0 ] = operatorKvStoreManager.createDefaultKVStore( regionId, operatorId );
        }
        return operatorKvStores;
    }

    private OperatorQueue createPipelineQueue ( final FlowDef flow,
                                                final int regionId,
                                                final int replicaIndex,
                                                final OperatorReplica[] pipelineOperatorReplicas )
    {
        final OperatorDef firstOperatorDef = pipelineOperatorReplicas[ 0 ].getOperatorDef( 0 );
        final String operatorId = firstOperatorDef.getId();
        if ( flow.isSourceOperator( operatorId ) )
        {
            LOGGER.debug( "Creating {} for pipeline queue of regionId={} as pipeline has no input port",
                          EmptyOperatorQueue.class.getSimpleName(),
                          regionId );
            return new EmptyOperatorQueue( operatorId, firstOperatorDef.getInputPortCount() );
        }
        else
        {
            if ( firstOperatorDef.getOperatorType() == PARTITIONED_STATEFUL )
            {
                LOGGER.debug( "Creating {} for pipeline queue of regionId={} for pipeline operator={}",
                              DefaultOperatorQueue.class.getSimpleName(),
                              regionId,
                              operatorId );
                return operatorQueueManager.createDefaultQueue( regionId, firstOperatorDef, replicaIndex, MULTI_THREADED );
            }
            else
            {
                LOGGER.debug( "Creating {} for pipeline queue of regionId={} as first operator is {}",
                              EmptyOperatorQueue.class.getSimpleName(),
                              regionId,
                              firstOperatorDef.getOperatorType() );
                return new EmptyOperatorQueue( operatorId, firstOperatorDef.getInputPortCount() );
            }
        }
    }

    private void preInitializeRegionOperators ( final FlowDef flow,
                                                final RegionDef regionDef,
                                                final SchedulingStrategy[] schedulingStrategies,
                                                final UpstreamCtx[] upstreamCtxes )
    {
        final Operator[] operators = new Operator[ regionDef.getOperatorCount() ];
        try
        {
            for ( int i = 0; i < regionDef.getOperatorCount(); i++ )
            {
                final OperatorDef operatorDef = regionDef.getOperator( i );
                final UpstreamCtx upstreamCtx = createInitialUpstreamCtx( flow, operatorDef.getId() );
                final boolean[] upstreamConnectionStatuses = upstreamCtx.getConnectionStatuses();
                final InitCtxImpl initCtx = new InitCtxImpl( operatorDef, upstreamConnectionStatuses );
                try
                {
                    final Operator operator = operatorDef.createOperator();
                    operators[ i ] = operator;
                    final SchedulingStrategy schedulingStrategy = operator.init( initCtx );
                    verifyInitialSchedulingStrategyTupleCounts( operatorDef.getId(), schedulingStrategy );
                    upstreamCtx.verifyInitializable( operatorDef, schedulingStrategy );
                    schedulingStrategies[ i ] = schedulingStrategy;
                    upstreamCtxes[ i ] = upstreamCtx;
                }
                catch ( Exception e )
                {
                    throw new InitializationException( "Operator " + operatorDef.getId() + " initialization failed!", e );
                }
            }
        }
        finally
        {
            for ( Operator operator : operators )
            {
                try
                {
                    if ( operator != null )
                    {
                        operator.shutdown();
                    }
                }
                catch ( Exception e )
                {
                    checkInterruption( e );
                }
            }
        }
    }

    private void verifyInitialSchedulingStrategyTupleCounts ( final String operatorId, final SchedulingStrategy schedulingStrategy )
    {
        if ( schedulingStrategy instanceof ScheduleWhenTuplesAvailable )
        {
            final ScheduleWhenTuplesAvailable s = (ScheduleWhenTuplesAvailable) schedulingStrategy;
            for ( int tupleCount : s.getTupleCounts() )
            {
                checkState( tupleCount <= config.getTupleQueueManagerConfig().getTupleQueueCapacity(),
                            "Tuple queues are too small for scheduling strategy: %s of Operator=%s",
                            operatorId,
                            schedulingStrategy );
            }
        }
    }

}
