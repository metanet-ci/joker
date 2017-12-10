package cs.bilkent.joker.engine.region.impl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import javax.annotation.concurrent.NotThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.inject.Inject;
import com.google.inject.Singleton;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import cs.bilkent.joker.engine.config.JokerConfig;
import cs.bilkent.joker.engine.config.ThreadingPreference;
import static cs.bilkent.joker.engine.config.ThreadingPreference.MULTI_THREADED;
import static cs.bilkent.joker.engine.config.ThreadingPreference.SINGLE_THREADED;
import cs.bilkent.joker.engine.exception.InitializationException;
import cs.bilkent.joker.engine.flow.PipelineId;
import cs.bilkent.joker.engine.flow.RegionDef;
import cs.bilkent.joker.engine.flow.RegionExecutionPlan;
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
import cs.bilkent.joker.engine.pipeline.UpstreamContext;
import static cs.bilkent.joker.engine.pipeline.UpstreamContext.createInitialUpstreamContext;
import cs.bilkent.joker.engine.pipeline.impl.tuplesupplier.CachedTuplesImplSupplier;
import cs.bilkent.joker.engine.region.PipelineTransformer;
import cs.bilkent.joker.engine.region.Region;
import cs.bilkent.joker.engine.region.RegionManager;
import static cs.bilkent.joker.engine.region.impl.RegionExecutionPlanUtil.getMergeablePipelineStartIndices;
import static cs.bilkent.joker.engine.region.impl.RegionExecutionPlanUtil.getPipelineStartIndicesToSplit;
import cs.bilkent.joker.engine.tuplequeue.OperatorTupleQueue;
import cs.bilkent.joker.engine.tuplequeue.OperatorTupleQueueManager;
import cs.bilkent.joker.engine.tuplequeue.TupleQueueDrainerPool;
import cs.bilkent.joker.engine.tuplequeue.impl.drainer.GreedyDrainer;
import cs.bilkent.joker.engine.tuplequeue.impl.drainer.pool.BlockingTupleQueueDrainerPool;
import cs.bilkent.joker.engine.tuplequeue.impl.drainer.pool.NonBlockingTupleQueueDrainerPool;
import cs.bilkent.joker.engine.tuplequeue.impl.operator.DefaultOperatorTupleQueue;
import cs.bilkent.joker.engine.tuplequeue.impl.operator.EmptyOperatorTupleQueue;
import cs.bilkent.joker.engine.tuplequeue.impl.operator.PartitionedOperatorTupleQueue;
import static cs.bilkent.joker.engine.util.ExceptionUtils.checkInterruption;
import cs.bilkent.joker.flow.FlowDef;
import cs.bilkent.joker.operator.Operator;
import cs.bilkent.joker.operator.OperatorDef;
import cs.bilkent.joker.operator.Tuple;
import cs.bilkent.joker.operator.impl.InitializationContextImpl;
import cs.bilkent.joker.operator.impl.TuplesImpl;
import cs.bilkent.joker.operator.scheduling.ScheduleWhenTuplesAvailable;
import cs.bilkent.joker.operator.scheduling.SchedulingStrategy;
import static cs.bilkent.joker.operator.spec.OperatorType.PARTITIONED_STATEFUL;
import static cs.bilkent.joker.operator.spec.OperatorType.STATEFUL;
import static cs.bilkent.joker.operator.spec.OperatorType.STATELESS;
import static java.lang.Math.max;
import static java.lang.System.arraycopy;

@Singleton
@NotThreadSafe
public class RegionManagerImpl implements RegionManager
{

    private static final Logger LOGGER = LoggerFactory.getLogger( RegionManagerImpl.class );


    private final JokerConfig config;

    private final PartitionService partitionService;

    private final OperatorKVStoreManager operatorKvStoreManager;

    private final OperatorTupleQueueManager operatorTupleQueueManager;

    private final PipelineTransformer pipelineTransformer;

    private final PartitionKeyExtractorFactory partitionKeyExtractorFactory;

    private final Map<Integer, Region> regions = new HashMap<>();

    @Inject
    public RegionManagerImpl ( final JokerConfig config,
                               final PartitionService partitionService,
                               final OperatorKVStoreManager operatorKvStoreManager,
                               final OperatorTupleQueueManager operatorTupleQueueManager,
                               final PipelineTransformer pipelineTransformer,
                               final PartitionKeyExtractorFactory partitionKeyExtractorFactory )
    {
        this.config = config;
        this.partitionService = partitionService;
        this.operatorKvStoreManager = operatorKvStoreManager;
        this.operatorTupleQueueManager = operatorTupleQueueManager;
        this.pipelineTransformer = pipelineTransformer;
        this.partitionKeyExtractorFactory = partitionKeyExtractorFactory;
    }

    @Override
    public Region createRegion ( final FlowDef flow, final RegionExecutionPlan regionExecutionPlan )
    {
        checkArgument( flow != null, "flow is null" );
        checkState( !regions.containsKey( regionExecutionPlan.getRegionId() ),
                    "Region %s is already created!",
                    regionExecutionPlan.getRegionId() );

        final int regionId = regionExecutionPlan.getRegionId();
        final int replicaCount = regionExecutionPlan.getReplicaCount();
        final int pipelineCount = regionExecutionPlan.getPipelineStartIndices().size();

        final RegionDef regionDef = regionExecutionPlan.getRegionDef();
        final int regionOperatorCount = regionDef.getOperatorCount();

        checkPipelineStartIndices( regionId, regionOperatorCount, regionExecutionPlan.getPipelineStartIndices() );

        LOGGER.debug( "Creating components for regionId={} pipelineCount={} replicaCount={}", regionId, pipelineCount, replicaCount );

        if ( regionDef.getRegionType() == PARTITIONED_STATEFUL )
        {
            partitionService.createPartitionDistribution( regionId, replicaCount );
            LOGGER.debug( "Created partition distribution for regionId={} with {} replicas", regionId, replicaCount );
        }

        final PipelineReplica[][] pipelineReplicas = new PipelineReplica[ pipelineCount ][ replicaCount ];
        final SchedulingStrategy[] schedulingStrategies = new SchedulingStrategy[ regionOperatorCount ];
        final UpstreamContext[] upstreamContexts = new UpstreamContext[ regionOperatorCount ];
        populate( flow, regionDef, schedulingStrategies, upstreamContexts );

        for ( int pipelineIndex = 0; pipelineIndex < pipelineCount; pipelineIndex++ )
        {
            final int pipelineId = regionExecutionPlan.getPipelineStartIndex( pipelineIndex );
            final OperatorDef[] operatorDefs = regionExecutionPlan.getOperatorDefsByPipelineIndex( pipelineIndex );
            final int pipelineOperatorCount = operatorDefs.length;
            final OperatorReplica[][] operatorReplicas = new OperatorReplica[ replicaCount ][ pipelineOperatorCount ];
            LOGGER.debug( "Initializing pipeline instance for regionId={} pipelineIndex={} with {} operators",
                          regionId,
                          pipelineIndex, pipelineOperatorCount );

            final PipelineReplicaId[] pipelineReplicaIds = createPipelineReplicaIds( regionId, replicaCount, pipelineId );
            final int forwardKeyLimit = regionDef.getPartitionFieldNames().size();

            final PipelineReplicaMeter[] replicaMeters = new PipelineReplicaMeter[ replicaCount ];
            for ( int replicaIndex = 0; replicaIndex < replicaCount; replicaIndex++ )
            {
                replicaMeters[ replicaIndex ] = new PipelineReplicaMeter( config.getMetricManagerConfig().getTickMask(),
                                                                          pipelineReplicaIds[ replicaIndex ],
                                                                          operatorDefs[ 0 ] );
            }

            for ( int operatorIndex = 0; operatorIndex < pipelineOperatorCount; operatorIndex++ )
            {
                final OperatorDef operatorDef = operatorDefs[ operatorIndex ];

                final boolean isFirstOperator = operatorIndex == 0;
                final OperatorTupleQueue[] operatorTupleQueues = createOperatorTupleQueues( flow,
                                                                                            regionId,
                                                                                            replicaCount,
                                                                                            isFirstOperator,
                                                                                            operatorDef,
                                                                                            forwardKeyLimit );

                final TupleQueueDrainerPool[] drainerPools = createTupleQueueDrainerPools( regionId,
                                                                                           replicaCount,
                                                                                           isFirstOperator,
                                                                                           operatorDef );

                final OperatorKVStore[] operatorKvStores = createOperatorKVStores( regionId, replicaCount, operatorDef );

                final Supplier<TuplesImpl>[] outputSuppliers = createOutputSuppliers( regionId, replicaCount, operatorDef );

                for ( int replicaIndex = 0; replicaIndex < replicaCount; replicaIndex++ )
                {
                    operatorReplicas[ replicaIndex ][ operatorIndex ] = new OperatorReplica( pipelineReplicaIds[ replicaIndex ],
                                                                                             operatorDef,
                                                                                             operatorTupleQueues[ replicaIndex ],
                                                                                             operatorKvStores[ replicaIndex ],
                                                                                             drainerPools[ replicaIndex ],
                                                                                             outputSuppliers[ replicaIndex ],
                                                                                             replicaMeters[ replicaIndex ] );
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
                final OperatorTupleQueue pipelineTupleQueue = createPipelineTupleQueue( flow,
                                                                                        regionId,
                                                                                        replicaIndex,
                                                                                        pipelineOperatorReplicas );

                pipelineReplicas[ pipelineIndex ][ replicaIndex ] = new PipelineReplica( config,
                                                                                         pipelineReplicaIds[ replicaIndex ],
                                                                                         pipelineOperatorReplicas,
                                                                                         pipelineTupleQueue,
                                                                                         replicaMeters[ replicaIndex ] );
            }
        }

        final Region region = new Region( regionExecutionPlan, schedulingStrategies, upstreamContexts, pipelineReplicas );
        regions.put( regionId, region );
        return region;
    }

    @Override
    public void validatePipelineMergeParameters ( final List<PipelineId> pipelineIds )
    {
        final Region region = regions.get( pipelineIds.get( 0 ).getRegionId() );
        checkArgument( region != null, "no region found for %s", pipelineIds );
        getMergeablePipelineStartIndices( region.getExecutionPlan(), pipelineIds );
    }

    @Override
    public Region mergePipelines ( final List<PipelineId> pipelineIdsToMerge )
    {
        final Region region = regions.get( pipelineIdsToMerge.get( 0 ).getRegionId() );
        final int regionId = region.getRegionId();

        final List<Integer> startIndicesToMerge = getMergeablePipelineStartIndices( region.getExecutionPlan(), pipelineIdsToMerge );

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
        getPipelineStartIndicesToSplit( region.getExecutionPlan(), pipelineId, pipelineOperatorIndicesToSplit );
    }

    @Override
    public Region splitPipeline ( final PipelineId pipelineId, final List<Integer> pipelineOperatorIndicesToSplit )
    {
        validatePipelineSplitParameters( pipelineId, pipelineOperatorIndicesToSplit );
        final int regionId = pipelineId.getRegionId();
        final Region region = regions.remove( regionId );
        final List<Integer> pipelineStartIndicesToSplit = getPipelineStartIndicesToSplit( region.getExecutionPlan(),
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

        final RegionExecutionPlan regionExecutionPlan = region.getExecutionPlan();
        final RegionDef regionDef = regionExecutionPlan.getRegionDef();
        checkState( regionDef.getRegionType() == PARTITIONED_STATEFUL,
                    "cannot rebalance %s regionId=%s",
                    regionDef.getRegionType(),
                    regionId );

        if ( newReplicaCount == regionExecutionPlan.getReplicaCount() )
        {
            regions.put( regionId, region );
            LOGGER.warn( "No rebalance since regionId={} already has the same replica count={}", regionId, newReplicaCount );
            return region;
        }

        LOGGER.info( "Rebalancing regionId={} to new replica count: {} from current replica count: {}",
                     regionId,
                     newReplicaCount,
                     regionExecutionPlan.getReplicaCount() );

        drainPipelineTupleQueues( region );

        rebalanceRegion( region, newReplicaCount );

        final Region newRegion;
        if ( regionExecutionPlan.getReplicaCount() < newReplicaCount )
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

    private void drainPipelineTupleQueues ( final Region region )
    {
        final RegionExecutionPlan execPlan = region.getExecutionPlan();
        for ( int pipelineIndex = 0; pipelineIndex < execPlan.getPipelineCount(); pipelineIndex++ )
        {
            for ( PipelineReplica pipelineReplica : region.getPipelineReplicas( pipelineIndex ) )
            {
                final OperatorTupleQueue pipelineTupleQueue = pipelineReplica.getSelfPipelineTupleQueue();
                final OperatorReplica operator = pipelineReplica.getOperator( 0 );
                final OperatorTupleQueue operatorTupleQueue = operator.getQueue();
                final OperatorDef operatorDef = operator.getOperatorDef();
                final TuplesImpl result = new TuplesImpl( operatorDef.getInputPortCount() );
                final GreedyDrainer drainer = new GreedyDrainer( operatorDef.getInputPortCount() );
                pipelineTupleQueue.drain( drainer, k -> result );
                if ( result.isNonEmpty() )
                {
                    LOGGER.debug( "Draining pipeline tuple queue of {}", pipelineReplica.id() );
                    for ( int portIndex = 0; portIndex < result.getPortCount(); portIndex++ )
                    {
                        final List<Tuple> tuples = result.getTuplesModifiable( portIndex );
                        if ( tuples.size() > 0 )
                        {
                            final int offered = operatorTupleQueue.offer( portIndex, tuples );
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

        final RegionExecutionPlan regionExecutionPlan = region.getExecutionPlan();

        final int currentReplicaCount = currentPartitionDistribution.getReplicaCount();

        for ( int pipelineIndex = 0; pipelineIndex < regionExecutionPlan.getPipelineCount(); pipelineIndex++ )
        {
            final PipelineReplica[] pipelineReplicas = region.getPipelineReplicas( pipelineIndex );
            final OperatorDef[] operatorDefs = regionExecutionPlan.getOperatorDefsByPipelineIndex( pipelineIndex );
            rebalancePartitionedStatefulOperators( regionId, currentPartitionDistribution, newPartitionDistribution, operatorDefs );
            rebalanceStatelessOperators( region, newPartitionDistribution, currentReplicaCount, pipelineReplicas, operatorDefs );
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
                operatorTupleQueueManager.rebalancePartitionedOperatorTupleQueues( regionId,
                                                                                   operatorDef,
                                                                                   currentPartitionDistribution,
                                                                                   newPartitionDistribution );
                operatorKvStoreManager.rebalancePartitionedOperatorKVStores( regionId, operatorDef.getId(),
                                                                             currentPartitionDistribution,
                                                                             newPartitionDistribution );
            }
            else
            {
                checkState( operatorDef.getOperatorType() == STATELESS );
            }
        }
    }

    private void rebalanceStatelessOperators ( final Region region,
                                               final PartitionDistribution newPartitionDistribution,
                                               final int currentReplicaCount,
                                               final PipelineReplica[] pipelineReplicas,
                                               final OperatorDef[] operatorDefs )
    {
        final int regionId = region.getRegionId();
        final PartitionKeyExtractor partitionKeyExtractor = partitionKeyExtractorFactory.createPartitionKeyExtractor( region.getRegionDef()
                                                                                                                            .getPartitionFieldNames() );
        final int newReplicaCount = newPartitionDistribution.getReplicaCount();
        for ( int operatorIndex = 0; operatorIndex < operatorDefs.length; operatorIndex++ )
        {
            final OperatorDef operatorDef = operatorDefs[ operatorIndex ];
            if ( operatorDef.getOperatorType() == STATELESS )
            {
                final int inputPortCount = operatorDef.getInputPortCount();
                final TuplesImpl[] buffer = drainOperatorTupleQueuesIntoBuffers( pipelineReplicas,
                                                                                 partitionKeyExtractor,
                                                                                 newPartitionDistribution,
                                                                                 operatorIndex,
                                                                                 inputPortCount,
                                                                                 newReplicaCount );

                LOGGER.debug( "Rebalancing regionId={} {} operator: {} to {} replicas",
                              regionId,
                              STATELESS,
                              operatorDef.getId(),
                              newReplicaCount );

                if ( newReplicaCount > currentReplicaCount )
                {
                    for ( int replicaIndex = currentReplicaCount; replicaIndex < newReplicaCount; replicaIndex++ )
                    {
                        final boolean isFirstOperator = ( operatorIndex == 0 );
                        final ThreadingPreference threadingPreference = getThreadingPreference( isFirstOperator );
                        LOGGER.debug( "Creating {} {} for regionId={} replicaIndex={} operatorId={}",
                                      threadingPreference,
                                      DefaultOperatorTupleQueue.class.getSimpleName(),
                                      regionId,
                                      replicaIndex,
                                      operatorDef.getId() );
                        operatorTupleQueueManager.createDefaultOperatorTupleQueue( regionId,
                                                                                   replicaIndex,
                                                                                   operatorDef,
                                                                                   threadingPreference );
                    }
                }
                else
                {
                    for ( int replicaIndex = newReplicaCount; replicaIndex < currentReplicaCount; replicaIndex++ )
                    {
                        LOGGER.debug( "Releasing operator tuple queue of Pipeline {} Operator {}",
                                      pipelineReplicas[ replicaIndex ].id(),
                                      operatorDef.getId() );
                        operatorTupleQueueManager.releaseDefaultOperatorTupleQueue( regionId, replicaIndex, operatorDef.getId() );
                    }
                }

                offerBuffersToOperatorTupleQueues( regionId, operatorDef, buffer );
            }
            else
            {
                checkState( operatorDef.getOperatorType() == PARTITIONED_STATEFUL );
            }
        }
    }

    private TuplesImpl[] drainOperatorTupleQueuesIntoBuffers ( final PipelineReplica[] pipelineReplicas,
                                                               final PartitionKeyExtractor partitionKeyExtractor,
                                                               final PartitionDistribution partitionDistribution,
                                                               final int operatorIndex,
                                                               final int inputPortCount,
                                                               final int newReplicaCount )
    {
        final TuplesImpl[] buffer = new TuplesImpl[ newReplicaCount ];
        for ( int replicaIndex = 0; replicaIndex < newReplicaCount; replicaIndex++ )
        {
            buffer[ replicaIndex ] = new TuplesImpl( inputPortCount );
        }

        for ( PipelineReplica pipelineReplica : pipelineReplicas )
        {
            final OperatorReplica operator = pipelineReplica.getOperator( operatorIndex );
            final TuplesImpl result = new TuplesImpl( inputPortCount );
            final GreedyDrainer drainer = new GreedyDrainer( inputPortCount );
            operator.getQueue().drain( drainer, key -> result );
            for ( int portIndex = 0; portIndex < inputPortCount; portIndex++ )
            {
                for ( Tuple tuple : result.getTuples( portIndex ) )
                {
                    final int partitionId = getPartitionId( partitionKeyExtractor.getPartitionHash( tuple ),
                                                            config.getPartitionServiceConfig().getPartitionCount() );
                    final int replicaIndex = partitionDistribution.getReplicaIndex( partitionId );
                    buffer[ replicaIndex ].add( portIndex, tuple );
                }
            }
        }

        return buffer;
    }

    private void offerBuffersToOperatorTupleQueues ( final int regionId, final OperatorDef operatorDef, final TuplesImpl[] buffer )
    {
        final int replicaCount = buffer.length;
        final OperatorTupleQueue[] operatorTupleQueues = new OperatorTupleQueue[ replicaCount ];
        for ( int replicaIndex = 0; replicaIndex < replicaCount; replicaIndex++ )
        {
            operatorTupleQueues[ replicaIndex ] = operatorTupleQueueManager.getDefaultOperatorTupleQueueOrFail( regionId,
                                                                                                                replicaIndex,
                                                                                                                operatorDef.getId() );
        }

        int capacity = config.getTupleQueueManagerConfig().getTupleQueueCapacity();
        for ( final TuplesImpl tuples : buffer )
        {
            for ( int portIndex = 0; portIndex < operatorDef.getInputPortCount(); portIndex++ )
            {
                capacity = max( capacity, tuples.getTupleCount( portIndex ) );
            }
        }

        if ( capacity != config.getTupleQueueManagerConfig().getTupleQueueCapacity() )
        {
            LOGGER.debug( "Growing tuple queues of regionId={} operator {} to {}", regionId, operatorDef.getId(), capacity );
        }

        for ( int replicaIndex = 0; replicaIndex < replicaCount; replicaIndex++ )
        {
            final OperatorTupleQueue operatorTupleQueue = operatorTupleQueues[ replicaIndex ];
            operatorTupleQueue.ensureCapacity( capacity );
            final TuplesImpl tuples = buffer[ replicaIndex ];
            for ( int portIndex = 0; portIndex < operatorDef.getInputPortCount(); portIndex++ )
            {
                operatorTupleQueue.offer( portIndex, tuples.getTuples( portIndex ) );
            }
        }
    }

    private Region expandRegionReplicas ( final FlowDef flow, final Region region, final int newReplicaCount )
    {
        final int regionId = region.getRegionId();
        final RegionExecutionPlan regionExecutionPlan = region.getExecutionPlan();
        final int currentReplicaCount = regionExecutionPlan.getReplicaCount();
        final PipelineReplica[][] newPipelineReplicas = new PipelineReplica[ regionExecutionPlan.getPipelineCount() ][ newReplicaCount ];
        for ( int pipelineIndex = 0; pipelineIndex < regionExecutionPlan.getPipelineCount(); pipelineIndex++ )
        {
            final PipelineReplica[] currentPipelineReplicas = region.getPipelineReplicas( pipelineIndex );
            arraycopy( currentPipelineReplicas, 0, newPipelineReplicas[ pipelineIndex ], 0, currentReplicaCount );

            LOGGER.debug( "{} replicas of pipelineIndex={} of regionId={} are copied.", currentReplicaCount, pipelineIndex, regionId );

            final int pipelineId = regionExecutionPlan.getPipelineStartIndex( pipelineIndex );
            final OperatorDef[] operatorDefs = regionExecutionPlan.getOperatorDefsByPipelineIndex( pipelineIndex );
            final int operatorCount = operatorDefs.length;

            for ( int replicaIndex = currentReplicaCount; replicaIndex < newReplicaCount; replicaIndex++ )
            {
                LOGGER.debug( "Initializing pipeline instance for regionId={} pipelineIndex={} replicaIndex={} with {} operators",
                              regionId,
                              pipelineIndex,
                              replicaIndex,
                              operatorCount );

                final OperatorReplica[] operatorReplicas = new OperatorReplica[ operatorCount ];
                final PipelineReplicaId pipelineReplicaId = new PipelineReplicaId( new PipelineId( regionId, pipelineId ), replicaIndex );
                final PipelineReplicaMeter replicaMeter = new PipelineReplicaMeter( config.getMetricManagerConfig().getTickMask(),
                                                                                    pipelineReplicaId,
                                                                                    operatorDefs[ 0 ] );
                for ( int operatorIndex = 0; operatorIndex < operatorCount; operatorIndex++ )
                {
                    final OperatorDef operatorDef = operatorDefs[ operatorIndex ];
                    final String operatorId = operatorDef.getId();
                    final boolean isFirstOperator = ( operatorIndex == 0 );

                    final OperatorTupleQueue operatorTupleQueue;
                    if ( flow.getInboundConnections( operatorId ).isEmpty() )
                    {
                        LOGGER.debug( "Creating {} for regionId={} replicaIndex={} operatorId={}",
                                      EmptyOperatorTupleQueue.class.getSimpleName(),
                                      regionId,
                                      replicaIndex,
                                      operatorId );
                        operatorTupleQueue = new EmptyOperatorTupleQueue( operatorId, operatorDef.getInputPortCount() );
                    }
                    else if ( operatorDef.getOperatorType() == PARTITIONED_STATEFUL )
                    {
                        final OperatorTupleQueue[] operatorTupleQueues = operatorTupleQueueManager.getPartitionedOperatorTupleQueuesOrFail(
                                regionId,
                                operatorDef.getId() );
                        operatorTupleQueue = operatorTupleQueues[ replicaIndex ];
                    }
                    else
                    {
                        operatorTupleQueue = operatorTupleQueueManager.getDefaultOperatorTupleQueueOrFail( regionId,
                                                                                                           replicaIndex,
                                                                                                           operatorDef.getId() );
                    }

                    final TupleQueueDrainerPool drainerPool = createTupleQueueDrainerPool( operatorDef, isFirstOperator );
                    LOGGER.debug( "Creating {} for regionId={} replicaIndex={} operatorId={}",
                                  drainerPool.getClass().getSimpleName(),
                                  regionId,
                                  replicaIndex,
                                  operatorId );

                    final OperatorKVStore operatorKVStore;
                    if ( operatorDef.getOperatorType() == STATELESS )
                    {
                        LOGGER.debug( "Creating {} for regionId={} replicaIndex={} operatorId={}",
                                      EmptyOperatorKVStore.class.getSimpleName(),
                                      regionId,
                                      replicaIndex,
                                      operatorId );
                        operatorKVStore = new EmptyOperatorKVStore( operatorId );
                    }
                    else
                    {
                        checkState( operatorDef.getOperatorType() == PARTITIONED_STATEFUL );
                        LOGGER.debug( "Creating {} for regionId={} replicaIndex={} operatorId={}",
                                      PartitionedOperatorKVStore.class.getSimpleName(),
                                      regionId,
                                      replicaIndex,
                                      operatorId );
                        final OperatorKVStore[] operatorKvStores = operatorKvStoreManager.getPartitionedOperatorKVStoresOrFail( regionId,
                                                                                                                                operatorId );
                        operatorKVStore = operatorKvStores[ replicaIndex ];
                    }

                    final Supplier<TuplesImpl> outputSupplier = new CachedTuplesImplSupplier( operatorDef.getOutputPortCount() );
                    LOGGER.debug( "Creating {} for regionId={} replicaIndex={} operatorId={}",
                                  outputSupplier.getClass().getSimpleName(),
                                  regionId,
                                  replicaIndex,
                                  operatorId );

                    operatorReplicas[ operatorIndex ] = new OperatorReplica( pipelineReplicaId,
                                                                             operatorDef,
                                                                             operatorTupleQueue,
                                                                             operatorKVStore,
                                                                             drainerPool,
                                                                             outputSupplier,
                                                                             replicaMeter );
                }

                final OperatorTupleQueue pipelineTupleQueue = createPipelineTupleQueue( flow, regionId, replicaIndex, operatorReplicas );

                newPipelineReplicas[ pipelineIndex ][ replicaIndex ] = new PipelineReplica( config,
                                                                                            pipelineReplicaId,
                                                                                            operatorReplicas,
                                                                                            pipelineTupleQueue,
                                                                                            replicaMeter );
            }
        }

        final RegionExecutionPlan newRegionExecutionPlan = new RegionExecutionPlan( regionExecutionPlan.getRegionDef(),
                                                                                    regionExecutionPlan.getPipelineStartIndices(),
                                                                                    newReplicaCount );

        LOGGER.info( "regionId={} is expanded to {} replicas", regionId, newReplicaCount );

        return new Region( newRegionExecutionPlan,
                           region.getOperatorSchedulingStrategies(),
                           region.getOperatorUpstreamContexts(),
                           newPipelineReplicas );
    }

    private Region shrinkRegionReplicas ( final Region region, final int newReplicaCount )
    {
        final int regionId = region.getRegionId();
        final RegionExecutionPlan regionExecutionPlan = region.getExecutionPlan();
        final PipelineReplica[][] newPipelineReplicas = new PipelineReplica[ regionExecutionPlan.getPipelineCount() ][ newReplicaCount ];
        for ( int pipelineIndex = 0; pipelineIndex < regionExecutionPlan.getPipelineCount(); pipelineIndex++ )
        {
            final PipelineReplica[] currentPipelineReplicas = region.getPipelineReplicas( pipelineIndex );
            arraycopy( currentPipelineReplicas, 0, newPipelineReplicas[ pipelineIndex ], 0, newReplicaCount );

            for ( int replicaIndex = newReplicaCount; replicaIndex < regionExecutionPlan.getReplicaCount(); replicaIndex++ )
            {
                final PipelineReplica pipelineReplica = currentPipelineReplicas[ replicaIndex ];
                final OperatorReplica[] operatorReplicas = pipelineReplica.getOperators();
                final OperatorDef firstOperatorDef = operatorReplicas[ 0 ].getOperatorDef();
                if ( firstOperatorDef.getOperatorType() == PARTITIONED_STATEFUL )
                {
                    operatorTupleQueueManager.releaseDefaultOperatorTupleQueue( regionId, replicaIndex, firstOperatorDef.getId() );
                    LOGGER.debug( "Released pipeline tuple queue of Pipeline {} Operator {}",
                                  pipelineReplica.id(),
                                  firstOperatorDef.getId() );
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

        final RegionExecutionPlan newRegionExecutionPlan = new RegionExecutionPlan( regionExecutionPlan.getRegionDef(),
                                                                                    regionExecutionPlan.getPipelineStartIndices(),
                                                                                    newReplicaCount );
        LOGGER.info( "regionId={} is shrank to {} replicas", regionId, newReplicaCount );

        return new Region( newRegionExecutionPlan,
                           region.getOperatorSchedulingStrategies(),
                           region.getOperatorUpstreamContexts(),
                           newPipelineReplicas );
    }

    @Override
    public void releaseRegion ( final int regionId )
    {
        final Region region = regions.remove( regionId );
        checkArgument( region != null, "Region %s not found to release", regionId );

        final RegionExecutionPlan regionExecutionPlan = region.getExecutionPlan();
        final int replicaCount = regionExecutionPlan.getReplicaCount();
        final int pipelineCount = regionExecutionPlan.getPipelineStartIndices().size();

        for ( int pipelineIndex = 0; pipelineIndex < pipelineCount; pipelineIndex++ )
        {
            final PipelineReplica[] pipelineReplicas = region.getPipelineReplicas( pipelineIndex );
            for ( int replicaIndex = 0; replicaIndex < replicaCount; replicaIndex++ )
            {
                final PipelineReplica pipelineReplica = pipelineReplicas[ replicaIndex ];

                final OperatorTupleQueue selfPipelineTupleQueue = pipelineReplica.getSelfPipelineTupleQueue();
                if ( selfPipelineTupleQueue instanceof DefaultOperatorTupleQueue )
                {
                    LOGGER.debug( "Releasing default tuple queue of pipeline {}", pipelineReplica.id() );
                    final OperatorDef[] operatorDefs = regionExecutionPlan.getOperatorDefsByPipelineIndex( pipelineIndex );
                    final String operatorId = operatorDefs[ 0 ].getId();
                    operatorTupleQueueManager.releaseDefaultOperatorTupleQueue( regionId, replicaIndex, operatorId );
                }

                for ( int i = 0; i < pipelineReplica.getOperatorCount(); i++ )
                {
                    final OperatorReplica operator = pipelineReplica.getOperator( i );
                    final OperatorDef operatorDef = operator.getOperatorDef();
                    final OperatorTupleQueue queue = operator.getQueue();
                    if ( queue instanceof DefaultOperatorTupleQueue )
                    {
                        LOGGER.debug( "Releasing default tuple queue of Operator {} in Pipeline {} replicaIndex {}", operatorDef.getId(),
                                      pipelineReplica.id(),
                                      replicaIndex );
                        operatorTupleQueueManager.releaseDefaultOperatorTupleQueue( regionId, replicaIndex, operatorDef.getId() );
                    }
                    else if ( queue instanceof PartitionedOperatorTupleQueue && replicaIndex == 0 )
                    {
                        LOGGER.debug( "Releasing partitioned tuple queue of Operator {} in Pipeline {}", operatorDef.getId(),
                                      pipelineReplica.id() );
                        operatorTupleQueueManager.releasePartitionedOperatorTupleQueues( regionId, operatorDef.getId() );
                    }

                    if ( replicaIndex == 0 )
                    {
                        if ( operatorDef.getOperatorType() == STATEFUL )
                        {
                            LOGGER.debug( "Releasing default operator kvStore of Operator {} in Pipeline {}", operatorDef.getId(),
                                          pipelineReplica.id() );
                            operatorKvStoreManager.releaseDefaultOperatorKVStore( regionId, operatorDef.getId() );
                        }
                        else if ( operatorDef.getOperatorType() == PARTITIONED_STATEFUL )
                        {
                            LOGGER.debug( "Releasing partitioned operator kvStore of Operator {} in Pipeline {}", operatorDef.getId(),
                                          pipelineReplica.id() );
                            operatorKvStoreManager.releasePartitionedOperatorKVStores( regionId, operatorDef.getId() );
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
            pipelineReplicaIds[ replicaIndex ] = new PipelineReplicaId( new PipelineId( regionId, pipelineId ), replicaIndex );
        }
        return pipelineReplicaIds;
    }

    private OperatorTupleQueue[] createOperatorTupleQueues ( final FlowDef flow,
                                                             final int regionId,
                                                             final int replicaCount,
                                                             final boolean isFirstOperator,
                                                             final OperatorDef operatorDef,
                                                             final int forwardKeyLimit )
    {
        final String operatorId = operatorDef.getId();
        final OperatorTupleQueue[] operatorTupleQueues;

        if ( flow.getInboundConnections( operatorId ).isEmpty() )
        {
            LOGGER.debug( "Creating {} for regionId={} operatorId={}",
                          EmptyOperatorTupleQueue.class.getSimpleName(),
                          regionId,
                          operatorId );
            operatorTupleQueues = new OperatorTupleQueue[ replicaCount ];
            for ( int replicaIndex = 0; replicaIndex < replicaCount; replicaIndex++ )
            {
                operatorTupleQueues[ replicaIndex ] = new EmptyOperatorTupleQueue( operatorId, operatorDef.getInputPortCount() );
            }
        }
        else if ( operatorDef.getOperatorType() == PARTITIONED_STATEFUL )
        {
            LOGGER.debug( "Creating {} for regionId={} operatorId={}", PartitionedOperatorTupleQueue.class.getSimpleName(),
                          regionId,
                          operatorId );
            final PartitionDistribution partitionDistribution = partitionService.getPartitionDistributionOrFail( regionId );
            operatorTupleQueues = operatorTupleQueueManager.createPartitionedOperatorTupleQueues( regionId,
                                                                                                  operatorDef,
                                                                                                  partitionDistribution,
                                                                                                  forwardKeyLimit );
        }
        else
        {
            final ThreadingPreference threadingPreference = getThreadingPreference( isFirstOperator );
            LOGGER.debug( "Creating {} {} for regionId={} operatorId={}",
                          threadingPreference,
                          DefaultOperatorTupleQueue.class.getSimpleName(),
                          regionId,
                          operatorId );
            operatorTupleQueues = new OperatorTupleQueue[ replicaCount ];
            for ( int replicaIndex = 0; replicaIndex < replicaCount; replicaIndex++ )
            {
                operatorTupleQueues[ replicaIndex ] = operatorTupleQueueManager.createDefaultOperatorTupleQueue( regionId,
                                                                                                                 replicaIndex,
                                                                                                                 operatorDef,
                                                                                                                 threadingPreference );
            }
        }

        return operatorTupleQueues;
    }

    private ThreadingPreference getThreadingPreference ( final boolean isFirstOperator )
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
        return ( isFirstOperator && operatorDef.getInputPortCount() > 0 && ( operatorDef.getOperatorType() == STATEFUL
                                                                             || operatorDef.getOperatorType() == STATELESS ) )
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
            operatorKvStores = operatorKvStoreManager.createPartitionedOperatorKVStores( regionId, operatorId, partitionDistribution );
        }
        else
        {
            LOGGER.debug( "Creating {} for regionId={} operatorId={}", DefaultOperatorKVStore.class.getSimpleName(), regionId, operatorId );
            checkArgument( replicaCount == 1, "invalid replica count for operator %s in region %s", operatorDef.getId(), regionId );
            operatorKvStores = new OperatorKVStore[ 1 ];
            operatorKvStores[ 0 ] = operatorKvStoreManager.createDefaultOperatorKVStore( regionId, operatorId );
        }
        return operatorKvStores;
    }

    private Supplier<TuplesImpl>[] createOutputSuppliers ( final int regionId, final int replicaCount, final OperatorDef operatorDef )
    {
        final String operatorId = operatorDef.getId();
        LOGGER.debug( "Creating {} for regionId={} operatorId={}", CachedTuplesImplSupplier.class.getSimpleName(), regionId, operatorId );

        final Supplier<TuplesImpl>[] outputSuppliers = new Supplier[ replicaCount ];
        for ( int replicaIndex = 0; replicaIndex < replicaCount; replicaIndex++ )
        {
            outputSuppliers[ replicaIndex ] = new CachedTuplesImplSupplier( operatorDef.getOutputPortCount() );
        }

        return outputSuppliers;
    }

    private OperatorTupleQueue createPipelineTupleQueue ( final FlowDef flow,
                                                          final int regionId,
                                                          final int replicaIndex,
                                                          final OperatorReplica[] pipelineOperatorReplicas )
    {
        final OperatorDef firstOperatorDef = pipelineOperatorReplicas[ 0 ].getOperatorDef();
        if ( flow.getInboundConnections( firstOperatorDef.getId() ).isEmpty() )
        {
            LOGGER.debug( "Creating {} for pipeline tuple queue of regionId={} as pipeline has no input port",
                          EmptyOperatorTupleQueue.class.getSimpleName(),
                          regionId );
            return new EmptyOperatorTupleQueue( firstOperatorDef.getId(), firstOperatorDef.getInputPortCount() );
        }
        else
        {
            if ( firstOperatorDef.getOperatorType() == PARTITIONED_STATEFUL )
            {
                LOGGER.debug( "Creating {} for pipeline tuple queue of regionId={} for pipeline operator={}",
                              DefaultOperatorTupleQueue.class.getSimpleName(),
                              regionId,
                              firstOperatorDef.getId() );
                return operatorTupleQueueManager.createDefaultOperatorTupleQueue( regionId,
                                                                                  replicaIndex,
                                                                                  firstOperatorDef,
                                                                                  MULTI_THREADED );
            }
            else
            {
                LOGGER.debug( "Creating {} for pipeline tuple queue of regionId={} as first operator is {}",
                              EmptyOperatorTupleQueue.class.getSimpleName(),
                              regionId,
                              firstOperatorDef.getOperatorType() );
                return new EmptyOperatorTupleQueue( firstOperatorDef.getId(), firstOperatorDef.getInputPortCount() );
            }
        }
    }

    private void populate ( final FlowDef flow,
                            final RegionDef regionDef,
                            final SchedulingStrategy[] schedulingStrategies,
                            final UpstreamContext[] upstreamContexts )
    {
        final Operator[] operators = new Operator[ regionDef.getOperatorCount() ];
        try
        {
            for ( int i = 0; i < regionDef.getOperatorCount(); i++ )
            {
                final OperatorDef operatorDef = regionDef.getOperator( i );
                final UpstreamContext upstreamContext = createInitialUpstreamContext( flow, operatorDef.getId() );
                final InitializationContextImpl initializationContext = new InitializationContextImpl( operatorDef,
                                                                                                       upstreamContext
                                                                                                               .getUpstreamConnectionStatuses() );
                try
                {
                    final Operator operator = operatorDef.createOperator();
                    operators[ i ] = operator;
                    final SchedulingStrategy schedulingStrategy = operator.init( initializationContext );
                    verifyInitialSchedulingStrategyTupleCounts( operatorDef.getId(), schedulingStrategy );
                    upstreamContext.verifyInitializable( operatorDef, schedulingStrategy );
                    schedulingStrategies[ i ] = schedulingStrategy;
                    upstreamContexts[ i ] = upstreamContext;
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
