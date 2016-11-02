package cs.bilkent.joker.engine.region.impl;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
import cs.bilkent.joker.engine.kvstore.OperatorKVStore;
import cs.bilkent.joker.engine.kvstore.OperatorKVStoreManager;
import cs.bilkent.joker.engine.kvstore.impl.DefaultOperatorKVStore;
import cs.bilkent.joker.engine.kvstore.impl.EmptyOperatorKVStore;
import cs.bilkent.joker.engine.kvstore.impl.PartitionedOperatorKVStore;
import cs.bilkent.joker.engine.partition.PartitionDistribution;
import cs.bilkent.joker.engine.partition.PartitionService;
import cs.bilkent.joker.engine.pipeline.OperatorReplica;
import cs.bilkent.joker.engine.pipeline.PipelineId;
import cs.bilkent.joker.engine.pipeline.PipelineReplica;
import cs.bilkent.joker.engine.pipeline.PipelineReplicaId;
import cs.bilkent.joker.engine.pipeline.impl.tuplesupplier.CachedTuplesImplSupplier;
import cs.bilkent.joker.engine.pipeline.impl.tuplesupplier.NonCachedTuplesImplSupplier;
import cs.bilkent.joker.engine.region.PipelineTransformer;
import cs.bilkent.joker.engine.region.Region;
import cs.bilkent.joker.engine.region.RegionConfig;
import cs.bilkent.joker.engine.region.RegionDef;
import cs.bilkent.joker.engine.region.RegionManager;
import cs.bilkent.joker.engine.tuplequeue.OperatorTupleQueue;
import cs.bilkent.joker.engine.tuplequeue.OperatorTupleQueueManager;
import cs.bilkent.joker.engine.tuplequeue.TupleQueueDrainerPool;
import cs.bilkent.joker.engine.tuplequeue.impl.drainer.GreedyDrainer;
import cs.bilkent.joker.engine.tuplequeue.impl.drainer.pool.BlockingTupleQueueDrainerPool;
import cs.bilkent.joker.engine.tuplequeue.impl.drainer.pool.NonBlockingTupleQueueDrainerPool;
import cs.bilkent.joker.engine.tuplequeue.impl.operator.DefaultOperatorTupleQueue;
import cs.bilkent.joker.engine.tuplequeue.impl.operator.EmptyOperatorTupleQueue;
import cs.bilkent.joker.engine.tuplequeue.impl.operator.PartitionedOperatorTupleQueue;
import cs.bilkent.joker.flow.FlowDef;
import cs.bilkent.joker.operator.OperatorDef;
import cs.bilkent.joker.operator.impl.TuplesImpl;
import cs.bilkent.joker.operator.spec.OperatorType;
import static cs.bilkent.joker.operator.spec.OperatorType.PARTITIONED_STATEFUL;
import static cs.bilkent.joker.operator.spec.OperatorType.STATEFUL;
import static cs.bilkent.joker.operator.spec.OperatorType.STATELESS;
import static java.lang.Integer.compare;
import static java.lang.System.arraycopy;
import static java.util.Collections.sort;
import static java.util.stream.Collectors.toList;

@Singleton
@NotThreadSafe
public class RegionManagerImpl implements RegionManager
{

    private static final Logger LOGGER = LoggerFactory.getLogger( RegionManagerImpl.class );

    private static final Set<OperatorType> REBALANCEABLE_REGION_TYPES = EnumSet.of( PARTITIONED_STATEFUL, STATELESS );


    private final JokerConfig config;

    private final PartitionService partitionService;

    private final OperatorKVStoreManager operatorKvStoreManager;

    private final OperatorTupleQueueManager operatorTupleQueueManager;

    private final PipelineTransformer pipelineTransformer;

    private final Map<Integer, Region> regions = new HashMap<>();

    @Inject
    public RegionManagerImpl ( final JokerConfig config,
                               final PartitionService partitionService,
                               final OperatorKVStoreManager operatorKvStoreManager,
                               final OperatorTupleQueueManager operatorTupleQueueManager,
                               final PipelineTransformer pipelineTransformer )
    {
        this.config = config;
        this.partitionService = partitionService;
        this.operatorKvStoreManager = operatorKvStoreManager;
        this.operatorTupleQueueManager = operatorTupleQueueManager;
        this.pipelineTransformer = pipelineTransformer;
    }

    @Override
    public Region createRegion ( final FlowDef flow, final RegionConfig regionConfig )
    {
        checkArgument( flow != null, "flow is null" );
        checkState( !regions.containsKey( regionConfig.getRegionId() ), "Region %s is already created!", regionConfig.getRegionId() );

        final int regionId = regionConfig.getRegionId();
        final int replicaCount = regionConfig.getReplicaCount();
        final int pipelineCount = regionConfig.getPipelineStartIndices().size();

        checkPipelineStartIndices( regionId, regionConfig.getRegionDef().getOperatorCount(), regionConfig.getPipelineStartIndices() );

        LOGGER.info( "Creating components for regionId={} pipelineCount={} replicaCount={}", regionId, pipelineCount, replicaCount );

        if ( regionConfig.getRegionDef().getRegionType() == PARTITIONED_STATEFUL )
        {
            partitionService.createPartitionDistribution( regionId, replicaCount );
            LOGGER.info( "Created partition distribution for regionId={} with {} replicas", regionId, replicaCount );
        }

        final PipelineReplica[][] pipelineReplicas = new PipelineReplica[ pipelineCount ][ replicaCount ];

        for ( int pipelineIndex = 0; pipelineIndex < pipelineCount; pipelineIndex++ )
        {
            final int pipelineId = regionConfig.getPipelineStartIndex( pipelineIndex );
            final OperatorDef[] operatorDefs = regionConfig.getOperatorDefsByPipelineIndex( pipelineIndex );
            final int operatorCount = operatorDefs.length;
            final OperatorReplica[][] operatorReplicas = new OperatorReplica[ replicaCount ][ operatorCount ];
            LOGGER.info( "Initializing pipeline instance for regionId={} pipelineIndex={} with {} operators", regionId, pipelineIndex,
                         operatorCount );

            final PipelineReplicaId[] pipelineReplicaIds = createPipelineReplicaIds( regionId, replicaCount, pipelineId );
            final int forwardKeyLimit = regionConfig.getRegionDef().getPartitionFieldNames().size();

            for ( int operatorIndex = 0; operatorIndex < operatorCount; operatorIndex++ )
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

                final boolean isLastOperator = operatorIndex == ( operatorCount - 1 );
                final Supplier<TuplesImpl>[] outputSuppliers = createOutputSuppliers( regionId, replicaCount, isLastOperator, operatorDef );

                for ( int replicaIndex = 0; replicaIndex < replicaCount; replicaIndex++ )
                {
                    operatorReplicas[ replicaIndex ][ operatorIndex ] = new OperatorReplica( pipelineReplicaIds[ replicaIndex ],
                                                                                             operatorDef,
                                                                                             operatorTupleQueues[ replicaIndex ],
                                                                                             operatorKvStores[ replicaIndex ],
                                                                                             drainerPools[ replicaIndex ],
                                                                                             outputSuppliers[ replicaIndex ] );
                }
            }

            for ( int replicaIndex = 0; replicaIndex < replicaCount; replicaIndex++ )
            {
                LOGGER.info( "Creating pipeline instance for regionId={} replicaIndex={} pipelineIndex={} pipelineId={}",
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
                                                                                         pipelineTupleQueue );
            }
        }

        final Region region = new Region( regionConfig, pipelineReplicas );
        regions.put( regionId, region );
        return region;
    }

    @Override
    public void validatePipelineMergeParameters ( final List<PipelineId> pipelineIds )
    {
        getMergeablePipelineIds( pipelineIds );
    }

    private List<PipelineId> getMergeablePipelineIds ( final List<PipelineId> pipelineIds )
    {
        checkArgument( pipelineIds != null && pipelineIds.size() > 1 );

        final List<PipelineId> pipelineIdsSorted = new ArrayList<>( pipelineIds );
        sort( pipelineIdsSorted, ( o1, o2 ) ->
        {
            final int c = compare( o1.regionId, o2.regionId );
            return c != 0 ? c : compare( o1.pipelineId, o2.pipelineId );
        } );

        checkArgument( pipelineIdsSorted.get( 0 ).regionId == pipelineIdsSorted.get( pipelineIdsSorted.size() - 1 ).regionId,
                       "multiple region ids in %s",
                       pipelineIds );
        checkArgument( pipelineIdsSorted.stream().map( id -> id.pipelineId ).distinct().count() == pipelineIds.size(),
                       "duplicate pipeline ids in %s",
                       pipelineIds );

        final Region region = regions.get( pipelineIdsSorted.get( 0 ).regionId );
        checkArgument( region != null, "no region found for %s", pipelineIds );

        return pipelineIdsSorted;
    }

    @Override
    public Region mergePipelines ( final List<PipelineId> pipelineIdsToMerge )
    {
        final List<Integer> startIndicesToMerge = getMergeablePipelineStartIndices( pipelineIdsToMerge );
        final int regionId = pipelineIdsToMerge.get( 0 ).regionId;

        final Region region = regions.remove( regionId );

        final Region newRegion = pipelineTransformer.mergePipelines( region, startIndicesToMerge );
        regions.put( regionId, newRegion );
        return newRegion;
    }

    @Override
    public void validatePipelineSplitParameters ( final PipelineId pipelineId, final List<Integer> pipelineOperatorIndicesToSplit )
    {
        getPipelineStartIndicesToSplit( pipelineId, pipelineOperatorIndicesToSplit );
    }

    private List<Integer> getPipelineStartIndicesToSplit ( final PipelineId pipelineId, final List<Integer> pipelineOperatorIndicesToSplit )
    {
        checkArgument( pipelineId != null, "pipeline id to split cannot be null" );
        checkArgument( pipelineOperatorIndicesToSplit != null && pipelineOperatorIndicesToSplit.size() > 1,
                       "there must be at least 2 operator split indices for Pipeline %s",
                       pipelineId );
        final Region region = regions.get( pipelineId.regionId );
        checkArgument( region != null, "invalid Pipeline %s to split", pipelineId );

        int curr = 0;
        final int operatorCount = region.getConfig().getOperatorCountByPipelineId( pipelineId.pipelineId );
        for ( int i = 0; i < pipelineOperatorIndicesToSplit.size(); i++ )
        {
            final int p = pipelineOperatorIndicesToSplit.get( i );
            checkArgument( p > curr && p < operatorCount );
            curr = p;
        }

        final List<Integer> pipelineStartIndicesToSplit = new ArrayList<>();
        pipelineStartIndicesToSplit.add( pipelineId.pipelineId );
        for ( int i : pipelineOperatorIndicesToSplit )
        {
            pipelineStartIndicesToSplit.add( pipelineId.pipelineId + i );
        }

        return pipelineStartIndicesToSplit;
    }

    @Override
    public Region splitPipeline ( final PipelineId pipelineId, final List<Integer> pipelineOperatorIndicesToSplit )
    {
        final List<Integer> pipelineStartIndicesToSplit = getPipelineStartIndicesToSplit( pipelineId, pipelineOperatorIndicesToSplit );
        final int regionId = pipelineId.regionId;

        final Region region = regions.remove( regionId );
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

        final RegionConfig regionConfig = region.getConfig();
        final RegionDef regionDef = regionConfig.getRegionDef();
        checkState( REBALANCEABLE_REGION_TYPES.contains( regionDef.getRegionType() ),
                    "cannot rebalance %s regionId=%s",
                    regionDef.getRegionType(),
                    regionId );

        if ( newReplicaCount == regionConfig.getReplicaCount() )
        {
            regions.put( regionId, region );
            LOGGER.warn( "No rebalance since regionId=%s already has the same replica count=%s", regionId, newReplicaCount );
            return region;
        }

        checkStatelessOperatorTupleQueuesEmpty( region );
        drainPipelineTupleQueues( region );

        rebalanceRegion( region, newReplicaCount );

        final Region newRegion;
        if ( regionConfig.getReplicaCount() < newReplicaCount )
        {
            newRegion = extendRegionReplicas( flow, region, newReplicaCount );
        }
        else
        {
            newRegion = shrinkRegionReplicas( region, newReplicaCount );
        }
        regions.put( regionId, newRegion );

        return newRegion;
    }

    private void rebalanceRegion ( final Region region, final int newReplicaCount )
    {
        final int regionId = region.getRegionId();
        final RegionConfig regionConfig = region.getConfig();

        if ( regionConfig.getRegionDef().getRegionType() == STATELESS )
        {
            LOGGER.info( "Will not rebalance partition distribution since regionId={} is {}", regionId, STATELESS );
            return;
        }

        final PartitionDistribution currentPartitionDistribution = partitionService.getPartitionDistributionOrFail( regionId );
        final PartitionDistribution rebalancedPartitionDistribution = partitionService.rebalancePartitionDistribution( regionId,
                                                                                                                       newReplicaCount );

        for ( int pipelineIndex = 0; pipelineIndex < regionConfig.getPipelineCount(); pipelineIndex++ )
        {
            final PipelineReplica[] currentPipelineReplicas = region.getPipelineReplicas( pipelineIndex );
            for ( OperatorReplica operator : currentPipelineReplicas[ 0 ].getOperators() )
            {
                final OperatorDef operatorDef = operator.getOperatorDef();
                if ( operatorDef.operatorType() == PARTITIONED_STATEFUL )
                {
                    operatorTupleQueueManager.rebalancePartitionedOperatorTupleQueues( regionId,
                                                                                       operatorDef,
                                                                                       currentPartitionDistribution,
                                                                                       rebalancedPartitionDistribution );
                    operatorKvStoreManager.rebalancePartitionedOperatorKVStores( regionId,
                                                                                 operatorDef.id(),
                                                                                 currentPartitionDistribution,
                                                                                 rebalancedPartitionDistribution );
                }
            }
        }
    }

    private Region extendRegionReplicas ( final FlowDef flow, final Region region, final int newReplicaCount )
    {
        final int regionId = region.getRegionId();
        final RegionConfig regionConfig = region.getConfig();
        final int currentReplicaCount = regionConfig.getReplicaCount();
        final PipelineReplica[][] newPipelineReplicas = new PipelineReplica[ regionConfig.getPipelineCount() ][ newReplicaCount ];
        for ( int pipelineIndex = 0; pipelineIndex < regionConfig.getPipelineCount(); pipelineIndex++ )
        {
            final PipelineReplica[] currentPipelineReplicas = region.getPipelineReplicas( pipelineIndex );
            arraycopy( currentPipelineReplicas, 0, newPipelineReplicas[ pipelineIndex ], 0, currentReplicaCount );

            LOGGER.info( "{} replicas of pipelineIndex={} of regionId={} are copied.", currentReplicaCount, pipelineIndex, regionId );

            final int pipelineId = regionConfig.getPipelineStartIndex( pipelineIndex );
            final OperatorDef[] operatorDefs = regionConfig.getOperatorDefsByPipelineIndex( pipelineIndex );
            final int operatorCount = operatorDefs.length;

            for ( int replicaIndex = currentReplicaCount; replicaIndex < newReplicaCount; replicaIndex++ )
            {
                LOGGER.info( "Initializing pipeline instance for regionId={} pipelineIndex={} replicaIndex={} with {} operators",
                             regionId,
                             pipelineIndex,
                             replicaIndex,
                             operatorCount );

                final OperatorReplica[] operatorReplicas = new OperatorReplica[ operatorCount ];
                final PipelineReplicaId pipelineReplicaId = new PipelineReplicaId( new PipelineId( regionId, pipelineId ), replicaIndex );

                for ( int operatorIndex = 0; operatorIndex < operatorCount; operatorIndex++ )
                {
                    final OperatorDef operatorDef = operatorDefs[ operatorIndex ];
                    final String operatorId = operatorDef.id();
                    final boolean isFirstOperator = ( operatorIndex == 0 ), isLastOperator = ( operatorIndex == ( operatorCount - 1 ) );

                    final OperatorTupleQueue operatorTupleQueue;
                    if ( flow.getUpstreamConnections( operatorId ).isEmpty() )
                    {
                        LOGGER.info( "Creating {} for regionId={} operatorId={}",
                                     EmptyOperatorTupleQueue.class.getSimpleName(),
                                     regionId,
                                     operatorId );
                        operatorTupleQueue = new EmptyOperatorTupleQueue( operatorId, operatorDef.inputPortCount() );
                    }
                    else if ( operatorDef.operatorType() == PARTITIONED_STATEFUL )
                    {
                        LOGGER.info( "Creating {} for regionId={} operatorId={}",
                                     PartitionedOperatorTupleQueue.class.getSimpleName(),
                                     regionId,
                                     operatorId );
                        final OperatorTupleQueue[] operatorTupleQueues = operatorTupleQueueManager.getPartitionedOperatorTupleQueuesOrFail(
                                regionId,
                                operatorDef.id() );
                        operatorTupleQueue = operatorTupleQueues[ replicaIndex ];
                    }
                    else
                    {
                        final ThreadingPreference threadingPreference = getThreadingPreference( isFirstOperator );
                        LOGGER.info( "Creating {} {} for regionId={} operatorId={}",
                                     threadingPreference,
                                     DefaultOperatorTupleQueue.class.getSimpleName(),
                                     regionId,
                                     operatorId );
                        operatorTupleQueue = operatorTupleQueueManager.createDefaultOperatorTupleQueue( regionId,
                                                                                                        replicaIndex,
                                                                                                        operatorDef,
                                                                                                        threadingPreference );
                    }

                    final TupleQueueDrainerPool drainerPool = createTupleQueueDrainer( operatorDef, isFirstOperator );
                    LOGGER.info( "Creating {} for regionId={} operatorId={}",
                                 drainerPool.getClass().getSimpleName(),
                                 regionId,
                                 operatorId );

                    final OperatorKVStore operatorKVStore;
                    if ( operatorDef.operatorType() == STATELESS )
                    {
                        LOGGER.info( "Creating {} for regionId={} operatorId={}",
                                     EmptyOperatorKVStore.class.getSimpleName(),
                                     regionId,
                                     operatorId );
                        operatorKVStore = new EmptyOperatorKVStore( operatorId );
                    }
                    else
                    {
                        checkState( operatorDef.operatorType() == PARTITIONED_STATEFUL );
                        LOGGER.info( "Creating {} for regionId={} operatorId={}",
                                     PartitionedOperatorKVStore.class.getSimpleName(),
                                     regionId,
                                     operatorId );
                        final OperatorKVStore[] operatorKvStores = operatorKvStoreManager.getPartitionedOperatorKVStoresOrFail( regionId,
                                                                                                                                operatorId );
                        operatorKVStore = operatorKvStores[ replicaIndex ];
                    }

                    final Supplier<TuplesImpl> outputSupplier = createOutputSupplier( operatorDef, isLastOperator );
                    LOGGER.info( "Creating {} for regionId={} operatorId={}",
                                 outputSupplier.getClass().getSimpleName(),
                                 regionId,
                                 operatorId );

                    operatorReplicas[ operatorIndex ] = new OperatorReplica( pipelineReplicaId,
                                                                             operatorDef,
                                                                             operatorTupleQueue,
                                                                             operatorKVStore,
                                                                             drainerPool,
                                                                             outputSupplier );
                }

                final OperatorTupleQueue pipelineTupleQueue = createPipelineTupleQueue( flow, regionId, replicaIndex, operatorReplicas );

                newPipelineReplicas[ pipelineIndex ][ replicaIndex ] = new PipelineReplica( config,
                                                                                            pipelineReplicaId,
                                                                                            operatorReplicas,
                                                                                            pipelineTupleQueue );
            }
        }

        final RegionConfig newRegionConfig = new RegionConfig( regionConfig.getRegionDef(),
                                                               regionConfig.getPipelineStartIndices(),
                                                               newReplicaCount );

        LOGGER.info( "regionId={} is extended to {} replicas", regionId, newReplicaCount );

        return new Region( newRegionConfig, newPipelineReplicas );
    }

    private Region shrinkRegionReplicas ( final Region region, final int newReplicaCount )
    {
        final int regionId = region.getRegionId();
        final RegionConfig regionConfig = region.getConfig();
        final PipelineReplica[][] newPipelineReplicas = new PipelineReplica[ regionConfig.getPipelineCount() ][ newReplicaCount ];
        for ( int pipelineIndex = 0; pipelineIndex < regionConfig.getPipelineCount(); pipelineIndex++ )
        {
            final PipelineReplica[] currentPipelineReplicas = region.getPipelineReplicas( pipelineIndex );
            arraycopy( currentPipelineReplicas, 0, newPipelineReplicas[ pipelineIndex ], 0, newReplicaCount );

            for ( int replicaIndex = newReplicaCount; replicaIndex < regionConfig.getReplicaCount(); replicaIndex++ )
            {
                final PipelineReplica pipelineReplica = currentPipelineReplicas[ replicaIndex ];
                final OperatorReplica[] operators = pipelineReplica.getOperators();
                final OperatorDef firstOperatorDef = operators[ 0 ].getOperatorDef();
                if ( firstOperatorDef.operatorType() == PARTITIONED_STATEFUL )
                {
                    operatorTupleQueueManager.releaseDefaultOperatorTupleQueue( regionId, replicaIndex, firstOperatorDef.id() );
                    LOGGER.info( "Released pipeline tuple queue of Pipeline {} Operator {}", pipelineReplica.id(), firstOperatorDef.id() );
                }

                for ( OperatorReplica operator : operators )
                {
                    final OperatorDef operatorDef = operator.getOperatorDef();
                    if ( operatorDef.operatorType() == STATELESS )
                    {
                        operatorTupleQueueManager.releaseDefaultOperatorTupleQueue( regionId, replicaIndex, operatorDef.id() );
                        LOGGER.info( "Released operator tuple queue of Pipeline {} Operator {}", pipelineReplica.id(), operatorDef.id() );
                    }
                    else
                    {
                        checkState( operatorDef.operatorType() == PARTITIONED_STATEFUL,
                                    "Cannot shrink {} because of {} operator {}",
                                    pipelineReplica.id(),
                                    operatorDef.id(),
                                    operatorDef.operatorType() );
                    }
                }
            }
        }

        final RegionConfig newRegionConfig = new RegionConfig( regionConfig.getRegionDef(),
                                                               regionConfig.getPipelineStartIndices(),
                                                               newReplicaCount );
        LOGGER.info( "regionId={} is shrank to {} replicas", regionId, newReplicaCount );

        return new Region( newRegionConfig, newPipelineReplicas );
    }

    private void checkStatelessOperatorTupleQueuesEmpty ( final Region region )
    {
        final RegionConfig config = region.getConfig();
        for ( int pipelineIndex = 0; pipelineIndex < config.getPipelineCount(); pipelineIndex++ )
        {
            for ( PipelineReplica pipelineReplica : region.getPipelineReplicas( pipelineIndex ) )
            {
                for ( OperatorReplica operatorReplica : pipelineReplica.getOperators() )
                {
                    final OperatorDef operatorDef = operatorReplica.getOperatorDef();
                    if ( operatorDef.operatorType() == STATELESS )
                    {
                        final OperatorTupleQueue operatorTupleQueue = operatorReplica.getQueue();
                        final GreedyDrainer drainer = new GreedyDrainer( operatorDef.inputPortCount() );
                        operatorTupleQueue.drain( drainer );
                        final TuplesImpl result = drainer.getResult();
                        checkState( result == null || result.isEmpty(),
                                    "%s %s operator %s has non-empty queue!",
                                    pipelineReplica.id(),
                                    STATELESS,
                                    operatorDef.id() );
                    }
                }
            }
        }
    }

    private void drainPipelineTupleQueues ( final Region region )
    {
        final RegionConfig config = region.getConfig();
        for ( int pipelineIndex = 0; pipelineIndex < config.getPipelineCount(); pipelineIndex++ )
        {
            for ( PipelineReplica pipelineReplica : region.getPipelineReplicas( pipelineIndex ) )
            {
                final OperatorTupleQueue pipelineTupleQueue = pipelineReplica.getSelfPipelineTupleQueue();
                final OperatorReplica operator = pipelineReplica.getOperator( 0 );
                final OperatorTupleQueue operatorTupleQueue = operator.getQueue();
                final OperatorDef operatorDef = operator.getOperatorDef();
                final GreedyDrainer drainer = new GreedyDrainer( operatorDef.inputPortCount() );
                pipelineTupleQueue.drain( drainer );
                final TuplesImpl result = drainer.getResult();
                if ( result != null && result.isNonEmpty() )
                {
                    LOGGER.info( "Draining pipeline tuple queue of %s", pipelineReplica.id() );
                    for ( int portIndex = 0; portIndex < result.getPortCount(); portIndex++ )
                    {
                        operatorTupleQueue.offer( portIndex, result.getTuples( portIndex ) );
                    }
                }
            }
        }
    }

    private List<Integer> getMergeablePipelineStartIndices ( final List<PipelineId> pipelineIds )
    {
        final List<PipelineId> pipelineIdsSorted = getMergeablePipelineIds( pipelineIds );
        final Region region = regions.get( pipelineIds.get( 0 ).regionId );

        final List<Integer> startIndicesToMerge = pipelineIdsSorted.stream().map( p -> p.pipelineId ).collect( toList() );
        final RegionConfig regionConfig = region.getConfig();

        if ( !pipelineTransformer.checkPipelineStartIndicesToMerge( regionConfig, startIndicesToMerge ) )
        {
            throw new IllegalArgumentException( "invalid pipeline start indices to merge: " + startIndicesToMerge
                                                + " current pipeline start indices: " + regionConfig.getPipelineStartIndices()
                                                + " regionId=" + region.getRegionId() );
        }

        return startIndicesToMerge;
    }

    @Override
    public void releaseRegion ( final int regionId )
    {
        final Region region = regions.remove( regionId );
        checkArgument( region != null, "Region %s not found to release", regionId );

        final RegionConfig regionConfig = region.getConfig();
        final int replicaCount = regionConfig.getReplicaCount();
        final int pipelineCount = regionConfig.getPipelineStartIndices().size();

        for ( int pipelineIndex = 0; pipelineIndex < pipelineCount; pipelineIndex++ )
        {
            final PipelineReplica[] pipelineReplicas = region.getPipelineReplicas( pipelineIndex );
            for ( int replicaIndex = 0; replicaIndex < replicaCount; replicaIndex++ )
            {
                final PipelineReplica pipelineReplica = pipelineReplicas[ replicaIndex ];

                final OperatorTupleQueue selfPipelineTupleQueue = pipelineReplica.getSelfPipelineTupleQueue();
                if ( selfPipelineTupleQueue instanceof DefaultOperatorTupleQueue )
                {
                    LOGGER.info( "Releasing default tuple queue of pipeline {}", pipelineReplica.id() );
                    final OperatorDef[] operatorDefs = regionConfig.getOperatorDefsByPipelineIndex( pipelineIndex );
                    final String operatorId = operatorDefs[ 0 ].id();
                    operatorTupleQueueManager.releaseDefaultOperatorTupleQueue( regionId, replicaIndex, operatorId );
                }

                for ( int i = 0; i < pipelineReplica.getOperatorCount(); i++ )
                {
                    final OperatorReplica operator = pipelineReplica.getOperator( i );
                    final OperatorDef operatorDef = operator.getOperatorDef();
                    final OperatorTupleQueue queue = operator.getQueue();
                    if ( queue instanceof DefaultOperatorTupleQueue )
                    {
                        LOGGER.info( "Releasing default tuple queue of Operator {} in Pipeline {} replicaIndex {}",
                                     operatorDef.id(),
                                     pipelineReplica.id(),
                                     replicaIndex );
                        operatorTupleQueueManager.releaseDefaultOperatorTupleQueue( regionId, replicaIndex, operatorDef.id() );
                    }
                    else if ( queue instanceof PartitionedOperatorTupleQueue && replicaIndex == 0 )
                    {
                        LOGGER.info( "Releasing partitioned tuple queue of Operator {} in Pipeline {}",
                                     operatorDef.id(),
                                     pipelineReplica.id() );
                        operatorTupleQueueManager.releasePartitionedOperatorTupleQueues( regionId, operatorDef.id() );
                    }

                    if ( replicaIndex == 0 )
                    {
                        if ( operatorDef.operatorType() == STATEFUL )
                        {
                            LOGGER.info( "Releasing default operator kvStore of Operator {} in Pipeline {}",
                                         operatorDef.id(),
                                         pipelineReplica.id() );
                            operatorKvStoreManager.releaseDefaultOperatorKVStore( regionId, operatorDef.id() );
                        }
                        else if ( operatorDef.operatorType() == PARTITIONED_STATEFUL )
                        {
                            LOGGER.info( "Releasing partitioned operator kvStore of Operator {} in Pipeline {}",
                                         operatorDef.id(),
                                         pipelineReplica.id() );
                            operatorKvStoreManager.releasePartitionedOperatorKVStores( regionId, operatorDef.id() );
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
        final String operatorId = operatorDef.id();
        final OperatorTupleQueue[] operatorTupleQueues;

        if ( flow.getUpstreamConnections( operatorId ).isEmpty() )
        {
            LOGGER.info( "Creating {} for regionId={} operatorId={}", EmptyOperatorTupleQueue.class.getSimpleName(), regionId, operatorId );
            operatorTupleQueues = new OperatorTupleQueue[ replicaCount ];
            for ( int replicaIndex = 0; replicaIndex < replicaCount; replicaIndex++ )
            {
                operatorTupleQueues[ replicaIndex ] = new EmptyOperatorTupleQueue( operatorId, operatorDef.inputPortCount() );
            }
        }
        else if ( operatorDef.operatorType() == PARTITIONED_STATEFUL )
        {
            LOGGER.info( "Creating {} for regionId={} operatorId={}", PartitionedOperatorTupleQueue.class.getSimpleName(),
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
            LOGGER.info( "Creating {} {} for regionId={} operatorId={}",
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
        final String operatorId = operatorDef.id();

        if ( isFirstOperator )
        {
            LOGGER.info( "Creating {} for regionId={} operatorId={}",
                         BlockingTupleQueueDrainerPool.class.getSimpleName(),
                         regionId,
                         operatorId );
        }
        else
        {
            LOGGER.info( "Creating {} for regionId={} operatorId={}",
                         NonBlockingTupleQueueDrainerPool.class.getSimpleName(),
                         regionId,
                         operatorId );
        }

        final TupleQueueDrainerPool[] drainerPools = new TupleQueueDrainerPool[ replicaCount ];
        for ( int replicaIndex = 0; replicaIndex < replicaCount; replicaIndex++ )
        {
            drainerPools[ replicaIndex ] = createTupleQueueDrainer( operatorDef, isFirstOperator );
        }

        return drainerPools;
    }

    private TupleQueueDrainerPool createTupleQueueDrainer ( final OperatorDef operatorDef, final boolean isFirstOperator )
    {
        return isFirstOperator
               ? new BlockingTupleQueueDrainerPool( config, operatorDef )
               : new NonBlockingTupleQueueDrainerPool( config, operatorDef );
    }

    private OperatorKVStore[] createOperatorKVStores ( final int regionId, final int replicaCount, final OperatorDef operatorDef )
    {
        final String operatorId = operatorDef.id();
        final OperatorKVStore[] operatorKvStores;
        if ( operatorDef.operatorType() == STATELESS )
        {
            LOGGER.info( "Creating {} for regionId={} operatorId={}", EmptyOperatorKVStore.class.getSimpleName(), regionId, operatorId );
            operatorKvStores = new OperatorKVStore[ replicaCount ];
            for ( int replicaIndex = 0; replicaIndex < replicaCount; replicaIndex++ )
            {

                operatorKvStores[ replicaIndex ] = new EmptyOperatorKVStore( operatorId );
            }
        }
        else if ( operatorDef.operatorType() == PARTITIONED_STATEFUL )
        {
            LOGGER.info( "Creating {} for regionId={} operatorId={}", PartitionedOperatorKVStore.class.getSimpleName(),
                         regionId,
                         operatorId );
            final PartitionDistribution partitionDistribution = partitionService.getPartitionDistributionOrFail( regionId );
            operatorKvStores = operatorKvStoreManager.createPartitionedOperatorKVStores( regionId, operatorId, partitionDistribution );
        }
        else
        {
            LOGGER.info( "Creating {} for regionId={} operatorId={}", DefaultOperatorKVStore.class.getSimpleName(), regionId, operatorId );
            checkArgument( replicaCount == 1, "invalid replica count for operator %s in region %s", operatorDef.id(), regionId );
            operatorKvStores = new OperatorKVStore[ 1 ];
            operatorKvStores[ 0 ] = operatorKvStoreManager.createDefaultOperatorKVStore( regionId, operatorId );
        }
        return operatorKvStores;
    }

    private Supplier<TuplesImpl>[] createOutputSuppliers ( final int regionId,
                                                           final int replicaCount,
                                                           final boolean isLastOperator,
                                                           final OperatorDef operatorDef )
    {
        final String operatorId = operatorDef.id();
        if ( isLastOperator )
        {
            LOGGER.info( "Creating {} for last operator of regionId={} operatorId={}",
                         NonCachedTuplesImplSupplier.class.getSimpleName(),
                         regionId,
                         operatorId );
        }
        else
        {
            LOGGER.info( "Creating {} for regionId={} operatorId={}",
                         CachedTuplesImplSupplier.class.getSimpleName(),
                         regionId,
                         operatorId );
        }

        final Supplier<TuplesImpl>[] outputSuppliers = new Supplier[ replicaCount ];
        for ( int replicaIndex = 0; replicaIndex < replicaCount; replicaIndex++ )
        {
            outputSuppliers[ replicaIndex ] = createOutputSupplier( operatorDef, isLastOperator );
        }

        return outputSuppliers;
    }

    private Supplier<TuplesImpl> createOutputSupplier ( final OperatorDef operatorDef, final boolean isLastOperator )
    {
        return isLastOperator
               ? new NonCachedTuplesImplSupplier( operatorDef.outputPortCount() )
               : new CachedTuplesImplSupplier( operatorDef.outputPortCount() );

    }

    private OperatorTupleQueue createPipelineTupleQueue ( final FlowDef flow,
                                                          final int regionId,
                                                          final int replicaIndex,
                                                          final OperatorReplica[] pipelineOperatorReplicas )
    {
        final OperatorDef firstOperatorDef = pipelineOperatorReplicas[ 0 ].getOperatorDef();
        if ( flow.getUpstreamConnections( firstOperatorDef.id() ).isEmpty() )
        {
            LOGGER.info( "Creating {} for pipeline tuple queue of regionId={} as pipeline has no input port",
                         EmptyOperatorTupleQueue.class.getSimpleName(),
                         regionId );
            return new EmptyOperatorTupleQueue( firstOperatorDef.id(), firstOperatorDef.inputPortCount() );
        }
        else
        {
            if ( firstOperatorDef.operatorType() == PARTITIONED_STATEFUL )
            {
                LOGGER.info( "Creating {} for pipeline tuple queue of regionId={} for pipeline operator={}",
                             DefaultOperatorTupleQueue.class.getSimpleName(),
                             regionId,
                             firstOperatorDef.id() );
                return operatorTupleQueueManager.createDefaultOperatorTupleQueue( regionId,
                                                                                  replicaIndex,
                                                                                  firstOperatorDef,
                                                                                  MULTI_THREADED );
            }
            else
            {
                LOGGER.info( "Creating {} for pipeline tuple queue of regionId={} as first operator is {}",
                             EmptyOperatorTupleQueue.class.getSimpleName(),
                             regionId,
                             firstOperatorDef.operatorType() );
                return new EmptyOperatorTupleQueue( firstOperatorDef.id(), firstOperatorDef.inputPortCount() );
            }
        }
    }

}
