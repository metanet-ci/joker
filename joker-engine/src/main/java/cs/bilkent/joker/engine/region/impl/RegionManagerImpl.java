package cs.bilkent.joker.engine.region.impl;

import java.util.ArrayList;
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
import cs.bilkent.joker.engine.pipeline.impl.tuplesupplier.CachedTuplesImplSupplier;
import cs.bilkent.joker.engine.region.PipelineTransformer;
import cs.bilkent.joker.engine.region.Region;
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
import cs.bilkent.joker.operator.Tuple;
import cs.bilkent.joker.operator.impl.TuplesImpl;
import static cs.bilkent.joker.operator.spec.OperatorType.PARTITIONED_STATEFUL;
import static cs.bilkent.joker.operator.spec.OperatorType.STATEFUL;
import static cs.bilkent.joker.operator.spec.OperatorType.STATELESS;
import static java.lang.Math.max;
import static java.lang.System.arraycopy;
import static java.util.stream.Collectors.toList;

@Singleton
@NotThreadSafe
public class RegionManagerImpl implements RegionManager
{

    private static final Logger LOGGER = LoggerFactory.getLogger( RegionManagerImpl.class );


    private final JokerConfig config;

    private final Class<Supplier<TuplesImpl>> pipelineTailOperatorOutputSupplierClass;

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
        this.pipelineTailOperatorOutputSupplierClass = config.getRegionManagerConfig().getPipelineTailOperatorOutputSupplierClass();
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

        checkPipelineStartIndices( regionId,
                                   regionExecutionPlan.getRegionDef().getOperatorCount(),
                                   regionExecutionPlan.getPipelineStartIndices() );

        LOGGER.info( "Creating components for regionId={} pipelineCount={} replicaCount={}", regionId, pipelineCount, replicaCount );

        if ( regionExecutionPlan.getRegionDef().getRegionType() == PARTITIONED_STATEFUL )
        {
            partitionService.createPartitionDistribution( regionId, replicaCount );
            LOGGER.info( "Created partition distribution for regionId={} with {} replicas", regionId, replicaCount );
        }

        final PipelineReplica[][] pipelineReplicas = new PipelineReplica[ pipelineCount ][ replicaCount ];

        for ( int pipelineIndex = 0; pipelineIndex < pipelineCount; pipelineIndex++ )
        {
            final int pipelineId = regionExecutionPlan.getPipelineStartIndex( pipelineIndex );
            final OperatorDef[] operatorDefs = regionExecutionPlan.getOperatorDefsByPipelineIndex( pipelineIndex );
            final int operatorCount = operatorDefs.length;
            final OperatorReplica[][] operatorReplicas = new OperatorReplica[ replicaCount ][ operatorCount ];
            LOGGER.info( "Initializing pipeline instance for regionId={} pipelineIndex={} with {} operators", regionId, pipelineIndex,
                         operatorCount );

            final PipelineReplicaId[] pipelineReplicaIds = createPipelineReplicaIds( regionId, replicaCount, pipelineId );
            final int forwardKeyLimit = regionExecutionPlan.getRegionDef().getPartitionFieldNames().size();

            final PipelineReplicaMeter[] replicaMeters = new PipelineReplicaMeter[ replicaCount ];
            for ( int replicaIndex = 0; replicaIndex < replicaCount; replicaIndex++ )
            {
                replicaMeters[ replicaIndex ] = new PipelineReplicaMeter( config.getMetricManagerConfig().getTickMask(),
                                                                          pipelineReplicaIds[ replicaIndex ],
                                                                          operatorDefs[ 0 ],
                                                                          operatorDefs[ operatorCount - 1 ] );
            }

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
                                                                                             outputSuppliers[ replicaIndex ],
                                                                                             replicaMeters[ replicaIndex ] );
                }
            }

            for ( int replicaIndex = 0; replicaIndex < replicaCount; replicaIndex++ )
            {
                LOGGER.info( "Creating pipeline instance for regionId={} replicaIndex={} pipelineIndex={} pipelineStartIndex={}",
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

        final Region region = new Region( regionExecutionPlan, pipelineReplicas );
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
        pipelineIdsSorted.sort( PipelineId::compareTo );

        checkArgument( pipelineIdsSorted.get( 0 ).getRegionId() == pipelineIdsSorted.get( pipelineIdsSorted.size() - 1 ).getRegionId(),
                       "multiple region ids in %s",
                       pipelineIds );
        checkArgument( pipelineIdsSorted.stream().map( PipelineId::getPipelineStartIndex ).distinct().count() == pipelineIds.size(),
                       "duplicate pipeline ids in %s",
                       pipelineIds );

        final Region region = regions.get( pipelineIdsSorted.get( 0 ).getRegionId() );
        checkArgument( region != null, "no region found for %s", pipelineIds );

        return pipelineIdsSorted;
    }

    @Override
    public Region mergePipelines ( final List<PipelineId> pipelineIdsToMerge )
    {
        final List<Integer> startIndicesToMerge = getMergeablePipelineStartIndices( pipelineIdsToMerge );
        final int regionId = pipelineIdsToMerge.get( 0 ).getRegionId();

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
        final Region region = regions.get( pipelineId.getRegionId() );
        checkArgument( region != null, "invalid Pipeline %s to split", pipelineId );

        int curr = 0;
        final int operatorCount = region.getExecutionPlan().getOperatorCountByPipelineStartIndex( pipelineId.getPipelineStartIndex() );
        for ( int p : pipelineOperatorIndicesToSplit )
        {
            checkArgument( p > curr && p < operatorCount );
            curr = p;
        }

        final List<Integer> pipelineStartIndicesToSplit = new ArrayList<>();
        pipelineStartIndicesToSplit.add( pipelineId.getPipelineStartIndex() );
        for ( int i : pipelineOperatorIndicesToSplit )
        {
            pipelineStartIndicesToSplit.add( pipelineId.getPipelineStartIndex() + i );
        }

        return pipelineStartIndicesToSplit;
    }

    @Override
    public Region splitPipeline ( final PipelineId pipelineId, final List<Integer> pipelineOperatorIndicesToSplit )
    {
        final List<Integer> pipelineStartIndicesToSplit = getPipelineStartIndicesToSplit( pipelineId, pipelineOperatorIndicesToSplit );
        final int regionId = pipelineId.getRegionId();

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
                     regionId, newReplicaCount, regionExecutionPlan.getReplicaCount() );

        drainPipelineTupleQueues( region );

        rebalanceRegion( region, newReplicaCount );

        final Region newRegion;
        if ( regionExecutionPlan.getReplicaCount() < newReplicaCount )
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
                final GreedyDrainer drainer = new GreedyDrainer( operatorDef.getInputPortCount() );
                pipelineTupleQueue.drain( drainer );
                final TuplesImpl result = drainer.getResult();
                if ( result != null && result.isNonEmpty() )
                {
                    LOGGER.info( "Draining pipeline tuple queue of {}", pipelineReplica.id() );
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

                LOGGER.info( "Rebalancing regionId={} {} operator: {} to {} replicas",
                             regionId, STATELESS, operatorDef.getId(),
                             newReplicaCount );

                if ( newReplicaCount > currentReplicaCount )
                {
                    for ( int replicaIndex = currentReplicaCount; replicaIndex < newReplicaCount; replicaIndex++ )
                    {
                        final boolean isFirstOperator = ( operatorIndex == 0 );
                        final ThreadingPreference threadingPreference = getThreadingPreference( isFirstOperator );
                        LOGGER.info( "Creating {} {} for regionId={} replicaIndex={} operatorId={}",
                                     threadingPreference,
                                     DefaultOperatorTupleQueue.class.getSimpleName(),
                                     regionId, replicaIndex, operatorDef.getId() );
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
                        LOGGER.info( "Releasing operator tuple queue of Pipeline {} Operator {}",
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
            final GreedyDrainer drainer = new GreedyDrainer( inputPortCount );
            operator.getQueue().drain( drainer );
            final TuplesImpl result = drainer.getResult();
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
            LOGGER.info( "Extending tuple queues of regionId={} operator {} to {}", regionId, operatorDef.getId(), capacity );
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

    private Region extendRegionReplicas ( final FlowDef flow, final Region region, final int newReplicaCount )
    {
        final int regionId = region.getRegionId();
        final RegionExecutionPlan regionExecutionPlan = region.getExecutionPlan();
        final int currentReplicaCount = regionExecutionPlan.getReplicaCount();
        final PipelineReplica[][] newPipelineReplicas = new PipelineReplica[ regionExecutionPlan.getPipelineCount() ][ newReplicaCount ];
        for ( int pipelineIndex = 0; pipelineIndex < regionExecutionPlan.getPipelineCount(); pipelineIndex++ )
        {
            final PipelineReplica[] currentPipelineReplicas = region.getPipelineReplicas( pipelineIndex );
            arraycopy( currentPipelineReplicas, 0, newPipelineReplicas[ pipelineIndex ], 0, currentReplicaCount );

            LOGGER.info( "{} replicas of pipelineIndex={} of regionId={} are copied.", currentReplicaCount, pipelineIndex, regionId );

            final int pipelineId = regionExecutionPlan.getPipelineStartIndex( pipelineIndex );
            final OperatorDef[] operatorDefs = regionExecutionPlan.getOperatorDefsByPipelineIndex( pipelineIndex );
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
                final PipelineReplicaMeter replicaMeter = new PipelineReplicaMeter( config.getMetricManagerConfig().getTickMask(),
                                                                                    pipelineReplicaId,
                                                                                    operatorDefs[ 0 ],
                                                                                    operatorDefs[ operatorCount - 1 ] );
                for ( int operatorIndex = 0; operatorIndex < operatorCount; operatorIndex++ )
                {
                    final OperatorDef operatorDef = operatorDefs[ operatorIndex ];
                    final String operatorId = operatorDef.getId();
                    final boolean isFirstOperator = ( operatorIndex == 0 ), isLastOperator = ( operatorIndex == ( operatorCount - 1 ) );

                    final OperatorTupleQueue operatorTupleQueue;
                    if ( flow.getInboundConnections( operatorId ).isEmpty() )
                    {
                        LOGGER.info( "Creating {} for regionId={} replicaIndex={} operatorId={}",
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
                    LOGGER.info( "Creating {} for regionId={} replicaIndex={} operatorId={}",
                                 drainerPool.getClass().getSimpleName(),
                                 regionId,
                                 replicaIndex,
                                 operatorId );

                    final OperatorKVStore operatorKVStore;
                    if ( operatorDef.getOperatorType() == STATELESS )
                    {
                        LOGGER.info( "Creating {} for regionId={} replicaIndex={} operatorId={}",
                                     EmptyOperatorKVStore.class.getSimpleName(),
                                     regionId,
                                     replicaIndex,
                                     operatorId );
                        operatorKVStore = new EmptyOperatorKVStore( operatorId );
                    }
                    else
                    {
                        checkState( operatorDef.getOperatorType() == PARTITIONED_STATEFUL );
                        LOGGER.info( "Creating {} for regionId={} replicaIndex={} operatorId={}",
                                     PartitionedOperatorKVStore.class.getSimpleName(),
                                     regionId,
                                     replicaIndex,
                                     operatorId );
                        final OperatorKVStore[] operatorKvStores = operatorKvStoreManager.getPartitionedOperatorKVStoresOrFail( regionId,
                                                                                                                                operatorId );
                        operatorKVStore = operatorKvStores[ replicaIndex ];
                    }

                    final Supplier<TuplesImpl> outputSupplier = createOutputSupplier( operatorDef, isLastOperator );
                    LOGGER.info( "Creating {} for regionId={} replicaIndex={} operatorId={}",
                                 outputSupplier.getClass().getSimpleName(),
                                 regionId,
                                 replicaIndex,
                                 operatorId );

                    operatorReplicas[ operatorIndex ] = new OperatorReplica( pipelineReplicaId,
                                                                             operatorDef,
                                                                             operatorTupleQueue,
                                                                             operatorKVStore, drainerPool, outputSupplier, replicaMeter );
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

        LOGGER.info( "regionId={} is extended to {} replicas", regionId, newReplicaCount );

        return new Region( newRegionExecutionPlan, newPipelineReplicas );
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
                    LOGGER.info( "Released pipeline tuple queue of Pipeline {} Operator {}",
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

        return new Region( newRegionExecutionPlan, newPipelineReplicas );
    }

    private List<Integer> getMergeablePipelineStartIndices ( final List<PipelineId> pipelineIds )
    {
        final List<PipelineId> pipelineIdsSorted = getMergeablePipelineIds( pipelineIds );
        final Region region = regions.get( pipelineIds.get( 0 ).getRegionId() );

        final List<Integer> startIndicesToMerge = pipelineIdsSorted.stream().map( PipelineId::getPipelineStartIndex ).collect( toList() );
        final RegionExecutionPlan regionExecutionPlan = region.getExecutionPlan();

        if ( !pipelineTransformer.checkPipelineStartIndicesToMerge( regionExecutionPlan, startIndicesToMerge ) )
        {
            throw new IllegalArgumentException( "invalid pipeline start indices to merge: " + startIndicesToMerge
                                                + " current pipeline start indices: " + regionExecutionPlan.getPipelineStartIndices()
                                                + " regionId=" + region.getRegionId() );
        }

        return startIndicesToMerge;
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
                    LOGGER.info( "Releasing default tuple queue of pipeline {}", pipelineReplica.id() );
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
                        LOGGER.info( "Releasing default tuple queue of Operator {} in Pipeline {} replicaIndex {}", operatorDef.getId(),
                                     pipelineReplica.id(),
                                     replicaIndex );
                        operatorTupleQueueManager.releaseDefaultOperatorTupleQueue( regionId, replicaIndex, operatorDef.getId() );
                    }
                    else if ( queue instanceof PartitionedOperatorTupleQueue && replicaIndex == 0 )
                    {
                        LOGGER.info( "Releasing partitioned tuple queue of Operator {} in Pipeline {}", operatorDef.getId(),
                                     pipelineReplica.id() );
                        operatorTupleQueueManager.releasePartitionedOperatorTupleQueues( regionId, operatorDef.getId() );
                    }

                    if ( replicaIndex == 0 )
                    {
                        if ( operatorDef.getOperatorType() == STATEFUL )
                        {
                            LOGGER.info( "Releasing default operator kvStore of Operator {} in Pipeline {}", operatorDef.getId(),
                                         pipelineReplica.id() );
                            operatorKvStoreManager.releaseDefaultOperatorKVStore( regionId, operatorDef.getId() );
                        }
                        else if ( operatorDef.getOperatorType() == PARTITIONED_STATEFUL )
                        {
                            LOGGER.info( "Releasing partitioned operator kvStore of Operator {} in Pipeline {}", operatorDef.getId(),
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
            LOGGER.info( "Creating {} for regionId={} operatorId={}", EmptyOperatorTupleQueue.class.getSimpleName(), regionId, operatorId );
            operatorTupleQueues = new OperatorTupleQueue[ replicaCount ];
            for ( int replicaIndex = 0; replicaIndex < replicaCount; replicaIndex++ )
            {
                operatorTupleQueues[ replicaIndex ] = new EmptyOperatorTupleQueue( operatorId, operatorDef.getInputPortCount() );
            }
        }
        else if ( operatorDef.getOperatorType() == PARTITIONED_STATEFUL )
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
        final String operatorId = operatorDef.getId();

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
            LOGGER.info( "Creating {} for regionId={} operatorId={}", EmptyOperatorKVStore.class.getSimpleName(), regionId, operatorId );
            operatorKvStores = new OperatorKVStore[ replicaCount ];
            for ( int replicaIndex = 0; replicaIndex < replicaCount; replicaIndex++ )
            {

                operatorKvStores[ replicaIndex ] = new EmptyOperatorKVStore( operatorId );
            }
        }
        else if ( operatorDef.getOperatorType() == PARTITIONED_STATEFUL )
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
            checkArgument( replicaCount == 1, "invalid replica count for operator %s in region %s", operatorDef.getId(), regionId );
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
        final String operatorId = operatorDef.getId();
        if ( isLastOperator )
        {
            LOGGER.info( "Creating {} for last operator of regionId={} operatorId={}",
                         pipelineTailOperatorOutputSupplierClass.getSimpleName(),
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
        return isLastOperator ? TuplesImplSupplierUtils.newInstance( pipelineTailOperatorOutputSupplierClass,
                                                                     operatorDef.getOutputPortCount() )
                              : new CachedTuplesImplSupplier( operatorDef.getOutputPortCount() );

    }

    private OperatorTupleQueue createPipelineTupleQueue ( final FlowDef flow,
                                                          final int regionId,
                                                          final int replicaIndex,
                                                          final OperatorReplica[] pipelineOperatorReplicas )
    {
        final OperatorDef firstOperatorDef = pipelineOperatorReplicas[ 0 ].getOperatorDef();
        if ( flow.getInboundConnections( firstOperatorDef.getId() ).isEmpty() )
        {
            LOGGER.info( "Creating {} for pipeline tuple queue of regionId={} as pipeline has no input port",
                         EmptyOperatorTupleQueue.class.getSimpleName(),
                         regionId );
            return new EmptyOperatorTupleQueue( firstOperatorDef.getId(), firstOperatorDef.getInputPortCount() );
        }
        else
        {
            if ( firstOperatorDef.getOperatorType() == PARTITIONED_STATEFUL )
            {
                LOGGER.info( "Creating {} for pipeline tuple queue of regionId={} for pipeline operator={}",
                             DefaultOperatorTupleQueue.class.getSimpleName(), regionId, firstOperatorDef.getId() );
                return operatorTupleQueueManager.createDefaultOperatorTupleQueue( regionId,
                                                                                  replicaIndex,
                                                                                  firstOperatorDef,
                                                                                  MULTI_THREADED );
            }
            else
            {
                LOGGER.info( "Creating {} for pipeline tuple queue of regionId={} as first operator is {}",
                             EmptyOperatorTupleQueue.class.getSimpleName(), regionId, firstOperatorDef.getOperatorType() );
                return new EmptyOperatorTupleQueue( firstOperatorDef.getId(), firstOperatorDef.getInputPortCount() );
            }
        }
    }

}
