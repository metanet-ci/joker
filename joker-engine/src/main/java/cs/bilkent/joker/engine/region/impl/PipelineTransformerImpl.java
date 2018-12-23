package cs.bilkent.joker.engine.region.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import javax.annotation.concurrent.NotThreadSafe;
import javax.inject.Inject;
import javax.inject.Singleton;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cs.bilkent.joker.engine.config.JokerConfig;
import static cs.bilkent.joker.engine.config.ThreadingPref.MULTI_THREADED;
import cs.bilkent.joker.engine.flow.PipelineId;
import cs.bilkent.joker.engine.flow.RegionDef;
import cs.bilkent.joker.engine.flow.RegionExecPlan;
import cs.bilkent.joker.engine.kvstore.OperatorKVStore;
import cs.bilkent.joker.engine.kvstore.OperatorKVStoreManager;
import cs.bilkent.joker.engine.metric.PipelineReplicaMeter;
import cs.bilkent.joker.engine.partition.PartitionKeyExtractor;
import cs.bilkent.joker.engine.partition.PartitionKeyExtractorFactory;
import cs.bilkent.joker.engine.partition.PartitionService;
import cs.bilkent.joker.engine.pipeline.OperatorReplica;
import cs.bilkent.joker.engine.pipeline.PipelineReplica;
import cs.bilkent.joker.engine.pipeline.PipelineReplicaId;
import cs.bilkent.joker.engine.pipeline.UpstreamCtx;
import cs.bilkent.joker.engine.pipeline.impl.invocation.DefaultOutputCollector;
import cs.bilkent.joker.engine.pipeline.impl.invocation.FusedInvocationCtx;
import cs.bilkent.joker.engine.pipeline.impl.invocation.FusedPartitionedInvocationCtx;
import cs.bilkent.joker.engine.region.PipelineTransformer;
import cs.bilkent.joker.engine.region.Region;
import static cs.bilkent.joker.engine.region.Region.isFusible;
import static cs.bilkent.joker.engine.region.impl.RegionExecPlanUtil.checkPipelineStartIndicesToSplit;
import cs.bilkent.joker.engine.tuplequeue.OperatorQueue;
import cs.bilkent.joker.engine.tuplequeue.OperatorQueueManager;
import cs.bilkent.joker.engine.tuplequeue.TupleQueueDrainerPool;
import cs.bilkent.joker.engine.tuplequeue.impl.drainer.GreedyDrainer;
import cs.bilkent.joker.engine.tuplequeue.impl.drainer.pool.BlockingTupleQueueDrainerPool;
import cs.bilkent.joker.engine.tuplequeue.impl.drainer.pool.NonBlockingTupleQueueDrainerPool;
import cs.bilkent.joker.engine.tuplequeue.impl.operator.DefaultOperatorQueue;
import cs.bilkent.joker.engine.tuplequeue.impl.operator.EmptyOperatorQueue;
import static cs.bilkent.joker.impl.com.google.common.base.Preconditions.checkArgument;
import static cs.bilkent.joker.impl.com.google.common.base.Preconditions.checkState;
import cs.bilkent.joker.operator.Operator;
import cs.bilkent.joker.operator.OperatorDef;
import cs.bilkent.joker.operator.Tuple;
import cs.bilkent.joker.operator.impl.DefaultInvocationCtx;
import cs.bilkent.joker.operator.impl.InternalInvocationCtx;
import cs.bilkent.joker.operator.impl.OutputCollector;
import cs.bilkent.joker.operator.impl.TuplesImpl;
import cs.bilkent.joker.operator.kvstore.KVStore;
import cs.bilkent.joker.operator.scheduling.SchedulingStrategy;
import static cs.bilkent.joker.operator.spec.OperatorType.PARTITIONED_STATEFUL;
import static cs.bilkent.joker.operator.spec.OperatorType.STATEFUL;
import cs.bilkent.joker.partition.impl.PartitionKey;
import static java.lang.System.arraycopy;
import static java.util.Arrays.asList;
import static java.util.Collections.reverse;
import static java.util.stream.Collectors.toList;

@Singleton
@NotThreadSafe
public class PipelineTransformerImpl implements PipelineTransformer
{

    private static final Logger LOGGER = LoggerFactory.getLogger( PipelineTransformerImpl.class );


    private final JokerConfig config;

    private final PartitionService partitionService;

    private final OperatorQueueManager operatorQueueManager;

    private final OperatorKVStoreManager operatorKVStoreManager;

    private final PartitionKeyExtractorFactory partitionKeyExtractorFactory;

    @Inject
    public PipelineTransformerImpl ( final JokerConfig config,
                                     final PartitionService partitionService,
                                     final OperatorQueueManager operatorQueueManager,
                                     final OperatorKVStoreManager operatorKVStoreManager,
                                     final PartitionKeyExtractorFactory partitionKeyExtractorFactory )
    {
        this.config = config;
        this.partitionService = partitionService;
        this.operatorQueueManager = operatorQueueManager;
        this.operatorKVStoreManager = operatorKVStoreManager;
        this.partitionKeyExtractorFactory = partitionKeyExtractorFactory;
    }

    @Override
    public Region mergePipelines ( Region region, final List<Integer> pipelineStartIndicesToMerge )
    {
        final int upPipelineStartIndex = pipelineStartIndicesToMerge.get( 0 );
        for ( int downPipelineStartIndex : pipelineStartIndicesToMerge.subList( 1, pipelineStartIndicesToMerge.size() ) )
        {
            region = mergePipelines( region, upPipelineStartIndex, downPipelineStartIndex );
        }

        return region;
    }

    public Region mergePipelines ( final Region region, final int upPipelineStartIndex, final int downPipelineStartIndex )
    {
        final int regionId = region.getRegionId();
        final RegionExecPlan execPlan = region.getExecPlan();
        LOGGER.info( "Merging pipelines: {} of regionId={} with pipeline start indices: {}",
                     asList( upPipelineStartIndex, downPipelineStartIndex ),
                     regionId,
                     execPlan.getPipelineStartIndices() );

        checkArgument( ( execPlan.getPipelineIndex( upPipelineStartIndex ) + 1 ) == execPlan.getPipelineIndex( downPipelineStartIndex ) );

        processMergedPipelineFirstOperatorQueue( region, downPipelineStartIndex );

        final int replicaCount = execPlan.getReplicaCount();
        final PipelineReplica[][] newPipelineReplicas = new PipelineReplica[ execPlan.getPipelineCount() - 1 ][ replicaCount ];
        final PipelineId pipelineId = new PipelineId( regionId, upPipelineStartIndex );
        final int upPipelineIndex = execPlan.getPipelineIndex( upPipelineStartIndex );

        copyNonMergedPipelines( region, upPipelineIndex, newPipelineReplicas );

        final SchedulingStrategy[] schedulingStrategies = region.getSchedulingStrategies();

        for ( int replicaIndex = 0; replicaIndex < replicaCount; replicaIndex++ )
        {
            final PipelineReplicaId upPipelineReplicaId = new PipelineReplicaId( pipelineId, replicaIndex );
            final PipelineReplicaMeter meter = new PipelineReplicaMeter( config.getMetricManagerConfig().getTickMask(),
                                                                         upPipelineReplicaId,
                                                                         region.getRegionDef().getOperator( upPipelineStartIndex ) );
            final PipelineReplica upPipelineReplica = region.getPipelineReplica( upPipelineReplicaId );
            final PipelineReplica downPipelineReplica = region.getPipelineReplica( new PipelineReplicaId( regionId,
                                                                                                          downPipelineStartIndex,
                                                                                                          replicaIndex ) );
            final List<OperatorReplica> operatorReplicas = new ArrayList<>();

            // operators of the upstream pipeline except the last operator are duplicated
            for ( int i = 0, j = upPipelineReplica.getOperatorReplicaCount() - 1; i < j; i++ )
            {
                final OperatorReplica operator = upPipelineReplica.getOperatorReplica( i );
                operatorReplicas.add( operator.duplicate( upPipelineReplicaId,
                                                          meter,
                                                          config.getPipelineManagerConfig().getLatencyStageTickMask(),
                                                          operator.getQueue(),
                                                          operator.getDrainerPool(),
                                                          operator.getDownstreamCtx() ) );
            }

            if ( isFusible( schedulingStrategies[ downPipelineStartIndex ] ) )
            {
                final OperatorReplica duplicate = fuseMergedOperatorReplicas( region,
                                                                              replicaIndex,
                                                                              upPipelineReplicaId,
                                                                              meter,
                                                                              upPipelineReplica,
                                                                              downPipelineReplica );
                operatorReplicas.add( duplicate );
            }
            else
            {
                final OperatorReplica upOperator = upPipelineReplica.getOperatorReplica( upPipelineReplica.getOperatorReplicaCount() - 1 );
                final OperatorReplica tailOperator = downPipelineReplica.getOperatorReplica( 0 );
                // last operator of the upstream pipeline gets a new downstream context
                operatorReplicas.add( upOperator.duplicate( upPipelineReplicaId,
                                                            meter, config.getPipelineManagerConfig().getLatencyStageTickMask(),
                                                            upOperator.getQueue(),
                                                            upOperator.getDrainerPool(),
                                                            tailOperator.getUpstreamCtx( 0 ) ) );
                // first operator of the downstream pipeline gets a new pipeline replica id
                final OperatorDef downOperatorDef = tailOperator.getOperatorDef( 0 );
                final OperatorQueue downOperatorQueue;
                if ( downOperatorDef.getOperatorType() == PARTITIONED_STATEFUL )
                {
                    downOperatorQueue = operatorQueueManager.getPartitionedQueueOrFail( regionId, downOperatorDef.getId(), replicaIndex );
                }
                else
                {
                    downOperatorQueue = operatorQueueManager.getDefaultQueue( regionId, downOperatorDef.getId(), replicaIndex );
                }
                final TupleQueueDrainerPool drainerPool = new NonBlockingTupleQueueDrainerPool( config, downOperatorDef );
                operatorReplicas.add( tailOperator.duplicate( upPipelineReplicaId,
                                                              meter,
                                                              config.getPipelineManagerConfig().getLatencyStageTickMask(),
                                                              downOperatorQueue,
                                                              drainerPool,
                                                              tailOperator.getDownstreamCtx() ) );
            }

            // operators of the downstream pipeline except the first operator are duplicated
            for ( int i = 1; i < downPipelineReplica.getOperatorReplicaCount(); i++ )
            {
                final OperatorReplica operator = downPipelineReplica.getOperatorReplica( i );
                operatorReplicas.add( operator.duplicate( upPipelineReplicaId,
                                                          meter,
                                                          config.getPipelineManagerConfig().getLatencyStageTickMask(),
                                                          operator.getQueue(),
                                                          operator.getDrainerPool(),
                                                          operator.getDownstreamCtx() ) );
            }

            final OperatorReplica[] operatorReplicasArray = operatorReplicas.toArray( new OperatorReplica[ 0 ] );
            newPipelineReplicas[ upPipelineIndex ][ replicaIndex ] = upPipelineReplica.duplicate( meter, operatorReplicasArray );
        }

        return new Region( execPlan.withMergedPipelines( asList( upPipelineStartIndex, downPipelineStartIndex ) ),
                           region.getSchedulingStrategies(),
                           region.getUpstreamCtxes(),
                           newPipelineReplicas );
    }

    private void processMergedPipelineFirstOperatorQueue ( final Region region, final int pipelineStartIndex )
    {
        final int regionId = region.getRegionId();
        final RegionExecPlan regionExecPlan = region.getExecPlan();
        final SchedulingStrategy[] schedulingStrategies = region.getSchedulingStrategies();
        final OperatorDef operatorDef = region.getRegionDef().getOperator( pipelineStartIndex );
        final String operatorId = operatorDef.getId();

        if ( isFusible( schedulingStrategies[ pipelineStartIndex ] ) )
        {
            for ( int replicaIndex = 0; replicaIndex < regionExecPlan.getReplicaCount(); replicaIndex++ )
            {
                final OperatorQueue queue = operatorQueueManager.getDefaultQueueOrFail( regionId, operatorId, replicaIndex );
                checkState( queue.isEmpty() );

                operatorQueueManager.releaseDefaultQueue( regionId, operatorId, replicaIndex );
            }

            if ( operatorDef.getOperatorType() == PARTITIONED_STATEFUL )
            {
                for ( OperatorQueue queue : operatorQueueManager.getPartitionedQueuesOrFail( regionId, operatorId ) )
                {
                    checkState( queue.isEmpty() );
                }

                operatorQueueManager.releasePartitionedQueues( regionId, operatorId );
            }
        }
        else if ( operatorDef.getOperatorType() == PARTITIONED_STATEFUL )
        {
            for ( int replicaIndex = 0; replicaIndex < regionExecPlan.getReplicaCount(); replicaIndex++ )
            {
                final OperatorQueue pipelineQueue = operatorQueueManager.getDefaultQueueOrFail( regionId, operatorId, replicaIndex );
                final OperatorQueue operatorQueue = operatorQueueManager.getPartitionedQueueOrFail( regionId, operatorId, replicaIndex );
                drainPipelineQueue( pipelineQueue, operatorQueue, operatorDef );

                operatorQueueManager.releaseDefaultQueue( regionId, operatorId, replicaIndex );
            }
        }
        else
        {
            for ( int replicaIndex = 0; replicaIndex < regionExecPlan.getReplicaCount(); replicaIndex++ )
            {
                operatorQueueManager.switchThreadingPref( regionId, operatorId, replicaIndex );
            }
        }
    }

    private void copyNonMergedPipelines ( final Region region, final int pipelineIndex, final PipelineReplica[][] newPipelineReplicas )
    {
        final RegionExecPlan execPlan = region.getExecPlan();
        final int replicaCount = execPlan.getReplicaCount();
        for ( int i = 0; i < pipelineIndex; i++ )
        {
            LOGGER.debug( "copying non-merged pipelineIndex={} of replicaId={}", i, region.getRegionId() );
            arraycopy( region.getPipelineReplicas( i ), 0, newPipelineReplicas[ i ], 0, replicaCount );
        }

        for ( int i = ( pipelineIndex + 2 ); i < execPlan.getPipelineCount(); i++ )
        {
            final int j = i - 1;
            LOGGER.debug( "copying non-merged pipelineIndex={} to new pipelineIndex={} of replicaId={}", i, j, region.getRegionId() );
            arraycopy( region.getPipelineReplicas( i ), 0, newPipelineReplicas[ j ], 0, replicaCount );
        }
    }

    private OperatorReplica fuseMergedOperatorReplicas ( final Region region,
                                                         final int replicaIndex,
                                                         final PipelineReplicaId pipelineReplicaId,
                                                         final PipelineReplicaMeter meter,
                                                         final PipelineReplica upPipelineReplica,
                                                         final PipelineReplica downPipelineReplica )
    {
        final int regionId = region.getRegionId();
        final OperatorReplica upOperatorReplica = upPipelineReplica.getOperatorReplica( upPipelineReplica.getOperatorReplicaCount() - 1 );
        final OperatorReplica downOperatorReplica = downPipelineReplica.getOperatorReplica( 0 );
        final int upOperatorCount = upOperatorReplica.getOperatorCount();
        final int downOperatorCount = downOperatorReplica.getOperatorCount();
        final OperatorDef[] operatorDefs = new OperatorDef[ upOperatorCount + downOperatorCount ];
        final Operator[] operators = new Operator[ upOperatorCount + downOperatorCount ];
        final UpstreamCtx[] upstreamCtxes = new UpstreamCtx[ upOperatorCount + downOperatorCount ];
        final InternalInvocationCtx[] invocationCtxes = new InternalInvocationCtx[ upOperatorCount + downOperatorCount ];

        // copy from the upstream operator replica
        for ( int i = 0; i < upOperatorCount; i++ )
        {
            operatorDefs[ i ] = upOperatorReplica.getOperatorDef( i );
            operators[ i ] = upOperatorReplica.getOperator( i );
            upstreamCtxes[ i ] = upOperatorReplica.getUpstreamCtx( i );
        }

        // copy from the downstream operator replica
        for ( int i = 0; i < downOperatorCount; i++ )
        {
            operatorDefs[ upOperatorCount + i ] = downOperatorReplica.getOperatorDef( i );
            operators[ upOperatorCount + i ] = downOperatorReplica.getOperator( i );
            upstreamCtxes[ upOperatorCount + i ] = downOperatorReplica.getUpstreamCtx( i );
        }

        // copy fused invocation contexts of the downstream operator replica
        for ( int i = 1; i < downOperatorCount; i++ )
        {
            invocationCtxes[ upOperatorCount + i ] = downOperatorReplica.getInvocationCtx( i );
        }

        // create a new fused invocation context for the first operator of the downstream operator replica
        final OperatorDef fusingOperatorDef = downOperatorReplica.getOperatorDef( 0 );
        final OutputCollector fusingOutputCollector;
        if ( downOperatorCount == 1 )
        {
            // TODO we should use the next operatorDef...
            final TuplesImpl output = new TuplesImpl( fusingOperatorDef.getOutputPortCount() );
            fusingOutputCollector = new DefaultOutputCollector( output );
        }
        else
        {
            fusingOutputCollector = (OutputCollector) invocationCtxes[ upOperatorCount + 1 ];
        }

        if ( fusingOperatorDef.getOperatorType() == PARTITIONED_STATEFUL )
        {
            final OperatorKVStore kvStore = operatorKVStoreManager.getPartitionedKVStore( regionId,
                                                                                          fusingOperatorDef.getId(),
                                                                                          replicaIndex );
            final List<String> partitionFieldNames = fusingOperatorDef.getPartitionFieldNames();
            final int forwardedKeySize = region.getRegionDef().getForwardedKeySize();
            final PartitionKeyExtractor ext = partitionKeyExtractorFactory.createPartitionKeyExtractor( partitionFieldNames,
                                                                                                        forwardedKeySize );

            invocationCtxes[ upOperatorCount ] = new FusedPartitionedInvocationCtx( fusingOperatorDef.getInputPortCount(),
                                                                                    kvStore::getKVStore,
                                                                                    ext,
                                                                                    fusingOutputCollector );

        }
        else
        {
            final Function<PartitionKey, KVStore> kvStoreSupplier;
            if ( fusingOperatorDef.getOperatorType() == STATEFUL )
            {
                kvStoreSupplier = operatorKVStoreManager.getDefaultKVStore( regionId, fusingOperatorDef.getId() )::getKVStore;
            }
            else
            {
                kvStoreSupplier = k -> null;
            }

            invocationCtxes[ upOperatorCount ] = new FusedInvocationCtx( fusingOperatorDef.getInputPortCount(),
                                                                         kvStoreSupplier,
                                                                         fusingOutputCollector );
        }

        // create new fused invocation contexts for fused operators of the upstream operator replica
        for ( int i = upOperatorCount - 1; i > 0; i-- )
        {
            final OperatorDef operatorDef = upOperatorReplica.getOperatorDef( i );
            final OutputCollector outputCollector = (OutputCollector) invocationCtxes[ i + 1 ];

            if ( operatorDef.getOperatorType() == PARTITIONED_STATEFUL )
            {
                final OperatorKVStore kvStore = operatorKVStoreManager.getPartitionedKVStore( regionId, operatorDef.getId(), replicaIndex );
                final List<String> partitionFieldNames = operatorDef.getPartitionFieldNames();
                final int forwardedKeySize = region.getRegionDef().getForwardedKeySize();
                final PartitionKeyExtractor ext = partitionKeyExtractorFactory.createPartitionKeyExtractor( partitionFieldNames,
                                                                                                            forwardedKeySize );
                invocationCtxes[ i ] = new FusedPartitionedInvocationCtx( operatorDef.getInputPortCount(),
                                                                          kvStore::getKVStore,
                                                                          ext,
                                                                          outputCollector );
            }
            else
            {
                final Function<PartitionKey, KVStore> kvStoreSupplier;
                if ( operatorDef.getOperatorType() == STATEFUL )
                {
                    kvStoreSupplier = operatorKVStoreManager.getDefaultKVStore( regionId, operatorDef.getId() )::getKVStore;
                }
                else
                {
                    kvStoreSupplier = k -> null;
                }

                invocationCtxes[ i ] = new FusedInvocationCtx( operatorDef.getInputPortCount(), kvStoreSupplier, outputCollector );
            }
        }

        final OperatorDef upOperatorDef = upOperatorReplica.getOperatorDef( 0 );
        final Function<PartitionKey, KVStore> kvStoreSupplier;
        if ( upOperatorDef.getOperatorType() == PARTITIONED_STATEFUL )
        {
            kvStoreSupplier = operatorKVStoreManager.getPartitionedKVStore( regionId, upOperatorDef.getId(), replicaIndex )::getKVStore;
        }
        else if ( upOperatorDef.getOperatorType() == STATEFUL )
        {
            kvStoreSupplier = operatorKVStoreManager.getDefaultKVStore( regionId, upOperatorDef.getId() )::getKVStore;
        }
        else
        {
            kvStoreSupplier = k -> null;
        }

        invocationCtxes[ 0 ] = new DefaultInvocationCtx( upOperatorDef.getInputPortCount(),
                                                         kvStoreSupplier,
                                                         (OutputCollector) invocationCtxes[ 1 ] );

        final Function<PartitionKey, TuplesImpl> drainerTuplesSupplier = ( (DefaultInvocationCtx) invocationCtxes[ 0 ] )::createInputTuples;
        return OperatorReplica.newRunningInstance( pipelineReplicaId,
                                                   meter, config.getPipelineManagerConfig().getLatencyStageTickMask(),
                                                   upOperatorReplica.getQueue(),
                                                   upOperatorReplica.getDrainerPool(),
                                                   operatorDefs,
                                                   drainerTuplesSupplier,
                                                   invocationCtxes,
                                                   operators,
                                                   upstreamCtxes,
                                                   region.getSchedulingStrategy( pipelineReplicaId.pipelineId.getPipelineStartIndex() ),
                                                   downOperatorReplica.getDownstreamCtx() );
    }

    private void drainPipelineQueue ( final OperatorQueue pipelineQueue, final OperatorQueue operatorQueue, final OperatorDef operatorDef )
    {
        final GreedyDrainer drainer = new GreedyDrainer( operatorDef.getInputPortCount() );
        final TuplesImpl result = new TuplesImpl( operatorDef.getInputPortCount() );
        pipelineQueue.drain( drainer, key -> result );
        if ( result.isNonEmpty() )
        {
            for ( int portIndex = 0; portIndex < result.getPortCount(); portIndex++ )
            {
                final List<Tuple> tuples = result.getTuplesModifiable( portIndex );
                final int offered = operatorQueue.offer( portIndex, tuples );
                checkState( offered == tuples.size() );
            }
        }
    }

    @Override
    public Region splitPipeline ( Region region, final List<Integer> pipelineStartIndicesToSplit )
    {
        final int pipelineStartIndex = pipelineStartIndicesToSplit.get( 0 );
        final List<Integer> splitIndices = new ArrayList<>( pipelineStartIndicesToSplit.subList( 1, pipelineStartIndicesToSplit.size() ) );
        reverse( splitIndices );

        for ( int splitPipelineStartIndex : splitIndices )
        {
            region = splitPipeline( region, pipelineStartIndex, splitPipelineStartIndex );
        }

        return region;
    }

    public Region splitPipeline ( final Region region, final int pipelineStartIndex, final int splitPipelineStartIndex )
    {
        final int regionId = region.getRegionId();
        final RegionExecPlan execPlan = region.getExecPlan();
        final RegionDef regionDef = execPlan.getRegionDef();
        LOGGER.info( "Splitting pipeline: {} of regionId={} at pipeline start index: {} from region pipeline start indices: {}",
                     pipelineStartIndex,
                     regionId,
                     splitPipelineStartIndex,
                     execPlan.getPipelineStartIndices() );

        checkPipelineStartIndicesToSplit( execPlan, pipelineStartIndex, splitPipelineStartIndex );

        final int replicaCount = execPlan.getReplicaCount();
        final PipelineReplica[][] newPipelineReplicas = new PipelineReplica[ execPlan.getPipelineCount() + 1 ][ replicaCount ];
        final OperatorDef splitOperatorDef = regionDef.getOperator( splitPipelineStartIndex );
        final int pipelineIndex = execPlan.getPipelineIndex( pipelineStartIndex );

        copyNonSplitPipelines( region, pipelineIndex, newPipelineReplicas );

        if ( isFusible( region.getSchedulingStrategies()[ splitPipelineStartIndex ] ) )
        {
            for ( int replicaIndex = 0; replicaIndex < replicaCount; replicaIndex++ )
            {
                operatorQueueManager.createDefaultQueue( regionId, splitOperatorDef, replicaIndex, MULTI_THREADED );
            }

            if ( splitOperatorDef.getOperatorType() == PARTITIONED_STATEFUL )
            {
                operatorQueueManager.createPartitionedQueues( regionId,
                                                              splitOperatorDef,
                                                              partitionService.getPartitionDistributionOrFail( regionId ),
                                                              regionDef.getForwardedKeySize() );
            }

            for ( int replicaIndex = 0; replicaIndex < replicaCount; replicaIndex++ )
            {
                final PipelineReplicaId upPipelineReplicaId = new PipelineReplicaId( new PipelineId( regionId, pipelineStartIndex ),
                                                                                     replicaIndex );
                final PipelineReplicaId downPipelineReplicaId = new PipelineReplicaId( new PipelineId( regionId, splitPipelineStartIndex ),
                                                                                       replicaIndex );
                final PipelineReplicaMeter upMeter = new PipelineReplicaMeter( config.getMetricManagerConfig().getTickMask(),
                                                                               upPipelineReplicaId,
                                                                               region.getRegionDef().getOperator( pipelineStartIndex ) );
                final PipelineReplicaMeter downMeter = new PipelineReplicaMeter( config.getMetricManagerConfig().getTickMask(),
                                                                                 downPipelineReplicaId,
                                                                                 splitOperatorDef );

                final PipelineReplica pipelineReplica = region.getPipelineReplica( upPipelineReplicaId );

                final List<OperatorReplica> upstreamOperators = new ArrayList<>();
                final List<OperatorReplica> downstreamOperators = new ArrayList<>();
                final OperatorReplica splitOperatorReplica = duplicateExceptSplitFusedOperator( splitOperatorDef,
                                                                                                pipelineReplica,
                                                                                                upPipelineReplicaId,
                                                                                                downPipelineReplicaId,
                                                                                                upMeter,
                                                                                                downMeter,
                                                                                                upstreamOperators,
                                                                                                downstreamOperators );

                final int idx = splitOperatorReplica.getIndex( splitOperatorDef );

                upstreamOperators.add( duplicateSplitFusedOperatorReplicaHead( region,
                                                                               upPipelineReplicaId,
                                                                               upMeter,
                                                                               splitOperatorReplica,
                                                                               idx ) );

                newPipelineReplicas[ pipelineIndex ][ replicaIndex ] = pipelineReplica.duplicate( upMeter,
                                                                                                  upstreamOperators.toArray( new OperatorReplica[ 0 ] ) );

                downstreamOperators.add( 0,
                                         duplicateSplitFusedOperatorReplicaTail( region,
                                                                                 splitOperatorDef,
                                                                                 replicaIndex,
                                                                                 downPipelineReplicaId,
                                                                                 downMeter,
                                                                                 splitOperatorReplica,
                                                                                 idx ) );

                final OperatorQueue pipelineQueue;
                if ( splitOperatorDef.getOperatorType() == PARTITIONED_STATEFUL )
                {
                    LOGGER.debug( "Creating {} for pipeline queue of regionId={} replicaIndex={} for pipeline operator={}",
                                  DefaultOperatorQueue.class.getSimpleName(),
                                  regionId,
                                  replicaIndex,
                                  splitOperatorDef.getId() );
                    pipelineQueue = operatorQueueManager.getDefaultQueue( regionId, splitOperatorDef.getId(), replicaIndex );
                }
                else
                {
                    LOGGER.debug( "Creating {} for pipeline queue of regionId={} replicaIndex={} as first operator is {}",
                                  EmptyOperatorQueue.class.getSimpleName(),
                                  regionId,
                                  replicaIndex,
                                  splitOperatorDef.getOperatorType() );
                    pipelineQueue = new EmptyOperatorQueue( splitOperatorDef.getId(), splitOperatorDef.getInputPortCount() );
                }

                newPipelineReplicas[ pipelineIndex + 1 ][ replicaIndex ] = PipelineReplica.running( downPipelineReplicaId,
                                                                                                    downstreamOperators.toArray( new OperatorReplica[ 0 ] ),
                                                                                                    pipelineQueue,
                                                                                                    downMeter,
                                                                                                    region.getUpstreamCtxes()[ splitPipelineStartIndex ] );
            }

        }
        else
        {
            for ( int replicaIndex = 0; replicaIndex < replicaCount; replicaIndex++ )
            {
                final PipelineReplica pipelineReplica = region.getReplicaPipelines( replicaIndex )[ pipelineIndex ];
                final List<OperatorReplica> upstreamOperators = new ArrayList<>();
                final List<OperatorReplica> downstreamOperators = new ArrayList<>();
                splitOperatorReplicas( splitOperatorDef, pipelineReplica, upstreamOperators, downstreamOperators );
                newPipelineReplicas[ pipelineIndex ][ replicaIndex ] = duplicateSplitPipelineHead( pipelineReplica, upstreamOperators );
                newPipelineReplicas[ pipelineIndex + 1 ][ replicaIndex ] = duplicateSplitPipelineTail( regionId,
                                                                                                       splitPipelineStartIndex,
                                                                                                       replicaIndex,
                                                                                                       downstreamOperators );
            }
        }

        return new Region( execPlan.withSplitPipeline( asList( pipelineStartIndex, splitPipelineStartIndex ) ),
                           region.getSchedulingStrategies(),
                           region.getUpstreamCtxes(),
                           newPipelineReplicas );
    }

    private void copyNonSplitPipelines ( final Region region, final int pipelineIndex, final PipelineReplica[][] newPipelineReplicas )
    {
        final RegionExecPlan execPlan = region.getExecPlan();
        final int replicaCount = execPlan.getReplicaCount();
        for ( int i = 0; i < pipelineIndex; i++ )
        {
            LOGGER.debug( "copying non-split pipelineIndex={} of replicaId={}", i, region.getRegionId() );
            arraycopy( region.getPipelineReplicas( i ), 0, newPipelineReplicas[ i ], 0, replicaCount );
        }

        for ( int i = ( pipelineIndex + 1 ); i < execPlan.getPipelineCount(); i++ )
        {
            final int j = i + 1;
            LOGGER.debug( "copying non-split pipelineIndex={} to new pipelineIndex={} of replicaId={}", i, j, region.getRegionId() );
            arraycopy( region.getPipelineReplicas( i ), 0, newPipelineReplicas[ j ], 0, replicaCount );
        }
    }

    private OperatorReplica duplicateExceptSplitFusedOperator ( final OperatorDef operatorDef,
                                                                final PipelineReplica pipelineReplica,
                                                                final PipelineReplicaId upPipelineReplicaId,
                                                                final PipelineReplicaId downPipelineReplicaId,
                                                                final PipelineReplicaMeter upMeter,
                                                                final PipelineReplicaMeter downMeter,
                                                                final List<OperatorReplica> upstreamOperators,
                                                                final List<OperatorReplica> downstreamOperators )
    {
        OperatorReplica splitOperatorReplica = null;
        for ( OperatorReplica operatorReplica : pipelineReplica.getOperators() )
        {
            if ( splitOperatorReplica == null )
            {
                if ( operatorReplica.getIndex( operatorDef ) == -1 )
                {
                    upstreamOperators.add( operatorReplica.duplicate( upPipelineReplicaId,
                                                                      upMeter, config.getPipelineManagerConfig().getLatencyStageTickMask(),
                                                                      operatorReplica.getQueue(),
                                                                      operatorReplica.getDrainerPool(),
                                                                      operatorReplica.getDownstreamCtx() ) );
                }
                else
                {
                    splitOperatorReplica = operatorReplica;
                }
            }
            else
            {
                downstreamOperators.add( operatorReplica.duplicate( downPipelineReplicaId,
                                                                    downMeter, config.getPipelineManagerConfig().getLatencyStageTickMask(),
                                                                    operatorReplica.getQueue(),
                                                                    operatorReplica.getDrainerPool(),
                                                                    operatorReplica.getDownstreamCtx() ) );
            }
        }

        if ( upstreamOperators.size() > 0 )
        {
            final OperatorReplica upPipelineTailOperator = upstreamOperators.remove( upstreamOperators.size() - 1 );
            upstreamOperators.add( upPipelineTailOperator.duplicate( upPipelineReplicaId,
                                                                     upMeter, config.getPipelineManagerConfig().getLatencyStageTickMask(),
                                                                     upPipelineTailOperator.getQueue(),
                                                                     upPipelineTailOperator.getDrainerPool(),
                                                                     null ) );
        }

        checkState( splitOperatorReplica != null );

        return splitOperatorReplica;
    }

    private OperatorReplica duplicateSplitFusedOperatorReplicaHead ( final Region region,
                                                                     final PipelineReplicaId pipelineReplicaId,
                                                                     final PipelineReplicaMeter meter,
                                                                     final OperatorReplica operatorReplica,
                                                                     final int until )
    {
        final OperatorDef[] operatorDefs = new OperatorDef[ until ];
        final Operator[] operators = new Operator[ until ];
        final UpstreamCtx[] upstreamCtxes = new UpstreamCtx[ until ];
        final InternalInvocationCtx[] invocationCtxes = new InternalInvocationCtx[ until ];

        final RegionDef regionDef = region.getRegionDef();

        duplicateFusedOperatorInvocationCtxes( regionDef,
                                               pipelineReplicaId,
                                               operatorReplica,
                                               0,
                                               until,
                                               operatorDefs,
                                               operators,
                                               upstreamCtxes,
                                               invocationCtxes );

        return OperatorReplica.newRunningInstance( pipelineReplicaId,
                                                   meter, config.getPipelineManagerConfig().getLatencyStageTickMask(),
                                                   operatorReplica.getQueue(),
                                                   operatorReplica.getDrainerPool(),
                                                   operatorDefs,
                                                   ( (DefaultInvocationCtx) invocationCtxes[ 0 ] )::createInputTuples,
                                                   invocationCtxes,
                                                   operators,
                                                   upstreamCtxes,
                                                   region.getSchedulingStrategy( pipelineReplicaId.pipelineId.getPipelineStartIndex() ),
                                                   null );
    }

    private OperatorReplica duplicateSplitFusedOperatorReplicaTail ( final Region region,
                                                                     final OperatorDef operatorDef,
                                                                     final int replicaIndex,
                                                                     final PipelineReplicaId pipelineReplicaId,
                                                                     final PipelineReplicaMeter meter,
                                                                     final OperatorReplica operatorReplica,
                                                                     final int from )
    {
        final int operatorCount = operatorReplica.getOperatorCount() - from;
        final OperatorDef[] operatorDefs = new OperatorDef[ operatorCount ];
        final Operator[] operators = new Operator[ operatorCount ];
        final UpstreamCtx[] upstreamCtxes = new UpstreamCtx[ operatorCount ];
        final InternalInvocationCtx[] invocationCtxes = new InternalInvocationCtx[ operatorCount ];

        final RegionDef regionDef = region.getRegionDef();
        final int regionId = regionDef.getRegionId();

        duplicateFusedOperatorInvocationCtxes( regionDef,
                                               pipelineReplicaId,
                                               operatorReplica,
                                               from,
                                               operatorReplica.getOperatorCount(),
                                               operatorDefs,
                                               operators,
                                               upstreamCtxes,
                                               invocationCtxes );

        final OperatorQueue headOperatorQueue;
        final TupleQueueDrainerPool headTupleQueueDrainerPool;
        if ( operatorDef.getOperatorType() == PARTITIONED_STATEFUL )
        {
            headOperatorQueue = operatorQueueManager.getPartitionedQueueOrFail( regionId, operatorDef.getId(), replicaIndex );
            headTupleQueueDrainerPool = new NonBlockingTupleQueueDrainerPool( config, operatorDef );
        }
        else
        {
            headOperatorQueue = operatorQueueManager.getDefaultQueue( regionId, operatorDef.getId(), replicaIndex );
            headTupleQueueDrainerPool = new BlockingTupleQueueDrainerPool( config, operatorDef );
        }

        return OperatorReplica.newRunningInstance( pipelineReplicaId,
                                                   meter, config.getPipelineManagerConfig().getLatencyStageTickMask(),
                                                   headOperatorQueue,
                                                   headTupleQueueDrainerPool,
                                                   operatorDefs,
                                                   ( (DefaultInvocationCtx) invocationCtxes[ 0 ] )::createInputTuples,
                                                   invocationCtxes,
                                                   operators,
                                                   upstreamCtxes,
                                                   region.getSchedulingStrategy( pipelineReplicaId.pipelineId.getPipelineStartIndex() ),
                                                   operatorReplica.getDownstreamCtx() );
    }

    // from: inclusive, until: exclusive
    private void duplicateFusedOperatorInvocationCtxes ( final RegionDef regionDef,
                                                         final PipelineReplicaId pipelineReplicaId,
                                                         final OperatorReplica operatorReplica,
                                                         final int from,
                                                         final int until,
                                                         final OperatorDef[] operatorDefs,
                                                         final Operator[] operators,
                                                         final UpstreamCtx[] upstreamCtxes,
                                                         final InternalInvocationCtx[] invocationCtxes )
    {
        checkState( from < until );

        for ( int i = from; i < until; i++ )
        {
            final int j = i - from;
            operatorDefs[ j ] = operatorReplica.getOperatorDef( i );
            operators[ j ] = operatorReplica.getOperator( i );
            upstreamCtxes[ j ] = operatorReplica.getUpstreamCtx( i );
        }

        for ( int i = ( until - 1 ); i > from; i-- )
        {
            final int j = i - from;
            final OperatorDef operatorDef = operatorReplica.getOperatorDef( i );
            final OutputCollector outputCollector;
            if ( i < ( until - 1 ) )
            {
                outputCollector = (OutputCollector) invocationCtxes[ j + 1 ];
            }
            else
            {
                outputCollector = new DefaultOutputCollector( new TuplesImpl( operatorDef.getOutputPortCount() ) );
            }

            if ( operatorDef.getOperatorType() == PARTITIONED_STATEFUL )
            {
                final OperatorKVStore kvStore = operatorKVStoreManager.getPartitionedKVStore( regionDef.getRegionId(),
                                                                                              operatorDef.getId(),
                                                                                              pipelineReplicaId.replicaIndex );
                final List<String> partitionFieldNames = operatorDef.getPartitionFieldNames();
                final int forwardedKeySize = regionDef.getForwardedKeySize();
                final PartitionKeyExtractor ext = partitionKeyExtractorFactory.createPartitionKeyExtractor( partitionFieldNames,
                                                                                                            forwardedKeySize );
                invocationCtxes[ j ] = new FusedPartitionedInvocationCtx( operatorDef.getInputPortCount(),
                                                                          kvStore::getKVStore,
                                                                          ext,
                                                                          outputCollector );
            }
            else
            {
                final Function<PartitionKey, KVStore> kvStoreSupplier;
                if ( operatorDef.getOperatorType() == STATEFUL )
                {
                    kvStoreSupplier = operatorKVStoreManager.getDefaultKVStore( regionDef.getRegionId(), operatorDef.getId() )::getKVStore;
                }
                else
                {
                    kvStoreSupplier = k -> null;
                }

                invocationCtxes[ j ] = new FusedInvocationCtx( operatorDef.getInputPortCount(), kvStoreSupplier, outputCollector );
            }
        }

        final OperatorDef headOperatorDef = operatorReplica.getOperatorDef( from );
        final Function<PartitionKey, KVStore> kvStoreSupplier;
        if ( headOperatorDef.getOperatorType() == PARTITIONED_STATEFUL )
        {
            kvStoreSupplier = operatorKVStoreManager.getPartitionedKVStore( regionDef.getRegionId(),
                                                                            headOperatorDef.getId(),
                                                                            pipelineReplicaId.replicaIndex )::getKVStore;
        }
        else if ( headOperatorDef.getOperatorType() == STATEFUL )
        {
            kvStoreSupplier = operatorKVStoreManager.getDefaultKVStore( regionDef.getRegionId(), headOperatorDef.getId() )::getKVStore;
        }
        else
        {
            kvStoreSupplier = k -> null;
        }

        final OutputCollector outputCollector;
        if ( invocationCtxes.length > 1 )
        {
            outputCollector = (OutputCollector) invocationCtxes[ 1 ];
        }
        else
        {
            outputCollector = new DefaultOutputCollector( new TuplesImpl( headOperatorDef.getOutputPortCount() ) );
        }

        invocationCtxes[ 0 ] = new DefaultInvocationCtx( headOperatorDef.getInputPortCount(), kvStoreSupplier, outputCollector );
    }

    private PipelineReplica duplicateSplitPipelineHead ( final PipelineReplica pipelineReplica,
                                                         final List<OperatorReplica> operatorReplicas )
    {
        final PipelineReplicaMeter meter = new PipelineReplicaMeter( config.getMetricManagerConfig().getTickMask(),
                                                                     pipelineReplica.id(),
                                                                     operatorReplicas.get( 0 ).getOperatorDef( 0 ) );
        final OperatorReplica tailOperatorReplica = operatorReplicas.remove( operatorReplicas.size() - 1 );
        final List<OperatorReplica> duplicates = operatorReplicas.stream()
                                                                 .map( op -> op.duplicate( pipelineReplica.id(),
                                                                                           meter,
                                                                                           config.getPipelineManagerConfig()
                                                                                                 .getLatencyStageTickMask(),
                                                                                           op.getQueue(),
                                                                                           op.getDrainerPool(),
                                                                                           op.getDownstreamCtx() ) )
                                                                 .collect( toList() );
        duplicates.add( tailOperatorReplica.duplicate( pipelineReplica.id(),
                                                       meter, config.getPipelineManagerConfig().getLatencyStageTickMask(),
                                                       tailOperatorReplica.getQueue(),
                                                       tailOperatorReplica.getDrainerPool(),
                                                       null ) );

        return pipelineReplica.duplicate( meter, duplicates.toArray( new OperatorReplica[ 0 ] ) );
    }

    private PipelineReplica duplicateSplitPipelineTail ( final int regionId,
                                                         final int newPipelineStartIndex,
                                                         final int replicaIndex,
                                                         final List<OperatorReplica> operatorReplicas )
    {
        final OperatorReplica headOperatorReplica = operatorReplicas.get( 0 );
        final OperatorDef headOperatorDef = headOperatorReplica.getOperatorDef( 0 );
        final PipelineReplicaId newPipelineReplicaId = new PipelineReplicaId( regionId, newPipelineStartIndex, replicaIndex );
        final PipelineReplicaMeter meter = new PipelineReplicaMeter( config.getMetricManagerConfig().getTickMask(),
                                                                     newPipelineReplicaId,
                                                                     headOperatorDef );

        final OperatorQueue pipelineQueue;
        final OperatorQueue headOperatorQueue;
        final TupleQueueDrainerPool headOperatorDrainerPool;

        if ( headOperatorDef.getOperatorType() == PARTITIONED_STATEFUL )
        {
            LOGGER.debug( "Creating {} for pipeline queue of regionId={} replicaIndex={} for pipeline operator={}",
                          DefaultOperatorQueue.class.getSimpleName(),
                          regionId,
                          replicaIndex,
                          headOperatorDef.getId() );
            pipelineQueue = operatorQueueManager.createDefaultQueue( regionId, headOperatorDef, replicaIndex, MULTI_THREADED );
            headOperatorQueue = headOperatorReplica.getQueue();
            headOperatorDrainerPool = headOperatorReplica.getDrainerPool();
        }
        else
        {
            LOGGER.debug( "Creating {} for pipeline queue of regionId={} replicaIndex={} as first operator is {}",
                          EmptyOperatorQueue.class.getSimpleName(),
                          regionId,
                          replicaIndex,
                          headOperatorDef.getOperatorType() );
            pipelineQueue = new EmptyOperatorQueue( headOperatorDef.getId(), headOperatorDef.getInputPortCount() );
            headOperatorQueue = operatorQueueManager.switchThreadingPref( regionId, headOperatorDef.getId(), replicaIndex );
            headOperatorDrainerPool = new BlockingTupleQueueDrainerPool( config, headOperatorDef );
        }

        final List<OperatorReplica> newOperatorReplicas = operatorReplicas.stream()
                                                                          .skip( 1 )
                                                                          .map( op -> op.duplicate( newPipelineReplicaId,
                                                                                                    meter,
                                                                                                    config.getPipelineManagerConfig()
                                                                                                          .getLatencyStageTickMask(),
                                                                                                    op.getQueue(),
                                                                                                    op.getDrainerPool(),
                                                                                                    op.getDownstreamCtx() ) )
                                                                          .collect( toList() );
        newOperatorReplicas.add( 0,
                                 headOperatorReplica.duplicate( newPipelineReplicaId,
                                                                meter, config.getPipelineManagerConfig().getLatencyStageTickMask(),
                                                                headOperatorQueue,
                                                                headOperatorDrainerPool,
                                                                headOperatorReplica.getDownstreamCtx() ) );

        return PipelineReplica.running( newPipelineReplicaId,
                                        newOperatorReplicas.toArray( new OperatorReplica[ 0 ] ),
                                        pipelineQueue,
                                        meter,
                                        newOperatorReplicas.get( 0 ).getUpstreamCtx( 0 ) );
    }

    private void splitOperatorReplicas ( final OperatorDef splitOperator,
                                         final PipelineReplica pipelineReplica,
                                         final List<OperatorReplica> upstreamOperatorReplicas,
                                         final List<OperatorReplica> downstreamOperatorReplicas )
    {
        List<OperatorReplica> l = upstreamOperatorReplicas;
        for ( int i = 0; i < pipelineReplica.getOperatorReplicaCount(); i++ )
        {
            final OperatorReplica operatorReplica = pipelineReplica.getOperatorReplica( i );
            if ( operatorReplica.getOperatorDef( 0 ).equals( splitOperator ) )
            {
                l = downstreamOperatorReplicas;
            }

            l.add( operatorReplica );
        }
    }

}
