package cs.bilkent.joker.engine.region.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.concurrent.NotThreadSafe;
import javax.inject.Inject;
import javax.inject.Singleton;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cs.bilkent.joker.engine.config.JokerConfig;
import static cs.bilkent.joker.engine.config.ThreadingPreference.MULTI_THREADED;
import cs.bilkent.joker.engine.flow.PipelineId;
import cs.bilkent.joker.engine.flow.RegionDef;
import cs.bilkent.joker.engine.flow.RegionExecutionPlan;
import cs.bilkent.joker.engine.metric.PipelineReplicaMeter;
import cs.bilkent.joker.engine.pipeline.OperatorReplica;
import cs.bilkent.joker.engine.pipeline.PipelineReplica;
import cs.bilkent.joker.engine.pipeline.PipelineReplicaId;
import cs.bilkent.joker.engine.pipeline.UpstreamContext;
import cs.bilkent.joker.engine.pipeline.impl.tuplesupplier.CachedTuplesImplSupplier;
import cs.bilkent.joker.engine.region.PipelineTransformer;
import cs.bilkent.joker.engine.region.Region;
import cs.bilkent.joker.engine.tuplequeue.OperatorTupleQueue;
import cs.bilkent.joker.engine.tuplequeue.OperatorTupleQueueManager;
import cs.bilkent.joker.engine.tuplequeue.TupleQueueDrainerPool;
import cs.bilkent.joker.engine.tuplequeue.impl.drainer.GreedyDrainer;
import cs.bilkent.joker.engine.tuplequeue.impl.drainer.pool.BlockingTupleQueueDrainerPool;
import cs.bilkent.joker.engine.tuplequeue.impl.drainer.pool.NonBlockingTupleQueueDrainerPool;
import cs.bilkent.joker.engine.tuplequeue.impl.operator.DefaultOperatorTupleQueue;
import cs.bilkent.joker.engine.tuplequeue.impl.operator.EmptyOperatorTupleQueue;
import static cs.bilkent.joker.impl.com.google.common.base.Preconditions.checkState;
import cs.bilkent.joker.operator.OperatorDef;
import cs.bilkent.joker.operator.Tuple;
import cs.bilkent.joker.operator.impl.TuplesImpl;
import static cs.bilkent.joker.operator.spec.OperatorType.PARTITIONED_STATEFUL;
import static cs.bilkent.joker.operator.spec.OperatorType.STATEFUL;
import static cs.bilkent.joker.operator.spec.OperatorType.STATELESS;
import static java.lang.System.arraycopy;

@Singleton
@NotThreadSafe
public class PipelineTransformerImpl implements PipelineTransformer
{

    private static final Logger LOGGER = LoggerFactory.getLogger( PipelineTransformerImpl.class );


    private final JokerConfig config;

    private final Class<Supplier<TuplesImpl>> pipelineTailOperatorOutputSupplierClass;

    private final OperatorTupleQueueManager operatorTupleQueueManager;

    @Inject
    public PipelineTransformerImpl ( final JokerConfig config, final OperatorTupleQueueManager operatorTupleQueueManager )
    {
        this.config = config;
        this.pipelineTailOperatorOutputSupplierClass = config.getRegionManagerConfig().getPipelineTailOperatorOutputSupplierClass();
        this.operatorTupleQueueManager = operatorTupleQueueManager;
    }

    @Override
    public Region mergePipelines ( final Region region, final List<Integer> startIndicesToMerge )
    {
        final int regionId = region.getRegionId();

        final RegionExecutionPlan regionExecutionPlan = region.getExecutionPlan();
        final RegionDef regionDef = regionExecutionPlan.getRegionDef();
        LOGGER.info( "Merging pipelines: {} of regionId={} with pipeline start indices: {}",
                     startIndicesToMerge,
                     regionId, regionExecutionPlan.getPipelineStartIndices() );
        if ( !checkPipelineStartIndicesToMerge( regionExecutionPlan, startIndicesToMerge ) )
        {
            throw new IllegalArgumentException( "invalid pipeline start indices to merge: " + startIndicesToMerge
                                                + " current pipeline start indices: " + regionExecutionPlan.getPipelineStartIndices()
                                                + " regionId=" + regionId );
        }

        final int replicaCount = regionExecutionPlan.getReplicaCount();
        final int pipelineCountDecrease = startIndicesToMerge.size() - 1;
        final int newPipelineCount = regionExecutionPlan.getPipelineCount() - pipelineCountDecrease;
        final PipelineReplica[][] newPipelineReplicas = new PipelineReplica[ newPipelineCount ][ replicaCount ];
        final int newOperatorCount = getMergedPipelineOperatorCount( regionExecutionPlan, startIndicesToMerge );

        final int firstPipelineIndex = regionExecutionPlan.getPipelineIndex( startIndicesToMerge.get( 0 ) );

        copyNonMergedPipelines( region, startIndicesToMerge, newPipelineReplicas, pipelineCountDecrease );

        for ( int replicaIndex = 0; replicaIndex < replicaCount; replicaIndex++ )
        {
            final OperatorReplica[] newOperatorReplicas = new OperatorReplica[ newOperatorCount ];
            final PipelineReplica[] replicaPipelines = region.getReplicaPipelines( replicaIndex );
            final PipelineReplica firstPipelineReplica = replicaPipelines[ regionExecutionPlan.getPipelineIndex( startIndicesToMerge.get(
                    0 ) ) ];

            final PipelineReplicaId pipelineReplicaId = replicaPipelines[ firstPipelineIndex ].id();
            final PipelineReplicaMeter firstPipelineReplicaMeter = createPipelineReplicaMeter( pipelineReplicaId, regionExecutionPlan,
                                                                                               startIndicesToMerge );
            int operatorIndex = duplicateFirstPipelineToMergeOperators( firstPipelineReplica,
                                                                        newOperatorReplicas,
                                                                        firstPipelineReplicaMeter );

            for ( int i = 1; i < startIndicesToMerge.size(); i++ )
            {
                final PipelineReplica pipelineReplica = replicaPipelines[ regionExecutionPlan.getPipelineIndex( startIndicesToMerge.get(
                        i ) ) ];
                final boolean isLastMergedPipeline = i == ( startIndicesToMerge.size() - 1 );

                operatorIndex = duplicateMergedPipelineOperators( regionId,
                                                                  replicaIndex,
                                                                  newOperatorReplicas,
                                                                  operatorIndex,
                                                                  pipelineReplicaId,
                                                                  firstPipelineReplicaMeter,
                                                                  pipelineReplica,
                                                                  isLastMergedPipeline );
            }

            final List<String> operatorIds = Arrays.stream( newOperatorReplicas ).map( o -> o.getOperatorDef().getId() )
                                                   .collect( Collectors.toList() );

            LOGGER.info( "Duplicating {} with new operators: {}", firstPipelineReplica.id(), operatorIds );

            newPipelineReplicas[ firstPipelineIndex ][ replicaIndex ] = firstPipelineReplica.duplicate( firstPipelineReplicaMeter,
                                                                                                        newOperatorReplicas );
        }

        final List<Integer> newPipelineStartIndices = getPipelineStartIndicesAfterMerge( regionExecutionPlan.getPipelineStartIndices(),
                                                                                         startIndicesToMerge );
        final RegionExecutionPlan newRegionExecutionPlan = new RegionExecutionPlan( regionDef, newPipelineStartIndices, replicaCount );
        LOGGER.info( "new pipeline start indices: {} after merge of pipelines: {} for regionId={}",
                     newPipelineStartIndices,
                     startIndicesToMerge,
                     regionId );
        return new Region( newRegionExecutionPlan, newPipelineReplicas );
    }

    private PipelineReplicaMeter createPipelineReplicaMeter ( final PipelineReplicaId pipelineReplicaId,
                                                              final RegionExecutionPlan regionExecutionPlan,
                                                              final List<Integer> startIndicesToMerge )
    {
        final OperatorDef[] firstPipeline = regionExecutionPlan.getOperatorDefsByPipelineStartIndex( startIndicesToMerge.get( 0 ) );
        final int pipelineCount = startIndicesToMerge.size();
        final OperatorDef[] lastPipeline = regionExecutionPlan.getOperatorDefsByPipelineStartIndex( startIndicesToMerge.get(
                pipelineCount - 1 ) );
        final OperatorDef firstOperator = firstPipeline[ 0 ];
        final OperatorDef lastOperator = lastPipeline[ lastPipeline.length - 1 ];
        return new PipelineReplicaMeter( config.getMetricManagerConfig().getTickMask(), pipelineReplicaId, firstOperator, lastOperator );
    }

    @Override
    public boolean checkPipelineStartIndicesToMerge ( final RegionExecutionPlan regionExecutionPlan,
                                                      final List<Integer> pipelineStartIndicesToMerge )
    {
        if ( pipelineStartIndicesToMerge.size() < 2 )
        {
            return false;
        }

        final List<Integer> pipelineStartIndices = regionExecutionPlan.getPipelineStartIndices();

        int index = pipelineStartIndices.indexOf( pipelineStartIndicesToMerge.get( 0 ) );
        if ( index < 0 )
        {
            return false;
        }

        for ( int i = 1; i < pipelineStartIndicesToMerge.size(); i++ )
        {
            final int j = pipelineStartIndices.indexOf( pipelineStartIndicesToMerge.get( i ) );
            if ( j != ( index + 1 ) )
            {
                return false;
            }
            index = j;
        }

        return true;
    }

    private int getMergedPipelineOperatorCount ( final RegionExecutionPlan regionExecutionPlan, final List<Integer> startIndicesToMerge )
    {
        int newOperatorCount = 0;
        for ( int startIndex : startIndicesToMerge )
        {
            newOperatorCount += regionExecutionPlan.getOperatorCountByPipelineStartIndex( startIndex );
        }
        return newOperatorCount;
    }

    private void copyNonMergedPipelines ( final Region region,
                                          final List<Integer> startIndicesToMerge,
                                          final PipelineReplica[][] newPipelineReplicas,
                                          final int pipelineCountDecrease )
    {
        final RegionExecutionPlan execPlan = region.getExecutionPlan();
        final List<Integer> currentStartIndices = execPlan.getPipelineStartIndices();
        final int replicaCount = execPlan.getReplicaCount();
        final int before = currentStartIndices.indexOf( startIndicesToMerge.get( 0 ) );
        for ( int i = 0; i < before; i++ )
        {
            LOGGER.info( "copying non-merged pipelineIndex={} of replicaId={}", i, region.getRegionId() );
            arraycopy( region.getPipelineReplicas( i ), 0, newPipelineReplicas[ i ], 0, replicaCount );
        }

        final int after = 1 + currentStartIndices.indexOf( startIndicesToMerge.get( pipelineCountDecrease ) );
        for ( int i = after; i < currentStartIndices.size(); i++ )
        {
            final int j = i - pipelineCountDecrease;
            LOGGER.info( "copying non-merged pipelineIndex={} to new pipelineIndex={} of replicaId={}", i, j, region.getRegionId() );
            arraycopy( region.getPipelineReplicas( i ), 0, newPipelineReplicas[ j ], 0, replicaCount );
        }
    }

    private int duplicateFirstPipelineToMergeOperators ( final PipelineReplica pipelineReplica,
                                                         final OperatorReplica[] newOperatorReplicas,
                                                         final PipelineReplicaMeter firstPipelineReplicaMeter )
    {
        int operatorIndex = 0;
        final PipelineReplicaId pipelineReplicaId = pipelineReplica.id();

        for ( int i = 0, j = pipelineReplica.getOperatorCount() - 1; i < j; i++ )
        {
            final OperatorReplica operator = pipelineReplica.getOperator( i );
            newOperatorReplicas[ operatorIndex++ ] = operator.duplicate( pipelineReplicaId,
                                                                         operator.getQueue(),
                                                                         operator.getDrainerPool(),
                                                                         operator.getOutputSupplier(),
                                                                         firstPipelineReplicaMeter );
        }

        final OperatorReplica lastOperator = pipelineReplica.getOperator( pipelineReplica.getOperatorCount() - 1 );
        final Supplier<TuplesImpl> outputSupplier = new CachedTuplesImplSupplier( lastOperator.getOperatorDef().getInputPortCount() );
        newOperatorReplicas[ operatorIndex++ ] = lastOperator.duplicate( pipelineReplicaId,
                                                                         lastOperator.getQueue(),
                                                                         lastOperator.getDrainerPool(),
                                                                         outputSupplier,
                                                                         firstPipelineReplicaMeter );

        final List<String> operatorIds = Arrays.stream( pipelineReplica.getOperators() ).map( o -> o.getOperatorDef().getId() )
                                               .collect( Collectors.toList() );

        LOGGER.info( "Duplicated first pipeline {} to merge. operators: {}", pipelineReplicaId, operatorIds );

        return operatorIndex;
    }

    private int duplicateMergedPipelineOperators ( final int regionId,
                                                   final int replicaIndex,
                                                   final OperatorReplica[] newOperatorReplicas,
                                                   int operatorIndex,
                                                   final PipelineReplicaId newPipelineReplicaId,
                                                   final PipelineReplicaMeter replicaMeter,
                                                   final PipelineReplica pipelineReplica,
                                                   final boolean isLastMergedPipeline )
    {
        newOperatorReplicas[ operatorIndex++ ] = duplicateMergedPipelineFirstOperator( regionId,
                                                                                       replicaIndex, newPipelineReplicaId, replicaMeter,
                                                                                       pipelineReplica,
                                                                                       isLastMergedPipeline );

        for ( int i = 1, j = pipelineReplica.getOperatorCount(); i < j; i++ )
        {
            final OperatorReplica operator = pipelineReplica.getOperator( i );
            final Supplier<TuplesImpl> outputSupplier = ( ( i == ( j - 1 ) ) && isLastMergedPipeline )
                                                        ? TuplesImplSupplierUtils.newInstance( pipelineTailOperatorOutputSupplierClass,
                                                                                               operator.getOperatorDef()
                                                                                                       .getInputPortCount() )
                                                        : new CachedTuplesImplSupplier( operator.getOperatorDef().getInputPortCount() );

            newOperatorReplicas[ operatorIndex++ ] = operator.duplicate( newPipelineReplicaId,
                                                                         operator.getQueue(),
                                                                         operator.getDrainerPool(),
                                                                         outputSupplier,
                                                                         replicaMeter );
            LOGGER.info( "Duplicating operator {} of {} to {}", operator.getOperatorDef().getId(),
                         pipelineReplica.id(),
                         newPipelineReplicaId );

        }

        return operatorIndex;
    }

    private OperatorReplica duplicateMergedPipelineFirstOperator ( final int regionId,
                                                                   final int replicaIndex,
                                                                   final PipelineReplicaId newPipelineReplicaId,
                                                                   final PipelineReplicaMeter replicaMeter,
                                                                   final PipelineReplica pipelineReplica,
                                                                   final boolean isLastMergedPipeline )
    {
        final OperatorTupleQueue pipelineSelfTupleQueue = pipelineReplica.getSelfPipelineTupleQueue();
        final OperatorReplica firstOperator = pipelineReplica.getOperator( 0 );
        final OperatorTupleQueue firstOperatorQueue;
        final OperatorDef firstOperatorDef = firstOperator.getOperatorDef();
        if ( pipelineSelfTupleQueue instanceof EmptyOperatorTupleQueue )
        {
            if ( firstOperator.getQueue() instanceof DefaultOperatorTupleQueue )
            {
                final String operatorId = firstOperatorDef.getId();
                firstOperatorQueue = operatorTupleQueueManager.switchThreadingPreference( regionId, replicaIndex, operatorId );
            }
            else
            {
                throw new IllegalStateException( "It is not expected to have " + firstOperator.getQueue() + " for first operator "
                                                 + firstOperator.getOperatorName() + " of PipelineReplica " + pipelineReplica.id() );
            }
        }
        else
        {
            firstOperatorQueue = firstOperator.getQueue();
            drainPipelineTupleQueue( pipelineSelfTupleQueue, firstOperatorQueue, firstOperatorDef );

            LOGGER.info( "Drained queue of pipeline {} for merge.", pipelineReplica.id() );
            operatorTupleQueueManager.releaseDefaultOperatorTupleQueue( regionId, replicaIndex, firstOperatorDef.getId() );
        }

        final NonBlockingTupleQueueDrainerPool drainerPool = new NonBlockingTupleQueueDrainerPool( config, firstOperatorDef );
        final Supplier<TuplesImpl> outputSupplier = ( isLastMergedPipeline && ( pipelineReplica.getOperatorCount() == 1 ) )
                                                    ? TuplesImplSupplierUtils.newInstance( pipelineTailOperatorOutputSupplierClass,
                                                                                           firstOperatorDef.getInputPortCount() )
                                                    : new CachedTuplesImplSupplier( firstOperatorDef.getInputPortCount() );
        LOGGER.info( "Duplicating first operator {} of {} to {}", firstOperatorDef.getId(), pipelineReplica.id(), newPipelineReplicaId );
        return firstOperator.duplicate( newPipelineReplicaId, firstOperatorQueue, drainerPool, outputSupplier, replicaMeter );
    }

    private void drainPipelineTupleQueue ( final OperatorTupleQueue pipelineTupleQueue,
                                           final OperatorTupleQueue operatorTupleQueue,
                                           final OperatorDef operatorDef )
    {
        final GreedyDrainer drainer = new GreedyDrainer( operatorDef.getInputPortCount() );
        pipelineTupleQueue.drain( drainer );
        final TuplesImpl result = drainer.getResult();
        if ( result != null && result.isNonEmpty() )
        {
            for ( int portIndex = 0; portIndex < result.getPortCount(); portIndex++ )
            {
                final List<Tuple> tuples = result.getTuplesModifiable( portIndex );
                final int offered = operatorTupleQueue.offer( portIndex, tuples );
                checkState( offered == tuples.size() );
            }
        }
    }

    private List<Integer> getPipelineStartIndicesAfterMerge ( final List<Integer> currentStartIndices,
                                                              final List<Integer> startIndicesToMerge )
    {
        final List<Integer> newPipelineStartIndices = new ArrayList<>();
        final List<Integer> excluded = startIndicesToMerge.subList( 1, startIndicesToMerge.size() );
        for ( int startIndex : currentStartIndices )
        {
            if ( !excluded.contains( startIndex ) )
            {
                newPipelineStartIndices.add( startIndex );
            }
        }
        return newPipelineStartIndices;
    }

    @Override
    public Region splitPipeline ( final Region region, final List<Integer> startIndicesToSplit )
    {
        final int regionId = region.getRegionId();

        final RegionExecutionPlan regionExecutionPlan = region.getExecutionPlan();
        final RegionDef regionDef = regionExecutionPlan.getRegionDef();
        LOGGER.info( "Splitting pipelines: {} of regionId={} with pipeline start indices: {}",
                     startIndicesToSplit,
                     regionId, regionExecutionPlan.getPipelineStartIndices() );

        if ( !checkPipelineStartIndicesToSplit( regionExecutionPlan, startIndicesToSplit ) )
        {
            throw new IllegalArgumentException( "invalid pipeline start indices to split: " + startIndicesToSplit
                                                + " current pipeline start indices: " + regionExecutionPlan.getPipelineStartIndices()
                                                + " regionId=" + regionId );
        }

        final int replicaCount = regionExecutionPlan.getReplicaCount();
        final int pipelineCountIncrease = startIndicesToSplit.size() - 1;
        final int newPipelineCount = regionExecutionPlan.getPipelineCount() + pipelineCountIncrease;
        final PipelineReplica[][] newPipelineReplicas = new PipelineReplica[ newPipelineCount ][ replicaCount ];
        final int firstPipelineIndex = regionExecutionPlan.getPipelineIndex( startIndicesToSplit.get( 0 ) );

        copyNonSplitPipelines( region, startIndicesToSplit, newPipelineReplicas, pipelineCountIncrease );

        for ( int replicaIndex = 0; replicaIndex < replicaCount; replicaIndex++ )
        {
            final PipelineReplica pipelineReplica = region.getReplicaPipelines( replicaIndex )[ firstPipelineIndex ];

            for ( int i = 0; i < startIndicesToSplit.size(); i++ )
            {
                final int curr = startIndicesToSplit.get( i );
                final int newOperatorCount;
                if ( i < startIndicesToSplit.size() - 1 )
                {
                    newOperatorCount = startIndicesToSplit.get( i + 1 ) - curr;
                }
                else if ( firstPipelineIndex == regionExecutionPlan.getPipelineCount() - 1 )
                {
                    newOperatorCount = regionDef.getOperatorCount() - curr;
                }
                else
                {
                    newOperatorCount = regionExecutionPlan.getPipelineStartIndex( firstPipelineIndex + 1 ) - curr;
                }

                final OperatorReplica[] newOperatorReplicas = new OperatorReplica[ newOperatorCount ];

                final PipelineId newPipelineId = new PipelineId( regionId, curr );
                final PipelineReplicaId newPipelineReplicaId = new PipelineReplicaId( newPipelineId, replicaIndex );

                final int startOperatorIndex = curr - startIndicesToSplit.get( 0 );
                final int endOperatorIndex = startOperatorIndex + newOperatorCount - 1;
                final PipelineReplicaMeter replicaMeter = new PipelineReplicaMeter( config.getMetricManagerConfig().getTickMask(),
                                                                                    newPipelineReplicaId,
                                                                                    pipelineReplica.getOperatorDef( startOperatorIndex ),
                                                                                    pipelineReplica.getOperatorDef( endOperatorIndex ) );

                final boolean switchThreadingPreferenceOfFirstOperator = ( i > 0 );
                duplicateSplitPipelineOperators( pipelineReplica,
                                                 startOperatorIndex,
                                                 newOperatorCount,
                                                 switchThreadingPreferenceOfFirstOperator,
                                                 newPipelineReplicaId,
                                                 replicaMeter,
                                                 newOperatorReplicas );

                final OperatorDef firstOperatorDef = newOperatorReplicas[ 0 ].getOperatorDef();

                final PipelineReplica newPipelineReplica;
                if ( i == 0 )
                {
                    newPipelineReplica = pipelineReplica.duplicate( replicaMeter, newOperatorReplicas );
                }
                else
                {
                    final OperatorTupleQueue pipelineTupleQueue;
                    if ( firstOperatorDef.getOperatorType() == PARTITIONED_STATEFUL )
                    {
                        LOGGER.info( "Creating {} for pipeline tuple queue of regionId={} replicaIndex={} for pipeline operator={}",
                                     DefaultOperatorTupleQueue.class.getSimpleName(),
                                     regionId,
                                     replicaIndex, firstOperatorDef.getId() );
                        pipelineTupleQueue = operatorTupleQueueManager.createDefaultOperatorTupleQueue( regionId,
                                                                                                        replicaIndex,
                                                                                                        firstOperatorDef,
                                                                                                        MULTI_THREADED );
                    }
                    else
                    {
                        LOGGER.info( "Creating {} for pipeline tuple queue of regionId={} replicaIndex={} as first operator is {}",
                                     EmptyOperatorTupleQueue.class.getSimpleName(),
                                     regionId,
                                     replicaIndex, firstOperatorDef.getOperatorType() );
                        pipelineTupleQueue = new EmptyOperatorTupleQueue( firstOperatorDef.getId(), firstOperatorDef.getInputPortCount() );
                    }

                    final PipelineReplica prevPipeline = newPipelineReplicas[ firstPipelineIndex + ( i - 1 ) ][ replicaIndex ];
                    final UpstreamContext upstreamContext = prevPipeline.getOperator( prevPipeline.getOperatorCount() - 1 )
                                                                        .getSelfUpstreamContext();
                    newPipelineReplica = new PipelineReplica( config,
                                                              newPipelineReplicaId,
                                                              newOperatorReplicas,
                                                              pipelineTupleQueue,
                                                              replicaMeter,
                                                              upstreamContext );

                    final List<String> operatorIds = Arrays.stream( newOperatorReplicas ).map( o -> o.getOperatorDef().getId() )
                                                           .collect( Collectors.toList() );

                    LOGGER.info( "Split pipeline {} to split with operators: {}", newPipelineReplicaId, operatorIds );
                }

                newPipelineReplicas[ firstPipelineIndex + i ][ replicaIndex ] = newPipelineReplica;
            }
        }

        final List<Integer> newPipelineStartIndices = getPipelineStartIndicesAfterSplit( regionExecutionPlan, startIndicesToSplit );
        final RegionExecutionPlan newRegionExecutionPlan = new RegionExecutionPlan( regionDef, newPipelineStartIndices, replicaCount );
        LOGGER.info( "new pipeline start indices: {} after split to pipelines: {} for regionId={}",
                     newPipelineStartIndices,
                     startIndicesToSplit,
                     regionId );
        return new Region( newRegionExecutionPlan, newPipelineReplicas );
    }

    @Override
    public boolean checkPipelineStartIndicesToSplit ( final RegionExecutionPlan regionExecutionPlan,
                                                      final List<Integer> pipelineStartIndicesToSplit )
    {
        if ( pipelineStartIndicesToSplit.size() < 2 )
        {
            return false;
        }

        final List<Integer> pipelineStartIndices = regionExecutionPlan.getPipelineStartIndices();

        int start = pipelineStartIndices.indexOf( pipelineStartIndicesToSplit.get( 0 ) );
        if ( start < 0 )
        {
            return false;
        }

        final int limit = ( start < pipelineStartIndices.size() - 1 )
                          ? pipelineStartIndices.get( start + 1 ) : regionExecutionPlan.getRegionDef().getOperatorCount();

        for ( int i = 1; i < pipelineStartIndicesToSplit.size(); i++ )
        {
            final int index = pipelineStartIndicesToSplit.get( i );
            if ( index <= pipelineStartIndicesToSplit.get( i - 1 ) || index >= limit )
            {
                return false;
            }
        }

        return true;
    }

    private void copyNonSplitPipelines ( final Region region,
                                         final List<Integer> startIndicesToSplit,
                                         final PipelineReplica[][] newPipelineReplicas,
                                         final int pipelineCountIncrease )
    {
        final RegionExecutionPlan execPlan = region.getExecutionPlan();
        final List<Integer> currentStartIndices = execPlan.getPipelineStartIndices();
        final int replicaCount = execPlan.getReplicaCount();
        final int before = currentStartIndices.indexOf( startIndicesToSplit.get( 0 ) );
        for ( int i = 0; i < before; i++ )
        {
            LOGGER.info( "copying non-split pipelineIndex={} of replicaId={}", i, region.getRegionId() );
            arraycopy( region.getPipelineReplicas( i ), 0, newPipelineReplicas[ i ], 0, replicaCount );
        }

        final int after = 1 + currentStartIndices.indexOf( startIndicesToSplit.get( 0 ) );
        for ( int i = after; i < currentStartIndices.size(); i++ )
        {
            final int j = i + pipelineCountIncrease;
            LOGGER.info( "copying non-split pipelineIndex={} to new pipelineIndex={} of replicaId={}", i, j, region.getRegionId() );
            arraycopy( region.getPipelineReplicas( i ), 0, newPipelineReplicas[ j ], 0, replicaCount );
        }
    }

    private void duplicateSplitPipelineOperators ( final PipelineReplica pipelineReplica,
                                                   final int startOperatorIndex,
                                                   final int operatorCount,
                                                   final boolean switchThreadingPreferenceOfFirstOperator,
                                                   final PipelineReplicaId newPipelineReplicaId,
                                                   final PipelineReplicaMeter replicaMeter,
                                                   final OperatorReplica[] newOperatorReplicas )
    {
        for ( int i = 0; i < operatorCount; i++ )
        {
            final OperatorReplica operator = pipelineReplica.getOperator( startOperatorIndex + i );
            final OperatorDef operatorDef = operator.getOperatorDef();

            final OperatorTupleQueue queue;
            final boolean isFirstOperator = ( i == 0 );
            final boolean multiThreaded =
                    isFirstOperator && ( operatorDef.getOperatorType() == STATEFUL || operatorDef.getOperatorType() == STATELESS );
            if ( switchThreadingPreferenceOfFirstOperator && multiThreaded )
            {
                queue = operatorTupleQueueManager.switchThreadingPreference( newPipelineReplicaId.pipelineId.getRegionId(),
                                                                             newPipelineReplicaId.replicaIndex, operatorDef.getId() );
            }
            else
            {
                queue = operator.getQueue();
            }

            final TupleQueueDrainerPool drainerPool = ( multiThreaded && operatorDef.getInputPortCount() > 0 )
                                                      ? new BlockingTupleQueueDrainerPool( config, operatorDef )
                                                      : new NonBlockingTupleQueueDrainerPool( config, operatorDef );

            final Supplier<TuplesImpl> outputSupplier = ( i == newOperatorReplicas.length - 1 ) ? TuplesImplSupplierUtils.newInstance(
                    pipelineTailOperatorOutputSupplierClass, operatorDef.getInputPortCount() ) : operator.getOutputSupplier();

            newOperatorReplicas[ i ] = operator.duplicate( newPipelineReplicaId, queue, drainerPool, outputSupplier, replicaMeter );
        }

        final List<String> operatorIds = Arrays.stream( newOperatorReplicas ).map( o -> o.getOperatorDef().getId() )
                                               .collect( Collectors.toList() );

        LOGGER.info( "Duplicated pipeline {} to split. operators: {}", newPipelineReplicaId, operatorIds );
    }

    private List<Integer> getPipelineStartIndicesAfterSplit ( final RegionExecutionPlan regionExecutionPlan,
                                                              final List<Integer> startIndicesToSplit )
    {
        final int firstPipelineIndex = regionExecutionPlan.getPipelineIndex( startIndicesToSplit.get( 0 ) );
        final List<Integer> newPipelineStartIndices = new ArrayList<>( regionExecutionPlan.getPipelineStartIndices() );
        for ( int i = 1; i < startIndicesToSplit.size(); i++ )
        {
            newPipelineStartIndices.add( firstPipelineIndex + i, startIndicesToSplit.get( i ) );
        }
        return newPipelineStartIndices;
    }

}
