package cs.bilkent.joker.engine.region.impl;

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
import static cs.bilkent.joker.engine.region.impl.RegionExecutionPlanUtil.checkPipelineStartIndicesToSplit;
import cs.bilkent.joker.engine.tuplequeue.OperatorTupleQueue;
import cs.bilkent.joker.engine.tuplequeue.OperatorTupleQueueManager;
import cs.bilkent.joker.engine.tuplequeue.TupleQueueDrainerPool;
import cs.bilkent.joker.engine.tuplequeue.impl.drainer.GreedyDrainer;
import cs.bilkent.joker.engine.tuplequeue.impl.drainer.pool.BlockingTupleQueueDrainerPool;
import cs.bilkent.joker.engine.tuplequeue.impl.drainer.pool.NonBlockingTupleQueueDrainerPool;
import cs.bilkent.joker.engine.tuplequeue.impl.operator.DefaultOperatorTupleQueue;
import cs.bilkent.joker.engine.tuplequeue.impl.operator.EmptyOperatorTupleQueue;
import static cs.bilkent.joker.impl.com.google.common.base.Preconditions.checkArgument;
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

    private final OperatorTupleQueueManager operatorTupleQueueManager;

    @Inject
    public PipelineTransformerImpl ( final JokerConfig config, final OperatorTupleQueueManager operatorTupleQueueManager )
    {
        this.config = config;
        this.operatorTupleQueueManager = operatorTupleQueueManager;
    }

    @Override
    public Region mergePipelines ( final Region region, final List<Integer> pipelineStartIndicesToMerge )
    {
        final int regionId = region.getRegionId();

        final RegionExecutionPlan regionExecutionPlan = region.getExecutionPlan();
        LOGGER.info( "Merging pipelines: {} of regionId={} with pipeline start indices: {}", pipelineStartIndicesToMerge,
                     regionId,
                     regionExecutionPlan.getPipelineStartIndices() );

        final RegionExecutionPlan newRegionExecutionPlan = regionExecutionPlan.withMergedPipelines( pipelineStartIndicesToMerge );

        final int replicaCount = regionExecutionPlan.getReplicaCount();
        final int pipelineCountDecrease = pipelineStartIndicesToMerge.size() - 1;
        final int newPipelineCount = regionExecutionPlan.getPipelineCount() - pipelineCountDecrease;
        final PipelineReplica[][] newPipelineReplicas = new PipelineReplica[ newPipelineCount ][ replicaCount ];
        final int newOperatorCount = getMergedPipelineOperatorCount( regionExecutionPlan, pipelineStartIndicesToMerge );

        final int firstPipelineIndex = regionExecutionPlan.getPipelineIndex( pipelineStartIndicesToMerge.get( 0 ) );

        copyNonMergedPipelines( region, pipelineStartIndicesToMerge, newPipelineReplicas, pipelineCountDecrease );

        for ( int replicaIndex = 0; replicaIndex < replicaCount; replicaIndex++ )
        {
            final OperatorReplica[] newOperatorReplicas = new OperatorReplica[ newOperatorCount ];
            final PipelineReplica[] replicaPipelines = region.getReplicaPipelines( replicaIndex );
            final PipelineReplica firstPipelineReplica = replicaPipelines[ regionExecutionPlan.getPipelineIndex( pipelineStartIndicesToMerge
                                                                                                                         .get( 0 ) ) ];

            final PipelineReplicaId pipelineReplicaId = replicaPipelines[ firstPipelineIndex ].id();
            final PipelineReplicaMeter firstPipelineReplicaMeter = createPipelineReplicaMeter( pipelineReplicaId,
                                                                                               regionExecutionPlan,
                                                                                               pipelineStartIndicesToMerge );
            int operatorIndex = duplicateFirstPipelineToMergeOperators( firstPipelineReplica,
                                                                        newOperatorReplicas,
                                                                        firstPipelineReplicaMeter );

            for ( int i = 1; i < pipelineStartIndicesToMerge.size(); i++ )
            {
                final PipelineReplica pipelineReplica = replicaPipelines[ regionExecutionPlan.getPipelineIndex(
                        pipelineStartIndicesToMerge.get(
                        i ) ) ];

                operatorIndex = duplicateMergedPipelineOperators( regionId,
                                                                  replicaIndex,
                                                                  newOperatorReplicas,
                                                                  operatorIndex,
                                                                  pipelineReplicaId,
                                                                  firstPipelineReplicaMeter, pipelineReplica );
            }

            final List<String> operatorIds = Arrays.stream( newOperatorReplicas ).map( o -> o.getOperatorDef().getId() )
                                                   .collect( Collectors.toList() );

            LOGGER.debug( "Duplicating {} with new operators: {}", firstPipelineReplica.id(), operatorIds );

            newPipelineReplicas[ firstPipelineIndex ][ replicaIndex ] = firstPipelineReplica.duplicate( firstPipelineReplicaMeter,
                                                                                                        newOperatorReplicas );
        }

        LOGGER.info( "new pipeline start indices: {} after merge of pipelines: {} for regionId={}",
                     newRegionExecutionPlan.getPipelineStartIndices(),
                     pipelineStartIndicesToMerge,
                     regionId );

        return new Region( newRegionExecutionPlan, newPipelineReplicas );
    }

    private PipelineReplicaMeter createPipelineReplicaMeter ( final PipelineReplicaId pipelineReplicaId,
                                                              final RegionExecutionPlan regionExecutionPlan,
                                                              final List<Integer> startIndicesToMerge )
    {
        final OperatorDef[] firstPipeline = regionExecutionPlan.getOperatorDefsByPipelineStartIndex( startIndicesToMerge.get( 0 ) );
        return new PipelineReplicaMeter( config.getMetricManagerConfig().getTickMask(), pipelineReplicaId, firstPipeline[ 0 ] );
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
            LOGGER.debug( "copying non-merged pipelineIndex={} of replicaId={}", i, region.getRegionId() );
            arraycopy( region.getPipelineReplicas( i ), 0, newPipelineReplicas[ i ], 0, replicaCount );
        }

        final int after = 1 + currentStartIndices.indexOf( startIndicesToMerge.get( pipelineCountDecrease ) );
        for ( int i = after; i < currentStartIndices.size(); i++ )
        {
            final int j = i - pipelineCountDecrease;
            LOGGER.debug( "copying non-merged pipelineIndex={} to new pipelineIndex={} of replicaId={}", i, j, region.getRegionId() );
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
        final Supplier<TuplesImpl> outputSupplier = new CachedTuplesImplSupplier( lastOperator.getOperatorDef().getOutputPortCount() );
        newOperatorReplicas[ operatorIndex++ ] = lastOperator.duplicate( pipelineReplicaId,
                                                                         lastOperator.getQueue(),
                                                                         lastOperator.getDrainerPool(),
                                                                         outputSupplier,
                                                                         firstPipelineReplicaMeter );

        final List<String> operatorIds = Arrays.stream( pipelineReplica.getOperators() ).map( o -> o.getOperatorDef().getId() )
                                               .collect( Collectors.toList() );

        LOGGER.debug( "Duplicated first pipeline {} to merge. operators: {}", pipelineReplicaId, operatorIds );

        return operatorIndex;
    }

    private int duplicateMergedPipelineOperators ( final int regionId,
                                                   final int replicaIndex,
                                                   final OperatorReplica[] newOperatorReplicas,
                                                   int operatorIndex,
                                                   final PipelineReplicaId newPipelineReplicaId,
                                                   final PipelineReplicaMeter replicaMeter, final PipelineReplica pipelineReplica )
    {
        newOperatorReplicas[ operatorIndex++ ] = duplicateMergedPipelineFirstOperator( regionId,
                                                                                       replicaIndex,
                                                                                       newPipelineReplicaId,
                                                                                       replicaMeter, pipelineReplica );

        for ( int i = 1, j = pipelineReplica.getOperatorCount(); i < j; i++ )
        {
            final OperatorReplica operator = pipelineReplica.getOperator( i );
            final int outputPortCount = operator.getOperatorDef().getOutputPortCount();
            final Supplier<TuplesImpl> outputSupplier = new CachedTuplesImplSupplier( outputPortCount );

            newOperatorReplicas[ operatorIndex++ ] = operator.duplicate( newPipelineReplicaId,
                                                                         operator.getQueue(),
                                                                         operator.getDrainerPool(),
                                                                         outputSupplier,
                                                                         replicaMeter );
            LOGGER.debug( "Duplicating operator {} of {} to {}", operator.getOperatorDef().getId(),
                          pipelineReplica.id(),
                          newPipelineReplicaId );

        }

        return operatorIndex;
    }

    private OperatorReplica duplicateMergedPipelineFirstOperator ( final int regionId,
                                                                   final int replicaIndex,
                                                                   final PipelineReplicaId newPipelineReplicaId,
                                                                   final PipelineReplicaMeter replicaMeter,
                                                                   final PipelineReplica pipelineReplica )
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

            LOGGER.debug( "Drained queue of pipeline {} for merge.", pipelineReplica.id() );
            operatorTupleQueueManager.releaseDefaultOperatorTupleQueue( regionId, replicaIndex, firstOperatorDef.getId() );
        }

        final NonBlockingTupleQueueDrainerPool drainerPool = new NonBlockingTupleQueueDrainerPool( config, firstOperatorDef );
        final Supplier<TuplesImpl> outputSupplier = new CachedTuplesImplSupplier( firstOperatorDef.getOutputPortCount() );
        LOGGER.debug( "Duplicating first operator {} of {} to {}", firstOperatorDef.getId(), pipelineReplica.id(), newPipelineReplicaId );
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

    @Override
    public Region splitPipeline ( final Region region, final List<Integer> pipelineStartIndicesToSplit )
    {
        final int regionId = region.getRegionId();

        final RegionExecutionPlan regionExecutionPlan = region.getExecutionPlan();
        final RegionDef regionDef = regionExecutionPlan.getRegionDef();
        LOGGER.info( "Splitting pipelines: {} of regionId={} with pipeline start indices: {}", pipelineStartIndicesToSplit,
                     regionId,
                     regionExecutionPlan.getPipelineStartIndices() );

        checkArgument( checkPipelineStartIndicesToSplit( regionExecutionPlan, pipelineStartIndicesToSplit ),
                       "invalid pipeline start indices to split: %s current pipeline start indices: %s regionId=%s",
                       pipelineStartIndicesToSplit,
                       regionExecutionPlan.getPipelineStartIndices(),
                       regionId );

        final int replicaCount = regionExecutionPlan.getReplicaCount();
        final int pipelineCountIncrease = pipelineStartIndicesToSplit.size() - 1;
        final int newPipelineCount = regionExecutionPlan.getPipelineCount() + pipelineCountIncrease;
        final PipelineReplica[][] newPipelineReplicas = new PipelineReplica[ newPipelineCount ][ replicaCount ];
        final int firstPipelineIndex = regionExecutionPlan.getPipelineIndex( pipelineStartIndicesToSplit.get( 0 ) );

        copyNonSplitPipelines( region, pipelineStartIndicesToSplit, newPipelineReplicas, pipelineCountIncrease );

        for ( int replicaIndex = 0; replicaIndex < replicaCount; replicaIndex++ )
        {
            final PipelineReplica pipelineReplica = region.getReplicaPipelines( replicaIndex )[ firstPipelineIndex ];

            for ( int i = 0; i < pipelineStartIndicesToSplit.size(); i++ )
            {
                final int curr = pipelineStartIndicesToSplit.get( i );
                final int newOperatorCount;
                if ( i < pipelineStartIndicesToSplit.size() - 1 )
                {
                    newOperatorCount = pipelineStartIndicesToSplit.get( i + 1 ) - curr;
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

                final int startOperatorIndex = curr - pipelineStartIndicesToSplit.get( 0 );
                final PipelineReplicaMeter replicaMeter = new PipelineReplicaMeter( config.getMetricManagerConfig().getTickMask(),
                                                                                    newPipelineReplicaId,
                                                                                    pipelineReplica.getOperatorDef( startOperatorIndex ) );

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
                        LOGGER.debug( "Creating {} for pipeline tuple queue of regionId={} replicaIndex={} for pipeline operator={}",
                                      DefaultOperatorTupleQueue.class.getSimpleName(),
                                      regionId,
                                      replicaIndex,
                                      firstOperatorDef.getId() );
                        pipelineTupleQueue = operatorTupleQueueManager.createDefaultOperatorTupleQueue( regionId,
                                                                                                        replicaIndex,
                                                                                                        firstOperatorDef,
                                                                                                        MULTI_THREADED );
                    }
                    else
                    {
                        LOGGER.debug( "Creating {} for pipeline tuple queue of regionId={} replicaIndex={} as first operator is {}",
                                      EmptyOperatorTupleQueue.class.getSimpleName(),
                                      regionId,
                                      replicaIndex,
                                      firstOperatorDef.getOperatorType() );
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

                    LOGGER.debug( "Split pipeline {} to split with operators: {}", newPipelineReplicaId, operatorIds );
                }

                newPipelineReplicas[ firstPipelineIndex + i ][ replicaIndex ] = newPipelineReplica;
            }
        }

        final RegionExecutionPlan newRegionExecutionPlan = regionExecutionPlan.withSplitPipeline( pipelineStartIndicesToSplit );
        LOGGER.info( "new pipeline start indices: {} after split to pipelines: {} for regionId={}",
                     newRegionExecutionPlan.getPipelineStartIndices(),
                     pipelineStartIndicesToSplit,
                     regionId );

        return new Region( newRegionExecutionPlan, newPipelineReplicas );
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
            LOGGER.debug( "copying non-split pipelineIndex={} of replicaId={}", i, region.getRegionId() );
            arraycopy( region.getPipelineReplicas( i ), 0, newPipelineReplicas[ i ], 0, replicaCount );
        }

        final int after = 1 + currentStartIndices.indexOf( startIndicesToSplit.get( 0 ) );
        for ( int i = after; i < currentStartIndices.size(); i++ )
        {
            final int j = i + pipelineCountIncrease;
            LOGGER.debug( "copying non-split pipelineIndex={} to new pipelineIndex={} of replicaId={}", i, j, region.getRegionId() );
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
                                                                             newPipelineReplicaId.replicaIndex,
                                                                             operatorDef.getId() );
            }
            else
            {
                queue = operator.getQueue();
            }

            final TupleQueueDrainerPool drainerPool = ( multiThreaded && operatorDef.getInputPortCount() > 0 )
                                                      ? new BlockingTupleQueueDrainerPool( config, operatorDef )
                                                      : new NonBlockingTupleQueueDrainerPool( config, operatorDef );

            final Supplier<TuplesImpl> outputSupplier = operator.getOutputSupplier();

            newOperatorReplicas[ i ] = operator.duplicate( newPipelineReplicaId, queue, drainerPool, outputSupplier, replicaMeter );
        }

        final List<String> operatorIds = Arrays.stream( newOperatorReplicas ).map( o -> o.getOperatorDef().getId() )
                                               .collect( Collectors.toList() );

        LOGGER.debug( "Duplicated pipeline {} to split. operators: {}", newPipelineReplicaId, operatorIds );
    }

}
