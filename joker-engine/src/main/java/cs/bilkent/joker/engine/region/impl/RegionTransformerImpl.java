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
import cs.bilkent.joker.engine.pipeline.OperatorReplica;
import cs.bilkent.joker.engine.pipeline.PipelineReplica;
import cs.bilkent.joker.engine.pipeline.PipelineReplicaId;
import cs.bilkent.joker.engine.pipeline.impl.tuplesupplier.CachedTuplesImplSupplier;
import cs.bilkent.joker.engine.pipeline.impl.tuplesupplier.NonCachedTuplesImplSupplier;
import cs.bilkent.joker.engine.region.Region;
import cs.bilkent.joker.engine.region.RegionConfig;
import cs.bilkent.joker.engine.region.RegionDef;
import cs.bilkent.joker.engine.region.RegionTransformer;
import cs.bilkent.joker.engine.tuplequeue.TupleQueueContext;
import cs.bilkent.joker.engine.tuplequeue.TupleQueueContextManager;
import cs.bilkent.joker.engine.tuplequeue.impl.context.DefaultTupleQueueContext;
import cs.bilkent.joker.engine.tuplequeue.impl.context.EmptyTupleQueueContext;
import cs.bilkent.joker.engine.tuplequeue.impl.drainer.GreedyDrainer;
import cs.bilkent.joker.engine.tuplequeue.impl.drainer.pool.NonBlockingTupleQueueDrainerPool;
import cs.bilkent.joker.operator.OperatorDef;
import cs.bilkent.joker.operator.impl.TuplesImpl;
import static java.lang.System.arraycopy;

@Singleton
@NotThreadSafe
public class RegionTransformerImpl implements RegionTransformer
{

    private static final Logger LOGGER = LoggerFactory.getLogger( RegionTransformerImpl.class );


    private final JokerConfig config;

    private final TupleQueueContextManager tupleQueueContextManager;

    @Inject
    public RegionTransformerImpl ( final JokerConfig config, final TupleQueueContextManager tupleQueueContextManager )
    {
        this.config = config;
        this.tupleQueueContextManager = tupleQueueContextManager;
    }

    @Override
    public Region mergePipelines ( final Region region, final List<Integer> startIndicesToMerge )
    {
        final int regionId = region.getRegionId();

        final RegionConfig regionConfig = region.getConfig();
        final RegionDef regionDef = regionConfig.getRegionDef();
        LOGGER.info( "Merging pipelines: {} of regionId={} with pipeline start indices: {}",
                     startIndicesToMerge,
                     regionId,
                     regionConfig.getPipelineStartIndices() );
        if ( !checkPipelineStartIndicesToMerge( regionConfig, startIndicesToMerge ) )
        {
            throw new IllegalArgumentException( "invalid pipeline start indices to merge: " + startIndicesToMerge
                                                + " current pipeline start indices: " + regionConfig.getPipelineStartIndices()
                                                + " regionId=" + regionId );
        }

        final int replicaCount = regionConfig.getReplicaCount();
        final int pipelineCountDecrease = startIndicesToMerge.size() - 1;
        final int newPipelineCount = regionConfig.getPipelineCount() - pipelineCountDecrease;
        final PipelineReplica[][] newPipelineReplicas = new PipelineReplica[ newPipelineCount ][ replicaCount ];
        final int newOperatorCount = getMergedPipelineOperatorCount( regionConfig, startIndicesToMerge );
        final int firstPipelineIndex = regionConfig.getPipelineIndex( startIndicesToMerge.get( 0 ) );

        copyNonMergedPipelines( region, startIndicesToMerge, newPipelineReplicas, pipelineCountDecrease );

        for ( int replicaIndex = 0; replicaIndex < replicaCount; replicaIndex++ )
        {
            final OperatorReplica[] newOperatorReplicas = new OperatorReplica[ newOperatorCount ];
            final PipelineReplica[] replicaPipelines = region.getReplicaPipelines( replicaIndex );
            final PipelineReplica firstPipelineReplica = replicaPipelines[ regionConfig.getPipelineIndex( startIndicesToMerge.get( 0 ) ) ];
            int operatorIndex = duplicateFirstPipeline( firstPipelineReplica, newOperatorReplicas );

            final PipelineReplicaId pipelineReplicaId = replicaPipelines[ firstPipelineIndex ].id();

            for ( int i = 1; i < startIndicesToMerge.size(); i++ )
            {
                final PipelineReplica pipelineReplica = replicaPipelines[ regionConfig.getPipelineIndex( startIndicesToMerge.get( i ) ) ];
                final boolean isLastMergedPipeline = i == ( startIndicesToMerge.size() - 1 );

                operatorIndex = duplicateMergedPipeline( regionId,
                                                         replicaIndex,
                                                         newOperatorReplicas,
                                                         operatorIndex,
                                                         pipelineReplicaId,
                                                         pipelineReplica,
                                                         isLastMergedPipeline );
            }

            final List<String> operatorIds = Arrays.stream( newOperatorReplicas )
                                                   .map( o -> o.getOperatorDef().id() )
                                                   .collect( Collectors.toList() );

            LOGGER.info( "Duplicating {} with new operators: {}", firstPipelineReplica.id(), operatorIds );

            newPipelineReplicas[ firstPipelineIndex ][ replicaIndex ] = firstPipelineReplica.duplicate( newOperatorReplicas );
        }

        final List<Integer> newPipelineStartIndices = getPipelineStartIndicesAfterMerge( regionConfig.getPipelineStartIndices(),
                                                                                         startIndicesToMerge );
        final RegionConfig newRegionConfig = new RegionConfig( regionDef, newPipelineStartIndices, replicaCount );
        LOGGER.info( "new pipeline start indices: {} after merge of pipelines: {} for regionId={}",
                     newPipelineStartIndices,
                     startIndicesToMerge,
                     regionId );
        return new Region( newRegionConfig, newPipelineReplicas );
    }

    @Override
    public boolean checkPipelineStartIndicesToMerge ( final RegionConfig regionConfig, final List<Integer> pipelineStartIndicesToMerge )
    {
        if ( pipelineStartIndicesToMerge.size() < 2 )
        {
            return false;
        }

        final List<Integer> pipelineStartIndices = regionConfig.getPipelineStartIndices();

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

    private void copyNonMergedPipelines ( final Region region,
                                          final List<Integer> startIndicesToMerge,
                                          final PipelineReplica[][] newPipelineReplicas,
                                          final int pipelineCountDecrease )
    {
        final RegionConfig config = region.getConfig();
        final List<Integer> currentStartIndices = config.getPipelineStartIndices();
        final int replicaCount = config.getReplicaCount();
        final int beforeMergeIndex = currentStartIndices.indexOf( startIndicesToMerge.get( 0 ) );
        for ( int i = 0; i < beforeMergeIndex; i++ )
        {
            LOGGER.info( "copying non-merged pipelineIndex={} of replicaId={}", i, region.getRegionId() );
            arraycopy( region.getPipelineReplicas( i ), 0, newPipelineReplicas[ i ], 0, replicaCount );
        }

        final int afterMergeIndex = 1 + currentStartIndices.indexOf( startIndicesToMerge.get( pipelineCountDecrease ) );
        for ( int i = afterMergeIndex; i < currentStartIndices.size(); i++ )
        {
            final int j = i - pipelineCountDecrease;
            LOGGER.info( "copying non-merged pipelineIndex={} to new pipelineIndex={} of replicaId={}", i, j, region.getRegionId() );
            arraycopy( region.getPipelineReplicas( i ), 0, newPipelineReplicas[ j ], 0, replicaCount );
        }
    }

    private int getMergedPipelineOperatorCount ( final RegionConfig regionConfig, final List<Integer> startIndicesToMerge )
    {
        int newOperatorCount = 0;
        for ( int startIndex : startIndicesToMerge )
        {
            newOperatorCount += regionConfig.getOperatorCountByPipelineId( startIndex );
        }
        return newOperatorCount;
    }

    private int duplicateFirstPipeline ( final PipelineReplica pipelineReplica, final OperatorReplica[] newOperatorReplicas )
    {
        int operatorIndex = 0;
        final PipelineReplicaId pipelineReplicaId = pipelineReplica.id();

        for ( int i = 0, j = pipelineReplica.getOperatorCount() - 1; i < j; i++ )
        {
            newOperatorReplicas[ operatorIndex++ ] = pipelineReplica.getOperator( i );
        }

        final OperatorReplica lastOperator = pipelineReplica.getOperator( pipelineReplica.getOperatorCount() - 1 );
        final Supplier<TuplesImpl> outputSupplier = new CachedTuplesImplSupplier( lastOperator.getOperatorDef().inputPortCount() );
        newOperatorReplicas[ operatorIndex++ ] = lastOperator.duplicate( pipelineReplicaId,
                                                                         lastOperator.getQueue(),
                                                                         lastOperator.getDrainerPool(),
                                                                         outputSupplier );

        final List<String> operatorIds = Arrays.stream( pipelineReplica.getOperators() )
                                               .map( o -> o.getOperatorDef().id() )
                                               .collect( Collectors.toList() );

        LOGGER.info( "Duplicated first pipeline {}. operators: {}", pipelineReplicaId, operatorIds );

        return operatorIndex;
    }

    private int duplicateMergedPipeline ( final int regionId,
                                          final int replicaIndex,
                                          final OperatorReplica[] newOperatorReplicas,
                                          int operatorIndex,
                                          final PipelineReplicaId newPipelineReplicaId,
                                          final PipelineReplica pipelineReplica,
                                          final boolean isLastMergedPipeline )
    {
        newOperatorReplicas[ operatorIndex++ ] = duplicateMergedPipelineFirstOperator( regionId,
                                                                                       replicaIndex,
                                                                                       newPipelineReplicaId,
                                                                                       pipelineReplica,
                                                                                       isLastMergedPipeline );

        for ( int i = 1, j = pipelineReplica.getOperatorCount(); i < j; i++ )
        {
            final OperatorReplica operator = pipelineReplica.getOperator( i );
            final Supplier<TuplesImpl> outputSupplier = ( ( i == ( j - 1 ) ) && isLastMergedPipeline )
                                                        ? new NonCachedTuplesImplSupplier( operator.getOperatorDef().inputPortCount() )
                                                        : new CachedTuplesImplSupplier( operator.getOperatorDef().inputPortCount() );

            newOperatorReplicas[ operatorIndex++ ] = operator.duplicate( newPipelineReplicaId,
                                                                         operator.getQueue(),
                                                                         operator.getDrainerPool(),
                                                                         outputSupplier );
            LOGGER.info( "Duplicating operator {} of {} to {}",
                         operator.getOperatorDef().id(),
                         pipelineReplica.id(),
                         newPipelineReplicaId );

        }

        return operatorIndex;
    }

    private OperatorReplica duplicateMergedPipelineFirstOperator ( final int regionId,
                                                                   final int replicaIndex,
                                                                   final PipelineReplicaId newPipelineReplicaId,
                                                                   final PipelineReplica pipelineReplica,
                                                                   final boolean isLastMergedPipeline )
    {
        final TupleQueueContext pipelineUpstreamTupleQueue = pipelineReplica.getSelfUpstreamTupleQueueContext();
        final OperatorReplica firstOperator = pipelineReplica.getOperator( 0 );
        final TupleQueueContext firstOperatorQueue;
        final OperatorDef firstOperatorDef = firstOperator.getOperatorDef();
        if ( pipelineUpstreamTupleQueue instanceof EmptyTupleQueueContext )
        {
            if ( firstOperator.getQueue() instanceof DefaultTupleQueueContext )
            {
                final String operatorId = firstOperatorDef.id();
                firstOperatorQueue = tupleQueueContextManager.convertToSingleThreaded( regionId, replicaIndex, operatorId );
            }
            else
            {
                // TODO
                throw new IllegalStateException();
            }
        }
        else
        {
            firstOperatorQueue = firstOperator.getQueue();
            final GreedyDrainer drainer = new GreedyDrainer( firstOperatorDef.inputPortCount() );
            pipelineUpstreamTupleQueue.drain( drainer );
            final TuplesImpl result = drainer.getResult();
            if ( result != null && result.isNonEmpty() )
            {
                for ( int portIndex = 0; portIndex < result.getPortCount(); portIndex++ )
                {
                    firstOperatorQueue.offer( portIndex, result.getTuples( portIndex ) );
                }
            }
            LOGGER.info( "Drained queue of pipeline {} for merge.", pipelineReplica.id() );
        }

        final NonBlockingTupleQueueDrainerPool drainerPool = new NonBlockingTupleQueueDrainerPool( config, firstOperatorDef );
        final Supplier<TuplesImpl> outputSupplier = ( isLastMergedPipeline && ( pipelineReplica.getOperatorCount() == 1 ) )
                                                    ? new NonCachedTuplesImplSupplier( firstOperatorDef.inputPortCount() )
                                                    : new CachedTuplesImplSupplier( firstOperatorDef.inputPortCount() );
        LOGGER.info( "Duplicating first operator {} of {} to {}", firstOperatorDef.id(), pipelineReplica.id(), newPipelineReplicaId );
        return firstOperator.duplicate( newPipelineReplicaId, firstOperatorQueue, drainerPool, outputSupplier );
    }

}
