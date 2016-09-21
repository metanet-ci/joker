package cs.bilkent.joker.engine.tuplequeue.impl;


import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;
import javax.annotation.concurrent.NotThreadSafe;
import javax.inject.Inject;
import javax.inject.Singleton;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import cs.bilkent.joker.engine.config.JokerConfig;
import cs.bilkent.joker.engine.config.ThreadingPreference;
import static cs.bilkent.joker.engine.config.ThreadingPreference.MULTI_THREADED;
import static cs.bilkent.joker.engine.config.ThreadingPreference.SINGLE_THREADED;
import cs.bilkent.joker.engine.config.TupleQueueManagerConfig;
import cs.bilkent.joker.engine.partition.PartitionKeyFunction;
import cs.bilkent.joker.engine.partition.PartitionKeyFunctionFactory;
import cs.bilkent.joker.engine.partition.PartitionService;
import cs.bilkent.joker.engine.tuplequeue.TupleQueue;
import cs.bilkent.joker.engine.tuplequeue.TupleQueueContext;
import cs.bilkent.joker.engine.tuplequeue.TupleQueueContextManager;
import cs.bilkent.joker.engine.tuplequeue.impl.context.DefaultTupleQueueContext;
import cs.bilkent.joker.engine.tuplequeue.impl.context.PartitionedTupleQueueContext;
import cs.bilkent.joker.engine.tuplequeue.impl.queue.MultiThreadedTupleQueue;
import cs.bilkent.joker.engine.tuplequeue.impl.queue.SingleThreadedTupleQueue;
import cs.bilkent.joker.operator.OperatorDef;
import static cs.bilkent.joker.operator.spec.OperatorType.PARTITIONED_STATEFUL;
import cs.bilkent.joker.utils.Pair;
import cs.bilkent.joker.utils.Triple;

@Singleton
@NotThreadSafe
public class TupleQueueContextManagerImpl implements TupleQueueContextManager
{

    private static final Logger LOGGER = LoggerFactory.getLogger( TupleQueueContextManagerImpl.class );


    private final PartitionService partitionService;

    private final PartitionKeyFunctionFactory partitionKeyFunctionFactory;

    private final TupleQueueManagerConfig tupleQueueManagerConfig;

    private final Map<Triple<Integer, Integer, String>, DefaultTupleQueueContext> singleTupleQueueContexts = new HashMap<>();

    private final Map<Pair<Integer, String>, PartitionedTupleQueueContext[]> partitionedTupleQueueContexts = new HashMap<>();

    private final Map<String, TupleQueueContainer[]> tupleQueueContainersByOperatorId = new HashMap<>();


    @Inject
    public TupleQueueContextManagerImpl ( final JokerConfig jokerConfig,
                                          final PartitionService partitionService,
                                          final PartitionKeyFunctionFactory partitionKeyFunctionFactory )
    {
        this.partitionService = partitionService;
        this.partitionKeyFunctionFactory = partitionKeyFunctionFactory;
        this.tupleQueueManagerConfig = jokerConfig.getTupleQueueManagerConfig();
    }

    @Override
    public TupleQueueContext createDefaultTupleQueueContext ( final int regionId,
                                                              final int replicaIndex,
                                                              final OperatorDef operatorDef,
                                                              final ThreadingPreference threadingPreference )
    {
        checkArgument( operatorDef != null, "No operator definition! regionId %s, replicaIndex %s", regionId, replicaIndex );
        checkArgument( threadingPreference != null,
                       "No threading preference is given! regionId %s, replicaIndex %s operatorId %s",
                       regionId,
                       replicaIndex,
                       operatorDef.id() );
        checkArgument( operatorDef.operatorType() != PARTITIONED_STATEFUL || threadingPreference == MULTI_THREADED,
                       "invalid <operator type, threading preference> pair! regionId %s operatorId %s operatorType %s threadingPreference"
                       + " %s ",
                       regionId,
                       operatorDef.id(),
                       operatorDef.operatorType(),
                       threadingPreference );
        checkArgument( replicaIndex >= 0,
                       "invalid replica index! regionId %s, replicaIndex %s operatorId %s",
                       regionId,
                       replicaIndex,
                       operatorDef.id() );

        final BiFunction<Integer, Boolean, TupleQueue> tupleQueueConstructor = getTupleQueueConstructor( threadingPreference );
        final String operatorId = operatorDef.id();
        final int inputPortCount = operatorDef.inputPortCount();

        final Function<Triple<Integer, Integer, String>, DefaultTupleQueueContext> c = t ->
        {
            LOGGER.info( "created single tuple queue context for regionId={} replicaIndex={} operatorId={}",
                         regionId,
                         replicaIndex,
                         operatorId );
            return new DefaultTupleQueueContext( operatorId,
                                                 inputPortCount,
                                                 threadingPreference,
                                                 tupleQueueConstructor,
                                                 tupleQueueManagerConfig.getMaxSingleThreadedTupleQueueSize() );
        };

        return singleTupleQueueContexts.computeIfAbsent( Triple.of( regionId, replicaIndex, operatorId ), c );
    }

    public TupleQueueContext getDefaultTupleQueueContext ( final int regionId, final int replicaIndex, final OperatorDef operatorDef )
    {
        return singleTupleQueueContexts.get( Triple.of( regionId, replicaIndex, operatorDef.id() ) );
    }

    @Override
    public PartitionedTupleQueueContext[] createPartitionedTupleQueueContext ( final int regionId,
                                                                               final int replicaCount,
                                                                               final OperatorDef operatorDef )
    {
        checkArgument( operatorDef != null, "No operator definition! regionId %s, replicaCount %s", regionId, replicaCount );
        checkArgument( operatorDef.operatorType() == PARTITIONED_STATEFUL,
                       "invalid operator type: %s ! regionId %s operatorId %s",
                       regionId,
                       operatorDef.id() );
        checkArgument( replicaCount > 0, "invalid replica count %s ! regionId %s operatorId %s", replicaCount, regionId, operatorDef.id() );
        final BiFunction<Integer, Boolean, TupleQueue> tupleQueueConstructor = getTupleQueueConstructor( SINGLE_THREADED );
        final String operatorId = operatorDef.id();
        final int inputPortCount = operatorDef.inputPortCount();

        final Function<Pair<Integer, String>, PartitionedTupleQueueContext[]> c = p ->
        {
            final PartitionedTupleQueueContext[] tupleQueueContexts = new PartitionedTupleQueueContext[ replicaCount ];
            for ( int replicaIndex = 0; replicaIndex < replicaCount; replicaIndex++ )
            {
                final TupleQueueContainer[] containers = getOrCreateTupleQueueContainers( operatorId,
                                                                                          inputPortCount,
                                                                                          partitionService.getPartitionCount(),
                                                                                          tupleQueueConstructor );
                final int[] partitions = partitionService.getOrCreatePartitionDistribution( regionId, replicaCount );
                final PartitionKeyFunction partitionKeyExtractor = partitionKeyFunctionFactory.createPartitionKeyFunction( operatorDef
                                                                                                                                   .partitionFieldNames() );
                tupleQueueContexts[ replicaIndex ] = new PartitionedTupleQueueContext( operatorId,
                                                                                       inputPortCount,
                                                                                       partitionService.getPartitionCount(),
                                                                                       replicaIndex,
                                                                                       partitionKeyExtractor,
                                                                                       containers,
                                                                                       partitions,
                                                                                       tupleQueueManagerConfig.getMaxDrainableKeyCount() );
            }

            LOGGER.info( "created partitioned tuple queue context for regionId={} replicaCount={} operatorId={}",
                         regionId,
                         replicaCount,
                         operatorId );
            return tupleQueueContexts;
        };

        return partitionedTupleQueueContexts.computeIfAbsent( Pair.of( regionId, operatorId ), c );

    }

    public PartitionedTupleQueueContext[] getPartitionedTupleQueueContexts ( final int regionId, final OperatorDef operatorDef )
    {
        return partitionedTupleQueueContexts.get( Pair.of( regionId, operatorDef.id() ) );
    }

    private BiFunction<Integer, Boolean, TupleQueue> getTupleQueueConstructor ( final ThreadingPreference threadingPreference )
    {
        return threadingPreference == SINGLE_THREADED
               ? ( portIndex, capacityCheckEnabled ) -> new SingleThreadedTupleQueue( tupleQueueManagerConfig.getTupleQueueInitialSize() )
               : ( portIndex, capacityCheckEnabled ) -> new MultiThreadedTupleQueue( tupleQueueManagerConfig.getTupleQueueInitialSize(),
                                                                                     capacityCheckEnabled );
    }

    @Override
    public boolean releaseDefaultTupleQueueContext ( final int regionId, final int replicaIndex, final String operatorId )
    {
        final TupleQueueContext tupleQueueContext = singleTupleQueueContexts.remove( Triple.of( regionId, replicaIndex, operatorId ) );
        if ( tupleQueueContext != null )
        {
            tupleQueueContext.clear();
            return true;
        }

        LOGGER.warn( "no single tuple queue context to release for regionId={} replicaIndex={} operatorId={}",
                     regionId,
                     replicaIndex,
                     operatorId );
        return false;
    }

    @Override
    public boolean releasePartitionedTupleQueueContexts ( final int regionId, final String operatorId )
    {
        final TupleQueueContext[] tupleQueueContexts = partitionedTupleQueueContexts.remove( Pair.of( regionId, operatorId ) );
        if ( tupleQueueContexts != null )
        {
            releaseTupleQueueContainers( operatorId );
            return true;
        }

        LOGGER.warn( "no partitioned tuple queue context to release for regionId={} operatorId={}", regionId, operatorId );
        return false;
    }

    @Override
    public TupleQueueContext convertToSingleThreaded ( final int regionId, final int replicaIndex, final String operatorId )
    {
        final Triple<Integer, Integer, String> tupleQueueId = Triple.of( regionId, replicaIndex, operatorId );
        final DefaultTupleQueueContext tupleQueueContext = singleTupleQueueContexts.remove( tupleQueueId );
        checkArgument( tupleQueueContext != null,
                       "No tuple queue context found to convert to single threaded for regionId=%s replicaIndex=%s operatorId=%s",
                       regionId,
                       replicaIndex,
                       operatorId );
        checkState( tupleQueueContext.getThreadingPreference() == MULTI_THREADED,
                    "tuple queue context is not %s for regionId=%s replicaIndex=%s operatorId=%s",
                    MULTI_THREADED,
                    regionId,
                    replicaIndex,
                    operatorId );

        final BiFunction<Integer, Boolean, TupleQueue> tupleQueueConstructor = ( portIndex, capacityCheckEnabled ) ->
        {
            final TupleQueue q = tupleQueueContext.getTupleQueue( portIndex );
            final MultiThreadedTupleQueue tupleQueue = (MultiThreadedTupleQueue) q;
            return tupleQueue.toSingleThreadedTupleQueue();
        };

        final DefaultTupleQueueContext newTupleQueueContext = new DefaultTupleQueueContext( operatorId,
                                                                                            tupleQueueContext.getInputPortCount(),
                                                                                            SINGLE_THREADED,
                                                                                            tupleQueueConstructor,
                                                                                            tupleQueueManagerConfig
                                                                                                    .getMaxSingleThreadedTupleQueueSize() );

        singleTupleQueueContexts.put( tupleQueueId, newTupleQueueContext );
        LOGGER.info( "{} default tuple queue context is converted to {} for regionId={} replicaIndex={} operatorId={}",
                     MULTI_THREADED,
                     SINGLE_THREADED,
                     regionId,
                     replicaIndex,
                     operatorId );

        return newTupleQueueContext;
    }

    private TupleQueueContainer[] getOrCreateTupleQueueContainers ( final String operatorId,
                                                                    final int inputPortCount,
                                                                    final int partitionCount,
                                                                    final BiFunction<Integer, Boolean, TupleQueue> tupleQueueConstructor )
    {
        return tupleQueueContainersByOperatorId.computeIfAbsent( operatorId, s ->
        {
            final TupleQueueContainer[] containers = new TupleQueueContainer[ partitionCount ];
            for ( int i = 0; i < partitionCount; i++ )
            {
                containers[ i ] = new TupleQueueContainer( operatorId, inputPortCount, i, tupleQueueConstructor );
            }

            return containers;
        } );
    }

    private boolean releaseTupleQueueContainers ( final String operatorId )
    {
        final TupleQueueContainer[] tupleQueueContainers = tupleQueueContainersByOperatorId.remove( operatorId );

        if ( tupleQueueContainers != null )
        {
            LOGGER.info( "Releasing tuple queue containers of operator {}", operatorId );
            for ( TupleQueueContainer container : tupleQueueContainers )
            {
                container.clear();
            }

            return true;
        }

        LOGGER.error( "no tuple queue containers are found for operator {}.", operatorId );
        return false;
    }

}
