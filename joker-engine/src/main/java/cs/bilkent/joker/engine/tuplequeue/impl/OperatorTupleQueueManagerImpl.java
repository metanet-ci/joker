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
import cs.bilkent.joker.engine.config.JokerConfig;
import cs.bilkent.joker.engine.config.ThreadingPreference;
import static cs.bilkent.joker.engine.config.ThreadingPreference.MULTI_THREADED;
import static cs.bilkent.joker.engine.config.ThreadingPreference.SINGLE_THREADED;
import cs.bilkent.joker.engine.config.TupleQueueManagerConfig;
import cs.bilkent.joker.engine.partition.PartitionKeyExtractor;
import cs.bilkent.joker.engine.partition.PartitionKeyExtractorFactory;
import cs.bilkent.joker.engine.partition.PartitionService;
import cs.bilkent.joker.engine.tuplequeue.OperatorTupleQueue;
import cs.bilkent.joker.engine.tuplequeue.OperatorTupleQueueManager;
import cs.bilkent.joker.engine.tuplequeue.TupleQueue;
import cs.bilkent.joker.engine.tuplequeue.impl.context.DefaultOperatorTupleQueue;
import cs.bilkent.joker.engine.tuplequeue.impl.context.PartitionedOperatorTupleQueue;
import cs.bilkent.joker.engine.tuplequeue.impl.queue.MultiThreadedTupleQueue;
import cs.bilkent.joker.engine.tuplequeue.impl.queue.SingleThreadedTupleQueue;
import cs.bilkent.joker.operator.OperatorDef;
import static cs.bilkent.joker.operator.spec.OperatorType.PARTITIONED_STATEFUL;
import cs.bilkent.joker.utils.Pair;
import cs.bilkent.joker.utils.Triple;

@Singleton
@NotThreadSafe
public class OperatorTupleQueueManagerImpl implements OperatorTupleQueueManager
{

    private static final Logger LOGGER = LoggerFactory.getLogger( OperatorTupleQueueManagerImpl.class );


    private final PartitionService partitionService;

    private final PartitionKeyExtractorFactory partitionKeyExtractorFactory;

    private final TupleQueueManagerConfig tupleQueueManagerConfig;

    private final Map<Triple<Integer, Integer, String>, DefaultOperatorTupleQueue> singleOperatorTupleQueues = new HashMap<>();

    private final Map<Pair<Integer, String>, PartitionedOperatorTupleQueue[]> partitionedOperatorTupleQueues = new HashMap<>();

    private final Map<String, TupleQueueContainer[]> tupleQueueContainersByOperatorId = new HashMap<>();


    @Inject
    public OperatorTupleQueueManagerImpl ( final JokerConfig jokerConfig,
                                           final PartitionService partitionService,
                                           final PartitionKeyExtractorFactory partitionKeyExtractorFactory )
    {
        this.partitionService = partitionService;
        this.partitionKeyExtractorFactory = partitionKeyExtractorFactory;
        this.tupleQueueManagerConfig = jokerConfig.getTupleQueueManagerConfig();
    }

    @Override
    public OperatorTupleQueue createDefaultOperatorTupleQueue ( final int regionId,
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

        final Function<Triple<Integer, Integer, String>, DefaultOperatorTupleQueue> c = t ->
        {
            LOGGER.info( "created default tuple queue for regionId={} replicaIndex={} operatorId={}",
                         regionId,
                         replicaIndex,
                         operatorId );
            return new DefaultOperatorTupleQueue( operatorId,
                                                  inputPortCount,
                                                  threadingPreference,
                                                  tupleQueueConstructor,
                                                  tupleQueueManagerConfig.getMaxSingleThreadedTupleQueueSize() );
        };

        return singleOperatorTupleQueues.computeIfAbsent( Triple.of( regionId, replicaIndex, operatorId ), c );
    }

    public OperatorTupleQueue getDefaultOperatorTupleQueue ( final int regionId, final int replicaIndex, final OperatorDef operatorDef )
    {
        return singleOperatorTupleQueues.get( Triple.of( regionId, replicaIndex, operatorDef.id() ) );
    }

    @Override
    public PartitionedOperatorTupleQueue[] createPartitionedOperatorTupleQueue ( final int regionId,
                                                                                 final int replicaCount,
                                                                                 final OperatorDef operatorDef,
                                                                                 final int forwardKeyLimit )
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

        final Function<Pair<Integer, String>, PartitionedOperatorTupleQueue[]> c = p ->
        {
            final PartitionedOperatorTupleQueue[] operatorTupleQueues = new PartitionedOperatorTupleQueue[ replicaCount ];
            final PartitionKeyExtractor partitionKeyExtractor = partitionKeyExtractorFactory.createPartitionKeyExtractor( operatorDef
                                                                                                                                  .partitionFieldNames(),
                                                                                                                          forwardKeyLimit );

            for ( int replicaIndex = 0; replicaIndex < replicaCount; replicaIndex++ )
            {
                final TupleQueueContainer[] containers = getOrCreateTupleQueueContainers( operatorId,
                                                                                          inputPortCount,
                                                                                          partitionService.getPartitionCount(),
                                                                                          tupleQueueConstructor );
                final int[] partitions = partitionService.getOrCreatePartitionDistribution( regionId, replicaCount );

                operatorTupleQueues[ replicaIndex ] = new PartitionedOperatorTupleQueue( operatorId,
                                                                                         inputPortCount,
                                                                                         partitionService.getPartitionCount(),
                                                                                         replicaIndex,
                                                                                         partitionKeyExtractor,
                                                                                         containers,
                                                                                         partitions,
                                                                                         tupleQueueManagerConfig.getMaxDrainableKeyCount() );
            }

            LOGGER.info( "created partitioned tuple queue with partition key extractor {} for regionId={} replicaCount={} operatorId={}",
                         partitionKeyExtractor.getClass().getSimpleName(),
                         regionId,
                         replicaCount,
                         operatorId );
            return operatorTupleQueues;
        };

        return partitionedOperatorTupleQueues.computeIfAbsent( Pair.of( regionId, operatorId ), c );

    }

    public PartitionedOperatorTupleQueue[] getPartitionedOperatorTupleQueues ( final int regionId, final OperatorDef operatorDef )
    {
        return partitionedOperatorTupleQueues.get( Pair.of( regionId, operatorDef.id() ) );
    }

    private BiFunction<Integer, Boolean, TupleQueue> getTupleQueueConstructor ( final ThreadingPreference threadingPreference )
    {
        return threadingPreference == SINGLE_THREADED
               ? ( portIndex, capacityCheckEnabled ) -> new SingleThreadedTupleQueue( tupleQueueManagerConfig.getTupleQueueInitialSize() )
               : ( portIndex, capacityCheckEnabled ) -> new MultiThreadedTupleQueue( tupleQueueManagerConfig.getTupleQueueInitialSize(),
                                                                                     capacityCheckEnabled );
    }

    @Override
    public boolean releaseDefaultOperatorTupleQueue ( final int regionId, final int replicaIndex, final String operatorId )
    {
        final OperatorTupleQueue operatorTupleQueue = singleOperatorTupleQueues.remove( Triple.of( regionId, replicaIndex, operatorId ) );
        if ( operatorTupleQueue != null )
        {
            operatorTupleQueue.clear();
            return true;
        }

        LOGGER.warn( "no default tuple queue to release for regionId={} replicaIndex={} operatorId={}",
                     regionId,
                     replicaIndex,
                     operatorId );
        return false;
    }

    @Override
    public boolean releasePartitionedOperatorTupleQueue ( final int regionId, final String operatorId )
    {
        final OperatorTupleQueue[] operatorTupleQueues = partitionedOperatorTupleQueues.remove( Pair.of( regionId, operatorId ) );
        if ( operatorTupleQueues != null )
        {
            releaseTupleQueueContainers( operatorId );
            return true;
        }

        LOGGER.warn( "no partitioned tuple queue to release for regionId={} operatorId={}", regionId, operatorId );
        return false;
    }

    @Override
    public OperatorTupleQueue switchThreadingPreference ( final int regionId, final int replicaIndex, final String operatorId )
    {
        final Triple<Integer, Integer, String> tupleQueueId = Triple.of( regionId, replicaIndex, operatorId );
        final DefaultOperatorTupleQueue operatorTupleQueue = singleOperatorTupleQueues.remove( tupleQueueId );
        checkArgument( operatorTupleQueue != null,
                       "No tuple queue found to convert to single threaded for regionId=%s replicaIndex=%s operatorId=%s",
                       regionId,
                       replicaIndex,
                       operatorId );

        final ThreadingPreference threadingPreference = operatorTupleQueue.getThreadingPreference(), newThreadingPreference;

        final BiFunction<Integer, Boolean, TupleQueue> tupleQueueConstructor;

        if ( threadingPreference == MULTI_THREADED )
        {
            newThreadingPreference = SINGLE_THREADED;
            tupleQueueConstructor = ( portIndex, capacityCheckEnabled ) ->
            {
                final TupleQueue q = operatorTupleQueue.getTupleQueue( portIndex );
                final MultiThreadedTupleQueue tupleQueue = (MultiThreadedTupleQueue) q;
                return tupleQueue.toSingleThreadedTupleQueue();
            };
        }
        else if ( threadingPreference == SINGLE_THREADED )
        {
            newThreadingPreference = MULTI_THREADED;
            tupleQueueConstructor = ( portIndex, capacityCheckEnabled ) ->
            {
                final TupleQueue q = operatorTupleQueue.getTupleQueue( portIndex );
                final SingleThreadedTupleQueue tupleQueue = (SingleThreadedTupleQueue) q;
                return tupleQueue.toMultiThreadedTupleQueue( tupleQueueManagerConfig.getTupleQueueInitialSize() );
            };
        }
        else
        {
            throw new IllegalStateException( "regionId=" + regionId + " has invalid threading preference: " + threadingPreference );
        }

        final DefaultOperatorTupleQueue newOperatorTupleQueue = new DefaultOperatorTupleQueue( operatorId,
                                                                                               operatorTupleQueue.getInputPortCount(),
                                                                                               newThreadingPreference,
                                                                                               tupleQueueConstructor,
                                                                                               tupleQueueManagerConfig
                                                                                                    .getMaxSingleThreadedTupleQueueSize() );

        singleOperatorTupleQueues.put( tupleQueueId, newOperatorTupleQueue );
        LOGGER.info( "{} default tuple queue is switched to {} for regionId={} replicaIndex={} operatorId={}",
                     threadingPreference,
                     newThreadingPreference,
                     regionId,
                     replicaIndex,
                     operatorId );

        return newOperatorTupleQueue;
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
