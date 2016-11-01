package cs.bilkent.joker.engine.tuplequeue.impl;


import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
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
import cs.bilkent.joker.engine.partition.PartitionDistribution;
import cs.bilkent.joker.engine.partition.PartitionKeyExtractor;
import cs.bilkent.joker.engine.partition.PartitionKeyExtractorFactory;
import cs.bilkent.joker.engine.tuplequeue.OperatorTupleQueue;
import cs.bilkent.joker.engine.tuplequeue.OperatorTupleQueueManager;
import cs.bilkent.joker.engine.tuplequeue.TupleQueue;
import cs.bilkent.joker.engine.tuplequeue.impl.operator.DefaultOperatorTupleQueue;
import cs.bilkent.joker.engine.tuplequeue.impl.operator.PartitionedOperatorTupleQueue;
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


    private final PartitionKeyExtractorFactory partitionKeyExtractorFactory;

    private final TupleQueueManagerConfig tupleQueueManagerConfig;

    private final Map<Triple<Integer, Integer, String>, DefaultOperatorTupleQueue> singleOperatorTupleQueues = new HashMap<>();

    private final Map<Pair<Integer, String>, PartitionedOperatorTupleQueue[]> partitionedOperatorTupleQueues = new HashMap<>();

    private final Map<String, TupleQueueContainer[]> tupleQueueContainersByOperatorId = new HashMap<>();


    @Inject
    public OperatorTupleQueueManagerImpl ( final JokerConfig jokerConfig, final PartitionKeyExtractorFactory partitionKeyExtractorFactory )
    {
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

        final String operatorId = operatorDef.id();
        final Triple<Integer, Integer, String> key = Triple.of( regionId, replicaIndex, operatorId );
        checkState( !singleOperatorTupleQueues.containsKey( key ),
                    "default operator tuple queue already exists for regionId %s operatorId %s replicaIndex %s",
                    regionId,
                    operatorId,
                    replicaIndex );

        final BiFunction<Integer, Boolean, TupleQueue> tupleQueueConstructor = getTupleQueueConstructor( threadingPreference );

        final int inputPortCount = operatorDef.inputPortCount();

        final int maxSingleThreadedTupleQueueSize = tupleQueueManagerConfig.getMaxSingleThreadedTupleQueueSize();
        final DefaultOperatorTupleQueue operatorTupleQueue = new DefaultOperatorTupleQueue( operatorId,
                                                                                            inputPortCount,
                                                                                            threadingPreference,
                                                                                            tupleQueueConstructor,
                                                                                            maxSingleThreadedTupleQueueSize );

        singleOperatorTupleQueues.put( key, operatorTupleQueue );
        LOGGER.info( "created default tuple queue for regionId={} replicaIndex={} operatorId={}", regionId, replicaIndex, operatorId );

        return operatorTupleQueue;
    }

    public OperatorTupleQueue getDefaultOperatorTupleQueue ( final int regionId, final int replicaIndex, final OperatorDef operatorDef )
    {
        return singleOperatorTupleQueues.get( Triple.of( regionId, replicaIndex, operatorDef.id() ) );
    }

    @Override
    public PartitionedOperatorTupleQueue[] createPartitionedOperatorTupleQueue ( final int regionId,
                                                                                 PartitionDistribution partitionDistribution,
                                                                                 final OperatorDef operatorDef,
                                                                                 final int forwardKeyLimit )
    {
        final int replicaCount = partitionDistribution.getReplicaCount();
        checkArgument( operatorDef != null, "No operator definition! regionId %s, replicaCount %s", regionId, replicaCount );

        final String operatorId = operatorDef.id();
        checkArgument( operatorDef.operatorType() == PARTITIONED_STATEFUL,
                       "invalid operator type: %s ! regionId %s operatorId %s",
                       regionId, operatorId );
        checkArgument( replicaCount > 0, "invalid replica count %s ! regionId %s operatorId %s", replicaCount, regionId, operatorId );

        final Pair<Integer, String> key = Pair.of( regionId, operatorId );
        checkState( !partitionedOperatorTupleQueues.containsKey( key ),
                    "partitioned operator tuple queues already exist for regionId=%s operatorId=%s",
                    regionId,
                    operatorId );

        final BiFunction<Integer, Boolean, TupleQueue> tupleQueueConstructor = getTupleQueueConstructor( SINGLE_THREADED );

        final int inputPortCount = operatorDef.inputPortCount();

        final PartitionedOperatorTupleQueue[] operatorTupleQueues = new PartitionedOperatorTupleQueue[ replicaCount ];
        final List<String> partitionFieldNames = operatorDef.partitionFieldNames();
        final PartitionKeyExtractor partitionKeyExtractor = partitionKeyExtractorFactory.createPartitionKeyExtractor( partitionFieldNames,
                                                                                                                      forwardKeyLimit );

        for ( int replicaIndex = 0; replicaIndex < replicaCount; replicaIndex++ )
        {
            final TupleQueueContainer[] containers = getOrCreateTupleQueueContainers( operatorId,
                                                                                      inputPortCount,
                                                                                      partitionDistribution.getPartitionCount(),
                                                                                      tupleQueueConstructor );
            final int[] partitions = partitionDistribution.getDistribution();

            operatorTupleQueues[ replicaIndex ] = new PartitionedOperatorTupleQueue( operatorId,
                                                                                     inputPortCount,
                                                                                     partitionDistribution.getPartitionCount(),
                                                                                     replicaIndex,
                                                                                     partitionKeyExtractor,
                                                                                     containers,
                                                                                     partitions,
                                                                                     tupleQueueManagerConfig.getMaxDrainableKeyCount() );
        }

        partitionedOperatorTupleQueues.put( key, operatorTupleQueues );
        LOGGER.info( "created partitioned tuple queue with partition key extractor {} for regionId={} replicaCount={} operatorId={}",
                     partitionKeyExtractor.getClass().getSimpleName(),
                     regionId,
                     replicaCount,
                     operatorId );

        return operatorTupleQueues;
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
    public void releaseDefaultOperatorTupleQueue ( final int regionId, final int replicaIndex, final String operatorId )
    {
        final OperatorTupleQueue operatorTupleQueue = singleOperatorTupleQueues.remove( Triple.of( regionId, replicaIndex, operatorId ) );
        checkState( operatorTupleQueue != null,
                    "no default tuple queue to release for regionId=%s replicaIndex=%s operatorId=%s",
                    regionId,
                    replicaIndex,
                    operatorId );

        operatorTupleQueue.clear();
    }

    @Override
    public void releasePartitionedOperatorTupleQueue ( final int regionId, final String operatorId )
    {
        final OperatorTupleQueue[] operatorTupleQueues = partitionedOperatorTupleQueues.remove( Pair.of( regionId, operatorId ) );
        checkState( operatorTupleQueues != null,
                    "no partitioned tuple queue to release for regionId=%s operatorId=%s",
                    regionId,
                    operatorId );

        releaseTupleQueueContainers( operatorId );
    }

    @Override
    public OperatorTupleQueue switchThreadingPreference ( final int regionId, final int replicaIndex, final String operatorId )
    {
        final Triple<Integer, Integer, String> tupleQueueId = Triple.of( regionId, replicaIndex, operatorId );
        final DefaultOperatorTupleQueue operatorTupleQueue = singleOperatorTupleQueues.remove( tupleQueueId );
        checkState( operatorTupleQueue != null,
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
