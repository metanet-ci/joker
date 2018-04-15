package cs.bilkent.joker.engine.tuplequeue.impl;


import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import javax.annotation.concurrent.NotThreadSafe;
import javax.inject.Inject;
import javax.inject.Singleton;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import cs.bilkent.joker.engine.config.JokerConfig;
import cs.bilkent.joker.engine.config.ThreadingPref;
import static cs.bilkent.joker.engine.config.ThreadingPref.MULTI_THREADED;
import static cs.bilkent.joker.engine.config.ThreadingPref.SINGLE_THREADED;
import cs.bilkent.joker.engine.config.TupleQueueManagerConfig;
import cs.bilkent.joker.engine.partition.PartitionDistribution;
import cs.bilkent.joker.engine.partition.PartitionKeyExtractor;
import cs.bilkent.joker.engine.partition.PartitionKeyExtractorFactory;
import cs.bilkent.joker.engine.tuplequeue.OperatorQueue;
import cs.bilkent.joker.engine.tuplequeue.OperatorQueueManager;
import cs.bilkent.joker.engine.tuplequeue.TupleQueue;
import cs.bilkent.joker.engine.tuplequeue.impl.operator.DefaultOperatorQueue;
import cs.bilkent.joker.engine.tuplequeue.impl.operator.PartitionedOperatorQueue;
import cs.bilkent.joker.engine.tuplequeue.impl.queue.MultiThreadedTupleQueue;
import cs.bilkent.joker.engine.tuplequeue.impl.queue.SingleThreadedTupleQueue;
import cs.bilkent.joker.operator.OperatorDef;
import cs.bilkent.joker.operator.Tuple;
import static cs.bilkent.joker.operator.spec.OperatorType.PARTITIONED_STATEFUL;
import cs.bilkent.joker.operator.utils.Pair;
import cs.bilkent.joker.operator.utils.Triple;
import cs.bilkent.joker.partition.impl.PartitionKey;
import static java.lang.Math.max;
import static java.util.Arrays.copyOf;

@Singleton
@NotThreadSafe
public class OperatorQueueManagerImpl implements OperatorQueueManager
{

    private static final Logger LOGGER = LoggerFactory.getLogger( OperatorQueueManagerImpl.class );


    private final PartitionKeyExtractorFactory partitionKeyExtractorFactory;

    private final TupleQueueManagerConfig tupleQueueManagerConfig;

    private final Map<Triple<Integer, String, Integer>, DefaultOperatorQueue> defaultOperatorQueues = new HashMap<>();

    private final Map<Pair<Integer, String>, PartitionedOperatorQueue[]> partitionedOperatorQueues = new HashMap<>();

    @Inject
    public OperatorQueueManagerImpl ( final JokerConfig jokerConfig, final PartitionKeyExtractorFactory partitionKeyExtractorFactory )
    {
        this.partitionKeyExtractorFactory = partitionKeyExtractorFactory;
        this.tupleQueueManagerConfig = jokerConfig.getTupleQueueManagerConfig();
    }

    @Override
    public OperatorQueue createDefaultQueue ( final int regionId,
                                              final OperatorDef operatorDef,
                                              final int replicaIndex,
                                              final ThreadingPref threadingPref )
    {
        checkArgument( operatorDef != null, "No operator definition! regionId %s, replicaIndex %s", regionId, replicaIndex );
        checkArgument( threadingPref != null,
                       "No threading preference is given! regionId %s, replicaIndex %s operatorId %s",
                       regionId,
                       replicaIndex,
                       operatorDef.getId() );
        checkArgument( operatorDef.getOperatorType() != PARTITIONED_STATEFUL || threadingPref == MULTI_THREADED,
                       "invalid <operator type, threading preference> pair! regionId %s operatorId %s operatorType %s threadingPreference"
                       + " %s", regionId, operatorDef.getId(), operatorDef.getOperatorType(), threadingPref );
        checkArgument( replicaIndex >= 0,
                       "invalid replica index! regionId %s, replicaIndex %s operatorId %s",
                       regionId,
                       replicaIndex,
                       operatorDef.getId() );

        final String operatorId = operatorDef.getId();
        final Triple<Integer, String, Integer> key = Triple.of( regionId, operatorId, replicaIndex );
        checkState( !defaultOperatorQueues.containsKey( key ),
                    "default operator queue already exists for regionId %s operatorId %s replicaIndex %s",
                    regionId,
                    operatorId,
                    replicaIndex );

        final int inputPortCount = operatorDef.getInputPortCount();
        final TupleQueue[] tupleQueues = new TupleQueue[ inputPortCount ];
        for ( int portIndex = 0; portIndex < inputPortCount; portIndex++ )
        {
            tupleQueues[ portIndex ] = threadingPref == SINGLE_THREADED
                                       ? new SingleThreadedTupleQueue( tupleQueueManagerConfig.getTupleQueueCapacity() )
                                       : new MultiThreadedTupleQueue( tupleQueueManagerConfig.getTupleQueueCapacity() );
        }

        final String operatorQueueId = toOperatorQueueId( operatorId, replicaIndex );
        final int drainLimit = tupleQueueManagerConfig.getDrainLimit( threadingPref );
        final DefaultOperatorQueue operatorQueue = new DefaultOperatorQueue( operatorQueueId,
                                                                             inputPortCount,
                                                                             threadingPref,
                                                                             tupleQueues,
                                                                             drainLimit );

        defaultOperatorQueues.put( key, operatorQueue );
        LOGGER.debug( "created default operator queue for regionId={} replicaIndex={} operatorId={}", regionId, replicaIndex, operatorId );

        return operatorQueue;
    }

    private String toOperatorQueueId ( final String operatorId, final int replicaIndex )
    {
        return operatorId + "_replica" + replicaIndex;
    }

    @Override
    public OperatorQueue getDefaultQueue ( final int regionId, final String operatorId, final int replicaIndex )
    {
        return defaultOperatorQueues.get( Triple.of( regionId, operatorId, replicaIndex ) );
    }

    @Override
    public OperatorQueue[] createPartitionedQueues ( final int regionId,
                                                     final OperatorDef operatorDef,
                                                     final PartitionDistribution partitionDistribution,
                                                     final int forwardedKeySize )
    {
        final int replicaCount = partitionDistribution.getReplicaCount();
        checkArgument( operatorDef != null, "No operator definition! regionId %s, replicaCount %s", regionId, replicaCount );

        final String operatorId = operatorDef.getId();
        checkArgument( operatorDef.getOperatorType() == PARTITIONED_STATEFUL,
                       "invalid operator type: %s ! regionId %s operatorId %s",
                       regionId,
                       operatorId );
        checkArgument( replicaCount > 0, "invalid replica count %s ! regionId %s operatorId %s", replicaCount, regionId, operatorId );

        final Pair<Integer, String> key = Pair.of( regionId, operatorId );
        checkState( !partitionedOperatorQueues.containsKey( key ),
                    "partitioned operator queues already exist for regionId=%s operatorId=%s",
                    regionId,
                    operatorId );

        final int inputPortCount = operatorDef.getInputPortCount();

        final PartitionedOperatorQueue[] operatorQueues = new PartitionedOperatorQueue[ replicaCount ];
        final List<String> partitionFieldNames = operatorDef.getPartitionFieldNames();
        final PartitionKeyExtractor partitionKeyExtractor = partitionKeyExtractorFactory.createPartitionKeyExtractor( partitionFieldNames,
                                                                                                                      forwardedKeySize );

        for ( int replicaIndex = 0; replicaIndex < replicaCount; replicaIndex++ )
        {
            operatorQueues[ replicaIndex ] = new PartitionedOperatorQueue( operatorId,
                                                                           inputPortCount,
                                                                           partitionDistribution.getPartitionCount(),
                                                                           replicaIndex,
                                                                           tupleQueueManagerConfig.getTupleQueueCapacity(),
                                                                           partitionKeyExtractor );
        }

        partitionedOperatorQueues.put( key, operatorQueues );
        LOGGER.debug( "created partitioned queue with partition key extractor {} for regionId={} replicaCount={} operatorId={}",
                      partitionKeyExtractor.getClass().getSimpleName(),
                      regionId,
                      replicaCount,
                      operatorId );

        return operatorQueues;
    }

    @Override
    public OperatorQueue[] getPartitionedQueues ( final int regionId, final String operatorId )
    {
        final Pair<Integer, String> key = Pair.of( regionId, operatorId );
        final PartitionedOperatorQueue[] operatorQueues = this.partitionedOperatorQueues.get( key );
        return operatorQueues != null ? Arrays.copyOf( operatorQueues, operatorQueues.length ) : null;
    }

    @Override
    public OperatorQueue[] rebalancePartitionedQueues ( final int regionId,
                                                        final OperatorDef operatorDef,
                                                        final PartitionDistribution currentPartitionDistribution,
                                                        final PartitionDistribution newPartitionDistribution )
    {
        final String operatorId = operatorDef.getId();
        final Pair<Integer, String> key = Pair.of( regionId, operatorId );
        PartitionedOperatorQueue[] queues = this.partitionedOperatorQueues.get( key );
        checkState( queues != null, "partitioned operator queues do not exist for regionId=%s operatorId=%s", regionId, operatorId );

        final Map<Integer, Map<PartitionKey, TupleQueue[]>> migratingPartitions = getMigratingPartitions( currentPartitionDistribution,
                                                                                                          newPartitionDistribution,
                                                                                                          queues );

        queues = migratePartitions( currentPartitionDistribution, newPartitionDistribution, queues, migratingPartitions );

        this.partitionedOperatorQueues.put( key, queues );
        LOGGER.debug( "partitioned operator queues of regionId={} operatorId={} are rebalanced to {} replicas",
                      regionId,
                      operatorId,
                      newPartitionDistribution.getReplicaCount() );

        return queues;
    }

    private Map<Integer, Map<PartitionKey, TupleQueue[]>> getMigratingPartitions ( final PartitionDistribution currentPartitionDistribution,
                                                                                   final PartitionDistribution newPartitionDistribution,
                                                                                   final PartitionedOperatorQueue[] queues )
    {
        final Map<Integer, Map<PartitionKey, TupleQueue[]>> migratingPartitions = new HashMap<>();

        for ( int replicaIndex = 0; replicaIndex < queues.length; replicaIndex++ )
        {
            final List<Integer> partitionIds = currentPartitionDistribution.getPartitionIdsMigratedFromReplicaIndex(
                    newPartitionDistribution,
                    replicaIndex );
            if ( partitionIds.size() > 0 )
            {
                migratingPartitions.putAll( queues[ replicaIndex ].releasePartitions( new HashSet<>( partitionIds ) ) );
            }
        }

        return migratingPartitions;
    }

    private PartitionedOperatorQueue[] migratePartitions ( final PartitionDistribution currentPartitionDistribution,
                                                           final PartitionDistribution newPartitionDistribution,
                                                           final PartitionedOperatorQueue[] queues,
                                                           final Map<Integer, Map<PartitionKey, TupleQueue[]>> movingPartitions )
    {
        final String operatorId = queues[ 0 ].getOperatorId();
        final int inputPortCount = queues[ 0 ].getInputPortCount();
        final PartitionKeyExtractor partitionKeyExtractor = queues[ 0 ].getPartitionKeyExtractor();
        final int currentReplicaCount = currentPartitionDistribution.getReplicaCount();
        final int newReplicaCount = newPartitionDistribution.getReplicaCount();
        final PartitionedOperatorQueue[] newQueues = copyOf( queues, newReplicaCount );
        if ( currentReplicaCount > newReplicaCount )
        {
            for ( int replicaIndex = 0; replicaIndex < newReplicaCount; replicaIndex++ )
            {
                final List<Integer> migratedPartitionIds = currentPartitionDistribution.getPartitionIdsMigratedToReplicaIndex(
                        newPartitionDistribution,
                        replicaIndex );
                LOGGER.info( "{} partitions are migrating to Operator: {} replicaIndex: {}",
                             migratedPartitionIds.size(),
                             operatorId,
                             replicaIndex );
                for ( int partitionId : migratedPartitionIds )
                {
                    final Map<PartitionKey, TupleQueue[]> keys = movingPartitions.remove( partitionId );
                    checkState( keys != null );
                    newQueues[ replicaIndex ].acquireKeys( partitionId, keys );
                }
            }
        }
        else
        {
            for ( int replicaIndex = currentReplicaCount; replicaIndex < newReplicaCount; replicaIndex++ )
            {
                final int partitionCount = newPartitionDistribution.getPartitionCount();
                newQueues[ replicaIndex ] = new PartitionedOperatorQueue( operatorId,
                                                                          inputPortCount,
                                                                          partitionCount,
                                                                          replicaIndex,
                                                                          tupleQueueManagerConfig.getTupleQueueCapacity(),
                                                                          partitionKeyExtractor );
                final List<Integer> migratedPartitionIds = currentPartitionDistribution.getPartitionIdsMigratedToReplicaIndex(
                        newPartitionDistribution,
                        replicaIndex );
                LOGGER.info( "{} partitions are migrating to Operator: {} replicaIndex: {}",
                             migratedPartitionIds.size(),
                             operatorId,
                             replicaIndex );
                for ( int partitionId : migratedPartitionIds )
                {
                    final Map<PartitionKey, TupleQueue[]> keys = movingPartitions.remove( partitionId );
                    checkState( keys != null );
                    newQueues[ replicaIndex ].acquireKeys( partitionId, keys );
                }
            }
        }

        return newQueues;
    }

    @Override
    public void releaseDefaultQueue ( final int regionId, final String operatorId, final int replicaIndex )
    {
        final OperatorQueue operatorQueue = defaultOperatorQueues.remove( Triple.of( regionId, operatorId, replicaIndex ) );
        checkState( operatorQueue != null, "no default operator queue to release for regionId=%s replicaIndex=%s operatorId=%s",
                    regionId,
                    replicaIndex,
                    operatorId );

        operatorQueue.clear();
    }

    @Override
    public void releasePartitionedQueues ( final int regionId, final String operatorId )
    {
        final OperatorQueue[] operatorQueues = partitionedOperatorQueues.remove( Pair.of( regionId, operatorId ) );
        checkState( operatorQueues != null, "no partitioned queue to release for regionId=%s operatorId=%s", regionId, operatorId );
    }

    @Override
    public OperatorQueue switchThreadingPref ( final int regionId, final String operatorId, final int replicaIndex )
    {
        final Triple<Integer, String, Integer> key = Triple.of( regionId, operatorId, replicaIndex );
        final DefaultOperatorQueue operatorQueue = defaultOperatorQueues.remove( key );
        checkState( operatorQueue != null,
                    "No operator queue found to convert to single threaded for regionId=%s replicaIndex=%s operatorId=%s",
                    regionId,
                    replicaIndex,
                    operatorId );

        final ThreadingPref threadingPref = operatorQueue.getThreadingPref();

        final TupleQueue[] tupleQueues = new TupleQueue[ operatorQueue.getInputPortCount() ];
        if ( threadingPref == SINGLE_THREADED )
        {
            int capacity = tupleQueueManagerConfig.getTupleQueueCapacity();
            for ( int portIndex = 0; portIndex < operatorQueue.getInputPortCount(); portIndex++ )
            {
                final TupleQueue currentQueue = operatorQueue.getTupleQueue( portIndex );
                capacity = max( capacity, currentQueue.size() );
            }

            if ( capacity != tupleQueueManagerConfig.getTupleQueueCapacity() )
            {
                LOGGER.warn( "Extending tuple queues of regionId={} replicaIndex={} operatorId={} to capacity={} while converting to {}",
                             regionId,
                             replicaIndex,
                             operatorId,
                             capacity,
                             MULTI_THREADED );
            }

            for ( int portIndex = 0; portIndex < operatorQueue.getInputPortCount(); portIndex++ )
            {
                final TupleQueue currentQueue = operatorQueue.getTupleQueue( portIndex );
                final MultiThreadedTupleQueue newQueue = new MultiThreadedTupleQueue( capacity );
                convey( currentQueue, newQueue );
                tupleQueues[ portIndex ] = newQueue;
            }
        }
        else if ( threadingPref == MULTI_THREADED )
        {
            for ( int portIndex = 0; portIndex < operatorQueue.getInputPortCount(); portIndex++ )
            {
                final TupleQueue currentQueue = operatorQueue.getTupleQueue( portIndex );
                final int capacity = max( tupleQueueManagerConfig.getTupleQueueCapacity(), currentQueue.size() );

                if ( capacity != tupleQueueManagerConfig.getTupleQueueCapacity() )
                {
                    LOGGER.warn( "Extending tuple queues of regionId={} replicaIndex={} operatorId={} to capacity={} while converting to "
                                 + "{}", regionId, replicaIndex, operatorId, SINGLE_THREADED );
                }

                final SingleThreadedTupleQueue newQueue = new SingleThreadedTupleQueue( capacity );
                convey( currentQueue, newQueue );
                tupleQueues[ portIndex ] = newQueue;
            }
        }
        else
        {
            throw new IllegalStateException( "regionId=" + regionId + " has invalid threading preference: " + threadingPref );
        }

        final ThreadingPref newThreadingPref = threadingPref.reverse();

        final String operatorQueueId = toOperatorQueueId( operatorId, replicaIndex );
        final int drainLimit = tupleQueueManagerConfig.getDrainLimit( newThreadingPref );
        final DefaultOperatorQueue newOperatorQueue = new DefaultOperatorQueue( operatorQueueId,
                                                                                operatorQueue.getInputPortCount(),
                                                                                newThreadingPref,
                                                                                tupleQueues,
                                                                                drainLimit );

        defaultOperatorQueues.put( key, newOperatorQueue );
        LOGGER.debug( "{} default operator queue is switched to {} for regionId={} replicaIndex={} operatorId={}",
                      threadingPref,
                      newThreadingPref,
                      regionId,
                      replicaIndex,
                      operatorId );

        return newOperatorQueue;
    }

    private void convey ( final TupleQueue sourceQueue, final TupleQueue targetQueue )
    {
        final List<Tuple> tuples = sourceQueue.poll( Integer.MAX_VALUE );
        final int count = targetQueue.offer( tuples );
        assert count == tuples.size();
    }

}
