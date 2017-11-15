package cs.bilkent.joker.engine.tuplequeue.impl;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
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
import cs.bilkent.joker.operator.Tuple;
import static cs.bilkent.joker.operator.spec.OperatorType.PARTITIONED_STATEFUL;
import cs.bilkent.joker.utils.Pair;
import cs.bilkent.joker.utils.Triple;
import static java.lang.Math.max;
import static java.util.Arrays.copyOf;

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
                       operatorDef.getId() );
        checkArgument( operatorDef.getOperatorType() != PARTITIONED_STATEFUL || threadingPreference == MULTI_THREADED,
                       "invalid <operator type, threading preference> pair! regionId %s operatorId %s operatorType %s threadingPreference"
                       + " %s ", regionId, operatorDef.getId(), operatorDef.getOperatorType(),
                       threadingPreference );
        checkArgument( replicaIndex >= 0,
                       "invalid replica index! regionId %s, replicaIndex %s operatorId %s",
                       regionId,
                       replicaIndex,
                       operatorDef.getId() );

        final String operatorId = operatorDef.getId();
        final Triple<Integer, Integer, String> key = Triple.of( regionId, replicaIndex, operatorId );
        checkState( !singleOperatorTupleQueues.containsKey( key ),
                    "default operator tuple queue already exists for regionId %s operatorId %s replicaIndex %s",
                    regionId,
                    operatorId,
                    replicaIndex );

        final int inputPortCount = operatorDef.getInputPortCount();
        final TupleQueue[] tupleQueues = new TupleQueue[ inputPortCount ];
        for ( int portIndex = 0; portIndex < inputPortCount; portIndex++ )
        {
            tupleQueues[ portIndex ] = threadingPreference == SINGLE_THREADED
                                       ? new SingleThreadedTupleQueue( tupleQueueManagerConfig.getTupleQueueCapacity() )
                                       : new MultiThreadedTupleQueue( tupleQueueManagerConfig.getTupleQueueCapacity() );
        }

        final String operatorTupleQueueId = toOperatorTupleQueueId( operatorId, replicaIndex );
        final DefaultOperatorTupleQueue operatorTupleQueue = new DefaultOperatorTupleQueue( operatorTupleQueueId,
                                                                                            inputPortCount,
                                                                                            threadingPreference,
                                                                                            tupleQueues,
                                                                                            tupleQueueManagerConfig.getTupleQueueCapacity
                                                                                                                            () );

        singleOperatorTupleQueues.put( key, operatorTupleQueue );
        LOGGER.debug( "created default tuple queue for regionId={} replicaIndex={} operatorId={}", regionId, replicaIndex, operatorId );

        return operatorTupleQueue;
    }

    private String toOperatorTupleQueueId ( final String operatorId, final int replicaIndex )
    {
        return operatorId + "_replica" + replicaIndex;
    }

    @Override
    public OperatorTupleQueue getDefaultOperatorTupleQueue ( final int regionId, final int replicaIndex, final String operatorId )
    {
        return singleOperatorTupleQueues.get( Triple.of( regionId, replicaIndex, operatorId ) );
    }

    @Override
    public OperatorTupleQueue[] createPartitionedOperatorTupleQueues ( final int regionId,
                                                                       final OperatorDef operatorDef,
                                                                       PartitionDistribution partitionDistribution,
                                                                       final int forwardKeyLimit )
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
        checkState( !partitionedOperatorTupleQueues.containsKey( key ),
                    "partitioned operator tuple queues already exist for regionId=%s operatorId=%s",
                    regionId,
                    operatorId );

        final int inputPortCount = operatorDef.getInputPortCount();

        final PartitionedOperatorTupleQueue[] operatorTupleQueues = new PartitionedOperatorTupleQueue[ replicaCount ];
        final List<String> partitionFieldNames = operatorDef.getPartitionFieldNames();
        final PartitionKeyExtractor partitionKeyExtractor = partitionKeyExtractorFactory.createPartitionKeyExtractor( partitionFieldNames,
                                                                                                                      forwardKeyLimit );

        for ( int replicaIndex = 0; replicaIndex < replicaCount; replicaIndex++ )
        {
            final TupleQueueContainer[] containers = createTupleQueueContainers( operatorId,
                                                                                 inputPortCount,
                                                                                 partitionDistribution.getPartitionCount() );
            final int[] partitions = partitionDistribution.getDistribution();

            operatorTupleQueues[ replicaIndex ] = new PartitionedOperatorTupleQueue( operatorId,
                                                                                     inputPortCount,
                                                                                     partitionDistribution.getPartitionCount(),
                                                                                     replicaIndex,
                                                                                     tupleQueueManagerConfig.getTupleQueueCapacity(),
                                                                                     partitionKeyExtractor,
                                                                                     containers,
                                                                                     partitions,
                                                                                     tupleQueueManagerConfig.getMaxDrainableKeyCount(),
                                                                                     tupleQueueManagerConfig
                                                                                             .getPartitionedTupleQueueDrainHint() );
        }

        partitionedOperatorTupleQueues.put( key, operatorTupleQueues );
        LOGGER.debug( "created partitioned tuple queue with partition key extractor {} for regionId={} replicaCount={} operatorId={}",
                      partitionKeyExtractor.getClass().getSimpleName(),
                      regionId,
                      replicaCount,
                      operatorId );

        return operatorTupleQueues;
    }

    @Override
    public OperatorTupleQueue[] getPartitionedOperatorTupleQueues ( final int regionId, final String operatorId )
    {
        final Pair<Integer, String> key = Pair.of( regionId, operatorId );
        final PartitionedOperatorTupleQueue[] operatorTupleQueues = this.partitionedOperatorTupleQueues.get( key );
        return operatorTupleQueues != null ? Arrays.copyOf( operatorTupleQueues, operatorTupleQueues.length ) : null;
    }

    @Override
    public OperatorTupleQueue[] rebalancePartitionedOperatorTupleQueues ( final int regionId,
                                                                          final OperatorDef operatorDef,
                                                                          final PartitionDistribution currentPartitionDistribution,
                                                                          final PartitionDistribution newPartitionDistribution )
    {
        final String operatorId = operatorDef.getId();
        final Pair<Integer, String> key = Pair.of( regionId, operatorId );
        PartitionedOperatorTupleQueue[] queues = this.partitionedOperatorTupleQueues.get( key );
        checkState( queues != null, "partitioned operator tuple queues do not exist for regionId=%s operatorId=%s", regionId, operatorId );

        final Map<Integer, TupleQueueContainer> migratingPartitions = getMigratingPartitions( currentPartitionDistribution,
                                                                                              newPartitionDistribution,
                                                                                              queues );

        queues = migratePartitions( currentPartitionDistribution, newPartitionDistribution, queues, migratingPartitions );

        this.partitionedOperatorTupleQueues.put( key, queues );
        LOGGER.debug( "partitioned operator tuple queues of regionId={} operatorId={} are rebalanced to {} replicas",
                      regionId,
                      operatorId,
                      newPartitionDistribution.getReplicaCount() );

        return queues;
    }

    private Map<Integer, TupleQueueContainer> getMigratingPartitions ( final PartitionDistribution currentPartitionDistribution,
                                                                       final PartitionDistribution newPartitionDistribution,
                                                                       final PartitionedOperatorTupleQueue[] queues )
    {
        final Map<Integer, TupleQueueContainer> migratingPartitions = new HashMap<>();

        for ( int replicaIndex = 0; replicaIndex < queues.length; replicaIndex++ )
        {
            final List<Integer> partitionIds = currentPartitionDistribution.getPartitionIdsMigratedFromReplicaIndex(
                    newPartitionDistribution,
                    replicaIndex );
            if ( partitionIds.size() > 0 )
            {
                queues[ replicaIndex ].releasePartitions( partitionIds ).forEach( c -> migratingPartitions.put( c.getPartitionId(), c ) );
            }
        }

        return migratingPartitions;
    }

    private PartitionedOperatorTupleQueue[] migratePartitions ( final PartitionDistribution currentPartitionDistribution,
                                                                final PartitionDistribution newPartitionDistribution,
                                                                final PartitionedOperatorTupleQueue[] queues,
                                                                final Map<Integer, TupleQueueContainer> movingPartitions )
    {
        final String operatorId = queues[ 0 ].getOperatorId();
        final int inputPortCount = queues[ 0 ].getInputPortCount();
        final PartitionKeyExtractor partitionKeyExtractor = queues[ 0 ].getPartitionKeyExtractor();
        final int currentReplicaCount = currentPartitionDistribution.getReplicaCount();
        final int newReplicaCount = newPartitionDistribution.getReplicaCount();
        final PartitionedOperatorTupleQueue[] newQueues = copyOf( queues, newReplicaCount );
        if ( currentReplicaCount > newReplicaCount )
        {
            for ( int replicaIndex = 0; replicaIndex < newReplicaCount; replicaIndex++ )
            {
                final List<TupleQueueContainer> partitions = getPartitionsMigratedToReplicaIndex( currentPartitionDistribution,
                                                                                                  newPartitionDistribution,
                                                                                                  movingPartitions,
                                                                                                  replicaIndex );
                newQueues[ replicaIndex ].acquirePartitions( partitions );
            }
        }
        else
        {
            for ( int replicaIndex = currentReplicaCount; replicaIndex < newReplicaCount; replicaIndex++ )
            {
                final int partitionCount = newPartitionDistribution.getPartitionCount();
                final TupleQueueContainer containers[] = new TupleQueueContainer[ partitionCount ];
                getPartitionsMigratedToReplicaIndex( currentPartitionDistribution,
                                                     newPartitionDistribution,
                                                     movingPartitions,
                                                     replicaIndex ).forEach( p -> containers[ p.getPartitionId() ] = p );
                newQueues[ replicaIndex ] = new PartitionedOperatorTupleQueue( operatorId,
                                                                               inputPortCount,
                                                                               partitionCount,
                                                                               replicaIndex,
                                                                               tupleQueueManagerConfig.getTupleQueueCapacity(),
                                                                               partitionKeyExtractor,
                                                                               containers,
                                                                               newPartitionDistribution.getDistribution(),
                                                                               tupleQueueManagerConfig.getMaxDrainableKeyCount(),
                                                                               tupleQueueManagerConfig.getPartitionedTupleQueueDrainHint
                                                                                                               () );

            }
        }

        return newQueues;
    }

    private List<TupleQueueContainer> getPartitionsMigratedToReplicaIndex ( final PartitionDistribution currentPartitionDistribution,
                                                                            final PartitionDistribution newPartitionDistribution,
                                                                            final Map<Integer, TupleQueueContainer> movingPartitions,
                                                                            final int replicaIndex )
    {
        final List<Integer> partitionIds = currentPartitionDistribution.getPartitionIdsMigratedToReplicaIndex( newPartitionDistribution,
                                                                                                               replicaIndex );

        final List<TupleQueueContainer> partitions = new ArrayList<>();
        for ( Integer partitionId : partitionIds )
        {
            partitions.add( movingPartitions.remove( partitionId ) );
        }

        return partitions;
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
    public void releasePartitionedOperatorTupleQueues ( final int regionId, final String operatorId )
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

        final ThreadingPreference threadingPreference = operatorTupleQueue.getThreadingPreference();

        final TupleQueue[] tupleQueues = new TupleQueue[ operatorTupleQueue.getInputPortCount() ];
        if ( threadingPreference == SINGLE_THREADED )
        {
            int capacity = tupleQueueManagerConfig.getTupleQueueCapacity();
            for ( int portIndex = 0; portIndex < operatorTupleQueue.getInputPortCount(); portIndex++ )
            {
                final TupleQueue currentQueue = operatorTupleQueue.getTupleQueue( portIndex );
                capacity = max( capacity, currentQueue.size() );
            }

            if ( capacity != tupleQueueManagerConfig.getTupleQueueCapacity() )
            {
                LOGGER.warn( "Extending tuple queues of regionId={} replicaIndex={} operatorId={} to capacity={} while converting to {}",
                             regionId,
                             replicaIndex,
                             operatorId, capacity,
                             MULTI_THREADED );
            }

            for ( int portIndex = 0; portIndex < operatorTupleQueue.getInputPortCount(); portIndex++ )
            {
                final TupleQueue currentQueue = operatorTupleQueue.getTupleQueue( portIndex );
                final MultiThreadedTupleQueue newQueue = new MultiThreadedTupleQueue( capacity );
                drain( currentQueue, newQueue );
                tupleQueues[ portIndex ] = newQueue;
            }
        }
        else if ( threadingPreference == MULTI_THREADED )
        {
            for ( int portIndex = 0; portIndex < operatorTupleQueue.getInputPortCount(); portIndex++ )
            {
                final TupleQueue currentQueue = operatorTupleQueue.getTupleQueue( portIndex );
                final int capacity = max( tupleQueueManagerConfig.getTupleQueueCapacity(), currentQueue.size() );

                if ( capacity != tupleQueueManagerConfig.getTupleQueueCapacity() )
                {
                    LOGGER.warn( "Extending tuple queues of regionId={} replicaIndex={} operatorId={} to capacity={} while converting to "
                                 + "{}", regionId, replicaIndex, operatorId, SINGLE_THREADED );
                }

                final SingleThreadedTupleQueue newQueue = new SingleThreadedTupleQueue( capacity );
                drain( currentQueue, newQueue );
                tupleQueues[ portIndex ] = newQueue;
            }
        }
        else
        {
            throw new IllegalStateException( "regionId=" + regionId + " has invalid threading preference: " + threadingPreference );
        }

        final ThreadingPreference newThreadingPreference = threadingPreference.reverse();

        final String operatorTupleQueueId = toOperatorTupleQueueId( operatorId, replicaIndex );
        final DefaultOperatorTupleQueue newOperatorTupleQueue = new DefaultOperatorTupleQueue( operatorTupleQueueId,
                                                                                               operatorTupleQueue.getInputPortCount(),
                                                                                               newThreadingPreference,
                                                                                               tupleQueues,
                                                                                               tupleQueueManagerConfig
                                                                                                       .getTupleQueueCapacity() );

        singleOperatorTupleQueues.put( tupleQueueId, newOperatorTupleQueue );
        LOGGER.debug( "{} default tuple queue is switched to {} for regionId={} replicaIndex={} operatorId={}",
                      threadingPreference,
                      newThreadingPreference,
                      regionId,
                      replicaIndex,
                      operatorId );

        return newOperatorTupleQueue;
    }

    private void drain ( final TupleQueue sourceQueue, final TupleQueue targetQueue )
    {
        Tuple tuple;
        while ( ( tuple = sourceQueue.poll() ) != null )
        {
            final boolean offered = targetQueue.offer( tuple );
            assert offered;
        }
    }

    private TupleQueueContainer[] createTupleQueueContainers ( final String operatorId, final int inputPortCount, final int partitionCount )
    {
        final TupleQueueContainer[] containers = new TupleQueueContainer[ partitionCount ];
        for ( int i = 0; i < partitionCount; i++ )
        {
            containers[ i ] = new TupleQueueContainer( operatorId, inputPortCount, i );
        }

        tupleQueueContainersByOperatorId.put( operatorId, containers );
        return containers;
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
