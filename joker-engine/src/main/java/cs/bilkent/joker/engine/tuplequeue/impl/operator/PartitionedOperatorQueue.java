package cs.bilkent.joker.engine.tuplequeue.impl.operator;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import cs.bilkent.joker.engine.partition.PartitionKeyExtractor;
import static cs.bilkent.joker.engine.partition.PartitionUtil.getPartitionId;
import cs.bilkent.joker.engine.tuplequeue.OperatorQueue;
import cs.bilkent.joker.engine.tuplequeue.TupleQueue;
import cs.bilkent.joker.engine.tuplequeue.TupleQueueDrainer;
import cs.bilkent.joker.engine.tuplequeue.impl.drainer.GreedyDrainer;
import cs.bilkent.joker.engine.tuplequeue.impl.queue.SingleThreadedTupleQueue;
import cs.bilkent.joker.operator.Tuple;
import cs.bilkent.joker.operator.impl.TuplesImpl;
import cs.bilkent.joker.operator.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByPort;
import static cs.bilkent.joker.operator.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByPort.ALL_PORTS;
import static cs.bilkent.joker.operator.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByPort.ANY_PORT;
import cs.bilkent.joker.partition.impl.PartitionKey;
import gnu.trove.map.hash.THashMap;
import gnu.trove.set.hash.TLinkedHashSet;


public class PartitionedOperatorQueue implements OperatorQueue
{

    private static final Logger LOGGER = LoggerFactory.getLogger( PartitionedOperatorQueue.class );

    private static final int TUPLE_QUEUE_INITIAL_SIZE = 10;


    private final String operatorId;

    private final int replicaIndex;

    private final int inputPortCount;

    private final int partitionCount;

    private final PartitionKeyExtractor partitionKeyExtractor;

    private final Function<Object, TupleQueue[]> tupleQueuesConstructor;

    private final Map<PartitionKey, TupleQueue[]> tupleQueuesByKeys;

    private final Set<PartitionKey> drainableKeys = new TLinkedHashSet<>();

    private int[] tupleCounts;

    private TupleAvailabilityByPort tupleAvailabilityByPort;

    public PartitionedOperatorQueue ( final String operatorId,
                                      final int inputPortCount,
                                      final int partitionCount,
                                      final int replicaIndex,
                                      final int tupleQueueCapacity,
                                      final PartitionKeyExtractor partitionKeyExtractor )
    {
        checkArgument( inputPortCount >= 0, "invalid input port count %s for partitioned tuple queue of operator $s",
                       inputPortCount,
                       operatorId );
        checkArgument( partitionCount > 0 );
        checkArgument( tupleQueueCapacity > 0 );
        this.operatorId = operatorId;
        this.replicaIndex = replicaIndex;
        this.inputPortCount = inputPortCount;
        this.partitionCount = partitionCount;
        this.partitionKeyExtractor = partitionKeyExtractor;
        this.tupleQueuesByKeys = new THashMap<>();
        this.tupleQueuesConstructor = ( partitionKey ) -> {
            final TupleQueue[] tupleQueues = new TupleQueue[ inputPortCount ];
            for ( int i = 0; i < inputPortCount; i++ )
            {
                tupleQueues[ i ] = new SingleThreadedTupleQueue( TUPLE_QUEUE_INITIAL_SIZE );
            }
            return tupleQueues;
        };
        this.tupleCounts = new int[ inputPortCount ];
        Arrays.fill( this.tupleCounts, 1 );
        this.tupleAvailabilityByPort = ANY_PORT;
    }

    @Override
    public String getOperatorId ()
    {
        return operatorId;
    }

    @Override
    public int getInputPortCount ()
    {
        return inputPortCount;
    }

    @Override
    public int offer ( final int portIndex, final List<Tuple> tuples )
    {
        return offer( portIndex, tuples, 0 );
    }

    @Override
    public int offer ( final int portIndex, final List<Tuple> tuples, final int startIndex )
    {
        if ( tuples == null )
        {
            return 0;
        }

        final int size = tuples.size();
        for ( int i = startIndex; i < size; i++ )
        {
            final Tuple tuple = tuples.get( i );
            final PartitionKey partitionKey = partitionKeyExtractor.getPartitionKey( tuple );
            final TupleQueue[] tupleQueues = getTupleQueues( partitionKey );
            tupleQueues[ portIndex ].offer( tuple );
            addToDrainableKeys( partitionKey, tupleQueues );
        }

        return ( size - startIndex );
    }

    @Override
    public void drain ( final TupleQueueDrainer drainer,
                        final Function<PartitionKey, TuplesImpl> tuplesSupplier )
    {
        if ( drainer instanceof GreedyDrainer )
        {
            final Iterator<PartitionKey> it = tupleQueuesByKeys.keySet().iterator();
            while ( it.hasNext() )
            {
                final PartitionKey key = it.next();
                drainer.drain( key, getTupleQueues( key ), tuplesSupplier );
                it.remove();
                drainableKeys.remove( key );
            }
        }
        else
        {
            // tuple count based draining

            if ( drainableKeys.isEmpty() )
            {
                return;
            }

            final Iterator<PartitionKey> it = drainableKeys.iterator();
            while ( it.hasNext() )
            {
                final PartitionKey key = it.next();
                final TupleQueue[] tupleQueues = getTupleQueues( key );
                while ( true )
                {
                    if ( !drainer.drain( key, tupleQueues, tuplesSupplier ) )
                    {
                        break;
                    }
                }
                it.remove();
            }
        }
    }

    @Override
    public void clear ()
    {
        LOGGER.debug( "Clearing partitioned tuple queues of operator: {}", operatorId );

        for ( Entry<PartitionKey, TupleQueue[]> e : tupleQueuesByKeys.entrySet() )
        {
            final TupleQueue[] tupleQueues = e.getValue();
            for ( int portIndex = 0; portIndex < tupleQueues.length; portIndex++ )
            {
                final TupleQueue tupleQueue = tupleQueues[ portIndex ];
                final int size = tupleQueue.size();
                if ( size > 0 )
                {
                    if ( LOGGER.isDebugEnabled() )
                    {
                        final List<Tuple> tuples = tupleQueue.poll( Integer.MAX_VALUE );
                        LOGGER.warn( "Tuple queue {} of operator: {} for key: {} has {} tuples before clear: {}",
                                     portIndex,
                                     operatorId,
                                     e.getKey(),
                                     size,
                                     tuples );
                    }
                    else
                    {
                        LOGGER.warn( "Tuple queue {} of operator: {} for key: {} has {} tuples before clear",
                                     portIndex,
                                     operatorId,
                                     e.getKey(),
                                     size );
                    }
                }
                tupleQueue.clear();
            }
        }

        tupleQueuesByKeys.clear();
        drainableKeys.clear();
    }

    @Override
    public void setTupleCounts ( final int[] tupleCounts, final TupleAvailabilityByPort tupleAvailabilityByPort )
    {
        LOGGER.info( "Setting tuple requirements {} , {} for partitioned tuple queues of operator: {}",
                     tupleCounts,
                     tupleAvailabilityByPort,
                     operatorId );

        checkArgument( inputPortCount == tupleCounts.length,
                       "mismatching input port counts for tuple queue container of operator: %s operator input port count: %s, arg: %s",
                       operatorId,
                       inputPortCount,
                       tupleCounts.length );
        checkArgument( tupleAvailabilityByPort == ANY_PORT || tupleAvailabilityByPort == ALL_PORTS,
                       "invalid %s:%s for tuple queue container of operator: %s operator",
                       TupleAvailabilityByPort.class,
                       tupleAvailabilityByPort,
                       operatorId );
        this.tupleCounts = Arrays.copyOf( tupleCounts, tupleCounts.length );
        this.tupleAvailabilityByPort = tupleAvailabilityByPort;
        this.drainableKeys.clear();
        for ( Entry<PartitionKey, TupleQueue[]> e : tupleQueuesByKeys.entrySet() )
        {
            addToDrainableKeys( e.getKey(), e.getValue() );
        }
    }

    @Override
    public boolean isEmpty ()
    {
        for ( TupleQueue[] tupleQueues : tupleQueuesByKeys.values() )
        {
            for ( TupleQueue tupleQueue : tupleQueues )
            {
                if ( tupleQueue.size() > 0 )
                {
                    return false;
                }
            }
        }

        return true;
    }

    @Override
    public void ensureCapacity ( final int capacity )
    {

    }

    public void acquireKeys ( final int partitionId, final Map<PartitionKey, TupleQueue[]> keys )
    {
        LOGGER.debug( "partition={} are acquired by operatorId={} replicaIndex={}", partitionId, operatorId, replicaIndex );

        for ( Entry<PartitionKey, TupleQueue[]> e : keys.entrySet() )
        {
            final PartitionKey partitionKey = e.getKey();
            final TupleQueue[] prev = tupleQueuesByKeys.put( partitionKey, e.getValue() );
            checkState( prev == null );
            addToDrainableKeys( partitionKey, e.getValue() );
        }
    }

    public Map<Integer, Map<PartitionKey, TupleQueue[]>> releasePartitions ( final Set<Integer> partitionIds )
    {
        checkArgument( partitionIds != null,
                       "cannot release null partition ids of operatorId=%s replicaIndex=%s",
                       operatorId,
                       replicaIndex );

        final Map<Integer, Map<PartitionKey, TupleQueue[]>> released = new HashMap<>();
        final Iterator<Entry<PartitionKey, TupleQueue[]>> it = tupleQueuesByKeys.entrySet().iterator();

        while ( it.hasNext() )
        {
            final Entry<PartitionKey, TupleQueue[]> e = it.next();
            final PartitionKey partitionKey = e.getKey();
            final TupleQueue[] tupleQueues = e.getValue();
            final int partitionId = getPartitionId( partitionKey.partitionHashCode(), partitionCount );

            if ( partitionIds.contains( partitionId ) )
            {
                released.computeIfAbsent( partitionId, i -> new HashMap<>() ).put( partitionKey, tupleQueues );
                it.remove();
                drainableKeys.remove( partitionKey );
            }
        }

        LOGGER.debug( "partitions={} are released by operatorId={} replicaIndex={}", partitionIds, operatorId, replicaIndex );

        return released;
    }

    public PartitionKeyExtractor getPartitionKeyExtractor ()
    {
        return partitionKeyExtractor;
    }

    public int getDrainableKeyCount ()
    {
        return drainableKeys.size();
    }

    private void addToDrainableKeys ( final PartitionKey key, final TupleQueue[] tupleQueues )
    {
        if ( !drainableKeys.contains( key ) && checkIfDrainable( tupleQueues ) )
        {
            drainableKeys.add( key );
        }
    }

    private boolean checkIfDrainable ( final TupleQueue[] tupleQueues )
    {
        if ( tupleAvailabilityByPort == ANY_PORT )
        {
            for ( int i = 0; i < inputPortCount; i++ )
            {
                if ( tupleCounts[ i ] > 0 && tupleQueues[ i ].size() >= tupleCounts[ i ] )
                {
                    return true;
                }
            }

            return false;
        }
        else
        {
            for ( int i = 0; i < inputPortCount; i++ )
            {
                if ( tupleCounts[ i ] > 0 && tupleQueues[ i ].size() < tupleCounts[ i ] )
                {
                    return false;
                }
            }

            return true;
        }
    }

    private TupleQueue[] getTupleQueues ( final PartitionKey key )
    {
        return tupleQueuesByKeys.computeIfAbsent( key, this.tupleQueuesConstructor );
    }

}
