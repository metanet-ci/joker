package cs.bilkent.zanza.engine.tuplequeue.impl;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkArgument;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueue;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueueDrainer;
import cs.bilkent.zanza.engine.tuplequeue.impl.drainer.GreedyDrainer;
import cs.bilkent.zanza.operator.Tuple;
import cs.bilkent.zanza.operator.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByPort;
import static cs.bilkent.zanza.operator.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByPort.ALL_PORTS;
import static cs.bilkent.zanza.operator.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByPort.ANY_PORT;
import gnu.trove.map.hash.THashMap;
import gnu.trove.set.hash.THashSet;

public class TupleQueueContainer
{

    private static final Logger LOGGER = LoggerFactory.getLogger( TupleQueueContainer.class );


    private final String operatorId;

    private final int inputPortCount;

    private final int partitionId;

    private final Function<Object, TupleQueue[]> tupleQueuesConstructor;

    private final Map<Object, TupleQueue[]> tupleQueuesByKeys;

    private int[] tupleCounts;

    private TupleAvailabilityByPort tupleAvailabilityByPort;

    private THashSet<Object> drainableKeys = new THashSet<>();

    public TupleQueueContainer ( final String operatorId, final int inputPortCount, final int partitionId,
                                 final Function<Boolean, TupleQueue> tupleQueueConstructor )
    {
        this.operatorId = operatorId;
        this.inputPortCount = inputPortCount;
        this.partitionId = partitionId;
        this.tupleQueuesByKeys = new THashMap<>();
        this.tupleQueuesConstructor = ( partitionKey ) ->
        {
            final TupleQueue[] tupleQueues = new TupleQueue[ inputPortCount ];
            for ( int i = 0; i < inputPortCount; i++ )
            {
                final TupleQueue queue = tupleQueueConstructor.apply( false );
                tupleQueues[ i ] = queue;
            }
            return tupleQueues;
        };
        this.tupleCounts = new int[ inputPortCount ];
        Arrays.fill( this.tupleCounts, 1 );
        this.tupleAvailabilityByPort = ANY_PORT;
    }

    public String getOperatorId ()
    {
        return operatorId;
    }

    public int getPartitionId ()
    {
        return partitionId;
    }

    public TupleQueue[] getTupleQueues ( final Object key )
    {
        return tupleQueuesByKeys.computeIfAbsent( key, this.tupleQueuesConstructor );
    }

    public boolean offer ( final int portIndex, final Tuple tuple, final Object key )
    {
        final TupleQueue[] tupleQueues = getTupleQueues( key );
        tupleQueues[ portIndex ].offerTuple( tuple );
        return addToDrainableKeys( key, tupleQueues );
    }

    public boolean forceOffer ( final int portIndex, final Tuple tuple, final Object key )
    {
        final TupleQueue[] tupleQueues = getTupleQueues( key );
        tupleQueues[ portIndex ].forceOfferTuple( tuple );
        return addToDrainableKeys( key, tupleQueues );
    }

    public void drain ( final TupleQueueDrainer drainer )
    {
        if ( drainer instanceof GreedyDrainer )
        {
            final Iterator<Object> it = tupleQueuesByKeys.keySet().iterator();
            while ( it.hasNext() )
            {
                final Object key = it.next();
                final TupleQueue[] tupleQueues = getTupleQueues( key );
                drainer.drain( key, tupleQueues );
                if ( drainer.getResult().isEmpty() )
                {
                    it.remove();
                    drainableKeys.remove( key );
                }
                else
                {
                    return;
                }
            }

            drainer.reset();
        }
        else
        {
            // tuple count based draining
            final Iterator<Object> it = drainableKeys.iterator();
            final boolean drainable = it.hasNext();
            if ( drainable )
            {
                final Object key = it.next();
                final TupleQueue[] tupleQueues = getTupleQueues( key );
                drainer.drain( key, tupleQueues );
                if ( !checkIfDrainable( tupleQueues ) )
                {
                    it.remove();
                }
            }
        }
    }

    public void clear ()
    {
        LOGGER.info( "Clearing partitioned tuple queues of operator: {} partitionId={}", operatorId, partitionId );

        for ( TupleQueue[] tupleQueues : tupleQueuesByKeys.values() )
        {
            for ( TupleQueue tupleQueue : tupleQueues )
            {
                tupleQueue.clear();
            }
        }

        tupleQueuesByKeys.clear();
        drainableKeys.clear();
    }

    public void setTupleCounts ( final int[] tupleCounts, final TupleAvailabilityByPort tupleAvailabilityByPort )
    {
        checkArgument( inputPortCount == tupleCounts.length,
                       "mismatching input port counts for tuple queue container of operator: %s operator input port count: %s, arg: %s",
                       operatorId,
                       inputPortCount,
                       tupleCounts.length );
        checkArgument( tupleAvailabilityByPort == ANY_PORT || tupleAvailabilityByPort == ALL_PORTS,
                       "invalid 5s:%s for tuple queue container of operator: %s operator",
                       TupleAvailabilityByPort.class,
                       tupleAvailabilityByPort,
                       operatorId );
        this.tupleCounts = tupleCounts;
        this.tupleAvailabilityByPort = tupleAvailabilityByPort;
        this.drainableKeys.clear();
        for ( Entry<Object, TupleQueue[]> e : tupleQueuesByKeys.entrySet() )
        {
            if ( checkIfDrainable( e.getValue() ) )
            {
                drainableKeys.add( e.getKey() );
            }
        }
    }

    private boolean addToDrainableKeys ( final Object key, final TupleQueue[] tupleQueues )
    {
        if ( drainableKeys.contains( key ) )
        {
            return true;
        }

        if ( checkIfDrainable( tupleQueues ) )
        {
            drainableKeys.add( key );
            return true;
        }

        return false;
    }

    private boolean checkIfDrainable ( final TupleQueue[] tupleQueues )
    {
        if ( tupleAvailabilityByPort == ANY_PORT )
        {
            for ( int i = 0; i < inputPortCount; i++ )
            {
                if ( tupleQueues[ i ].size() >= tupleCounts[ i ] )
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
                if ( tupleQueues[ i ].size() < tupleCounts[ i ] )
                {
                    return false;
                }
            }

            return true;
        }
    }

}
