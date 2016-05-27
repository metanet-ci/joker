package cs.bilkent.zanza.engine.tuplequeue.impl.context;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.locks.StampedLock;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkState;
import cs.bilkent.zanza.engine.config.ThreadingPreference;
import static cs.bilkent.zanza.engine.config.ThreadingPreference.MULTI_THREADED;
import static cs.bilkent.zanza.engine.config.ThreadingPreference.SINGLE_THREADED;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueue;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueueDrainer;
import cs.bilkent.zanza.operator.Tuple;


public class PartitionedTupleQueueContext extends AbstractTupleQueueContext
{

    private static final Logger LOGGER = LoggerFactory.getLogger( PartitionedTupleQueueContext.class );

    private static final int CAPACITY_CHECK_ENABLED = 0, CAPACITY_CHECK_DISABLED = 1;


    private final ThreadingPreference threadingPreference;

    private final Function<Tuple, Object> partitionKeyExtractor;

    private final Function<Object, TupleQueue[]> tupleQueuesConstructor;

    private final Function<Object, TupleQueue[]> tupleQueuesAccessor;

    private final Map<Object, TupleQueue[]> tupleQueuesByPartitionKeys;

    private final AtomicIntegerArray capacityCheckFlags;

    private final StampedLock lock = new StampedLock();

    public PartitionedTupleQueueContext ( final String operatorId,
                                          final int inputPortCount,
                                          final ThreadingPreference threadingPreference,
                                          final Function<Tuple, Object> partitionKeyExtractor,
                                          final Function<Boolean, TupleQueue> tupleQueueConstructor )
    {
        super( operatorId, inputPortCount );
        this.threadingPreference = threadingPreference;
        this.partitionKeyExtractor = partitionKeyExtractor;
        this.capacityCheckFlags = new AtomicIntegerArray( inputPortCount );
        this.tupleQueuesByPartitionKeys = threadingPreference == MULTI_THREADED ? new ConcurrentHashMap<>() : new HashMap<>();

        this.tupleQueuesConstructor = ( partitionKey ) -> {
            final TupleQueue[] tupleQueues = new TupleQueue[ inputPortCount ];
            for ( int i = 0; i < inputPortCount; i++ )
            {
                final TupleQueue queue = tupleQueueConstructor.apply( isCapacityCheckEnabled( i ) );
                tupleQueues[ i ] = queue;
            }
            return tupleQueues;
        };

        if ( threadingPreference == SINGLE_THREADED )
        {
            tupleQueuesAccessor = partitionKey -> tupleQueuesByPartitionKeys.computeIfAbsent( partitionKey, tupleQueuesConstructor );
        }
        else if ( threadingPreference == MULTI_THREADED )
        {
            tupleQueuesAccessor = partitionKey -> {
                TupleQueue[] tupleQueues = tupleQueuesByPartitionKeys.get( partitionKey );

                if ( tupleQueues == null )
                {
                    long stamp = lock.tryOptimisticRead();
                    tupleQueues = tupleQueuesByPartitionKeys.computeIfAbsent( partitionKey, tupleQueuesConstructor );
                    if ( !lock.validate( stamp ) )
                    {
                        stamp = lock.readLock();
                        try
                        {
                            for ( int i = 0; i < inputPortCount; i++ )
                            {
                                if ( capacityCheckFlags.get( i ) == CAPACITY_CHECK_ENABLED )
                                {
                                    tupleQueues[ i ].enableCapacityCheck();
                                }
                                else
                                {
                                    tupleQueues[ i ].disableCapacityCheck();
                                }
                            }
                        }
                        finally
                        {
                            lock.unlockRead( stamp );
                        }
                    }
                }

                return tupleQueues;
            };
        }
        else
        {
            throw new IllegalArgumentException( "Wrong threading preference: " + threadingPreference.toString() + " for operatorId="
                                                + operatorId );
        }
    }

    @Override
    public String getOperatorId ()
    {
        return operatorId;
    }

    @Override
    protected TupleQueue[] getTupleQueues ( final List<Tuple> tuples )
    {
        if ( tuples == null || tuples.isEmpty() )
        {
            return null;
        }

        return getTupleQueues( tuples.get( 0 ) );
    }

    protected TupleQueue[] getTupleQueues ( final Tuple tuple )
    {
        final Object partitionKey = partitionKeyExtractor.apply( tuple );

        if ( partitionKey == null )
        {
            return null;
        }

        return tupleQueuesAccessor.apply( partitionKey );
    }

    @Override
    public void drain ( TupleQueueDrainer drainer )
    {
        // TODO RANDOMIZATION IS NEEDED HERE !!!
        for ( Map.Entry<Object, TupleQueue[]> entry : tupleQueuesByPartitionKeys.entrySet() )
        {
            drainer.drain( entry.getKey(), entry.getValue() );
            if ( drainer.getResult() != null )
            {
                return;
            }
        }
    }

    @Override
    public void ensureCapacity ( final int portIndex, final int capacity )
    {
        for ( TupleQueue[] tupleQueues : tupleQueuesByPartitionKeys.values() )
        {
            tupleQueues[ portIndex ].ensureCapacity( capacity );
        }
    }

    @Override
    public void clear ()
    {
        LOGGER.info( "Clearing partitioned tuple queues of operator: {}", operatorId );

        for ( TupleQueue[] tupleQueues : tupleQueuesByPartitionKeys.values() )
        {
            for ( TupleQueue tupleQueue : tupleQueues )
            {
                tupleQueue.clear();
            }
        }

        tupleQueuesByPartitionKeys.clear();
    }

    @Override
    public void enableCapacityCheck ( final int portIndex )
    {
        checkState( threadingPreference == MULTI_THREADED );

        final long stamp = lock.writeLock();
        try
        {
            capacityCheckFlags.set( portIndex, CAPACITY_CHECK_ENABLED );
            tupleQueuesByPartitionKeys.forEach( ( key, queues ) -> {
                queues[ portIndex ].enableCapacityCheck();
            } );
        }
        finally
        {
            lock.unlockWrite( stamp );
        }
    }

    @Override
    public void disableCapacityCheck ( final int portIndex )
    {
        checkState( threadingPreference == MULTI_THREADED );

        final long stamp = lock.writeLock();
        try
        {
            capacityCheckFlags.set( portIndex, CAPACITY_CHECK_DISABLED );
            tupleQueuesByPartitionKeys.forEach( ( key, queues ) -> {
                queues[ portIndex ].disableCapacityCheck();
            } );
        }
        finally
        {
            lock.unlockWrite( stamp );
        }
    }

    @Override
    public boolean isCapacityCheckEnabled ( final int portIndex )
    {
        checkState( threadingPreference == MULTI_THREADED );
        return capacityCheckFlags.get( portIndex ) == CAPACITY_CHECK_ENABLED;
    }

    @Override
    public boolean isCapacityCheckDisabled ( final int portIndex )
    {
        checkState( threadingPreference == MULTI_THREADED );
        return capacityCheckFlags.get( portIndex ) == CAPACITY_CHECK_DISABLED;
    }

}
