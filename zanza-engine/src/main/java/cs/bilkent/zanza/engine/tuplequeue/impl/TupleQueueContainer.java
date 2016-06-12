package cs.bilkent.zanza.engine.tuplequeue.impl;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.StampedLock;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cs.bilkent.zanza.engine.config.ThreadingPreference;
import static cs.bilkent.zanza.engine.config.ThreadingPreference.MULTI_THREADED;
import static cs.bilkent.zanza.engine.config.ThreadingPreference.SINGLE_THREADED;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueue;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueueDrainer;

public class TupleQueueContainer
{

    private static final Logger LOGGER = LoggerFactory.getLogger( TupleQueueContainer.class );


    private final String operatorId;

    private final Function<Object, TupleQueue[]> tupleQueuesConstructor;

    private final Function<Object, TupleQueue[]> tupleQueuesAccessor;

    private final Map<Object, TupleQueue[]> tupleQueuesByKeys;


    private final StampedLock lock = new StampedLock();

    public TupleQueueContainer ( final String operatorId,
                                 final int inputPortCount,
                                 final ThreadingPreference threadingPreference,
                                 final TupleQueueCapacityState tupleQueueCapacityState,
                                 final Function<Boolean, TupleQueue> tupleQueueConstructor )
    {
        this.operatorId = operatorId;
        this.tupleQueuesByKeys = threadingPreference == MULTI_THREADED ? new ConcurrentHashMap<>() : new HashMap<>();

        Function<Object, TupleQueue[]> tupleQueuesConstructor;
        if ( threadingPreference == MULTI_THREADED )
        {
            tupleQueuesConstructor = ( partitionKey ) -> {
                final TupleQueue[] tupleQueues = new TupleQueue[ inputPortCount ];
                for ( int i = 0; i < inputPortCount; i++ )
                {
                    final TupleQueue queue = tupleQueueConstructor.apply( tupleQueueCapacityState.isCapacityCheckEnabled( i ) );
                    tupleQueues[ i ] = queue;
                }
                return tupleQueues;
            };
        }
        else
        {
            tupleQueuesConstructor = ( partitionKey ) -> {
                final TupleQueue[] tupleQueues = new TupleQueue[ inputPortCount ];
                for ( int i = 0; i < inputPortCount; i++ )
                {
                    final TupleQueue queue = tupleQueueConstructor.apply( false );
                    tupleQueues[ i ] = queue;
                }
                return tupleQueues;
            };
        }

        this.tupleQueuesConstructor = tupleQueuesConstructor;

        if ( threadingPreference == SINGLE_THREADED )
        {
            tupleQueuesAccessor = partitionKey -> tupleQueuesByKeys.computeIfAbsent( partitionKey, this.tupleQueuesConstructor );
        }
        else if ( threadingPreference == MULTI_THREADED )
        {
            tupleQueuesAccessor = partitionKey -> {
                TupleQueue[] tupleQueues = tupleQueuesByKeys.get( partitionKey );

                if ( tupleQueues == null )
                {
                    long stamp = lock.tryOptimisticRead();
                    tupleQueues = tupleQueuesByKeys.computeIfAbsent( partitionKey, this.tupleQueuesConstructor );
                    if ( !lock.validate( stamp ) )
                    {
                        stamp = lock.readLock();
                        try
                        {
                            for ( int i = 0; i < inputPortCount; i++ )
                            {
                                if ( tupleQueueCapacityState.isCapacityCheckEnabled( i ) )
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

    public String getOperatorId ()
    {
        return operatorId;
    }

    public TupleQueue[] getTupleQueues ( final Object key )
    {
        return tupleQueuesAccessor.apply( key );
    }

    public void drain ( TupleQueueDrainer drainer )
    {
        // TODO RANDOMIZATION AND PRUNING IS NEEDED HERE !!!
        for ( Map.Entry<Object, TupleQueue[]> entry : tupleQueuesByKeys.entrySet() )
        {
            drainer.drain( entry.getKey(), entry.getValue() );
            if ( drainer.getResult() != null )
            {
                return;
            }
        }
    }

    public void ensureCapacity ( final int portIndex, final int capacity )
    {
        for ( TupleQueue[] tupleQueues : tupleQueuesByKeys.values() )
        {
            tupleQueues[ portIndex ].ensureCapacity( capacity );
        }
    }

    public void clear ()
    {
        LOGGER.info( "Clearing partitioned tuple queues of operator: {}", operatorId );

        for ( TupleQueue[] tupleQueues : tupleQueuesByKeys.values() )
        {
            for ( TupleQueue tupleQueue : tupleQueues )
            {
                tupleQueue.clear();
            }
        }

        tupleQueuesByKeys.clear();
    }

    public void enableCapacityCheck ( final int portIndex )
    {
        final long stamp = lock.writeLock();
        try
        {
            tupleQueuesByKeys.forEach( ( key, queues ) -> {
                queues[ portIndex ].enableCapacityCheck();
            } );
        }
        finally
        {
            lock.unlockWrite( stamp );
        }
    }

    public void disableCapacityCheck ( final int portIndex )
    {
        final long stamp = lock.writeLock();
        try
        {
            tupleQueuesByKeys.forEach( ( key, queues ) -> {
                queues[ portIndex ].disableCapacityCheck();
            } );
        }
        finally
        {
            lock.unlockWrite( stamp );
        }
    }

}
