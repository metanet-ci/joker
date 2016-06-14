package cs.bilkent.zanza.engine.tuplequeue.impl;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cs.bilkent.zanza.engine.tuplequeue.TupleQueue;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueueDrainer;

public class TupleQueueContainer
{

    private static final Logger LOGGER = LoggerFactory.getLogger( TupleQueueContainer.class );


    private final String operatorId;

    private final Function<Object, TupleQueue[]> tupleQueuesConstructor;

    private final Function<Object, TupleQueue[]> tupleQueuesAccessor;

    private final Map<Object, TupleQueue[]> tupleQueuesByKeys;

    public TupleQueueContainer ( final String operatorId,
                                 final int inputPortCount,
                                 final Function<Boolean, TupleQueue> tupleQueueConstructor )
    {
        this.operatorId = operatorId;
        this.tupleQueuesByKeys = new HashMap<>();
        this.tupleQueuesConstructor = ( partitionKey ) -> {
            final TupleQueue[] tupleQueues = new TupleQueue[ inputPortCount ];
            for ( int i = 0; i < inputPortCount; i++ )
            {
                final TupleQueue queue = tupleQueueConstructor.apply( false );
                tupleQueues[ i ] = queue;
            }
            return tupleQueues;
        };
        tupleQueuesAccessor = partitionKey -> tupleQueuesByKeys.computeIfAbsent( partitionKey, this.tupleQueuesConstructor );
    }

    public String getOperatorId ()
    {
        return operatorId;
    }

    public TupleQueue[] getTupleQueues ( final Object key )
    {
        return tupleQueuesAccessor.apply( key );
    }

    public void drain ( final TupleQueueDrainer drainer )
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

}
