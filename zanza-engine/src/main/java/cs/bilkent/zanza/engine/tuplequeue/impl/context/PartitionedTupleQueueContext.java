package cs.bilkent.zanza.engine.tuplequeue.impl.context;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import cs.bilkent.zanza.engine.config.ThreadingPreference;
import static cs.bilkent.zanza.engine.config.ThreadingPreference.MULTI_THREADED;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueue;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueueDrainer;
import cs.bilkent.zanza.engine.tuplequeue.impl.TupleQueueCapacityState;
import cs.bilkent.zanza.engine.tuplequeue.impl.TupleQueueContainer;
import cs.bilkent.zanza.operator.Tuple;


public class PartitionedTupleQueueContext extends AbstractTupleQueueContext
{

    private static final Logger LOGGER = LoggerFactory.getLogger( PartitionedTupleQueueContext.class );


    private final int partitionCount;

    private final ThreadingPreference threadingPreference;

    private final Function<Tuple, Object> partitionKeyExtractor;

    private final TupleQueueCapacityState tupleQueueCapacityState;

    private final TupleQueueContainer[] tupleQueueContainers;

    private final int[] ownedPartitions;

    private final Object monitor = new Object();

    public PartitionedTupleQueueContext ( final String operatorId,
                                          final int inputPortCount,
                                          final int partitionCount,
                                          final int replicaIndex,
                                          final ThreadingPreference threadingPreference,
                                          final Function<Tuple, Object> partitionKeyExtractor,
                                          final TupleQueueCapacityState tupleQueueCapacityState,
                                          final TupleQueueContainer[] tupleQueueContainers,
                                          final int[] partitions )
    {
        super( operatorId, inputPortCount );
        checkArgument( partitionCount == tupleQueueContainers.length );
        checkArgument( partitionCount == partitions.length );
        this.partitionCount = partitionCount;
        this.threadingPreference = threadingPreference;
        this.partitionKeyExtractor = partitionKeyExtractor;
        this.tupleQueueCapacityState = tupleQueueCapacityState;
        this.tupleQueueContainers = Arrays.copyOf( tupleQueueContainers, partitionCount );
        int ownedPartitionCount = 0;
        for ( int i = 0; i < partitionCount; i++ )
        {
            if ( partitions[ i ] == replicaIndex )
            {
                ownedPartitionCount++;
            }
            else
            {
                tupleQueueContainers[ i ] = null;
            }
        }
        this.ownedPartitions = new int[ ownedPartitionCount ];
        for ( int i = 0, j = 0; i < partitionCount; i++ )
        {
            if ( partitions[ i ] == replicaIndex )
            {
                ownedPartitions[ j++ ] = i;
            }
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

    TupleQueue[] getTupleQueues ( final Tuple tuple )
    {
        final Object partitionKey = partitionKeyExtractor.apply( tuple );

        if ( partitionKey == null )
        {
            return null;
        }

        int partitionId = partitionKey.hashCode() % partitionCount;
        if ( partitionId < 0 )
        {
            partitionId += partitionCount;
        }

        return tupleQueueContainers[ partitionId ].getTupleQueues( partitionKey );
    }

    @Override
    public void drain ( TupleQueueDrainer drainer )
    {
        // TODO RANDOMIZATION AND PRUNING IS NEEDED HERE !!!

        for ( int i = 0; i < ownedPartitions.length; i++ )
        {
            tupleQueueContainers[ ownedPartitions[ i ] ].drain( drainer );
            if ( drainer.getResult() != null )
            {
                return;
            }
        }
    }

    @Override
    public void ensureCapacity ( final int portIndex, final int capacity )
    {
        for ( int i = 0; i < ownedPartitions.length; i++ )
        {
            tupleQueueContainers[ ownedPartitions[ i ] ].ensureCapacity( portIndex, capacity );
        }
    }

    @Override
    public void clear ()
    {
        LOGGER.info( "Clearing partitioned tuple queues of operator: {}", operatorId );

        for ( TupleQueueContainer container : tupleQueueContainers )
        {
            if ( container != null )
            {
                container.clear();
            }
        }
    }

    @Override
    public void enableCapacityCheck ( final int portIndex )
    {
        checkState( threadingPreference == MULTI_THREADED );

        synchronized ( monitor )
        {
            tupleQueueCapacityState.enableCapacityCheck( portIndex );
            for ( TupleQueueContainer container : tupleQueueContainers )
            {
                if ( container != null )
                {
                    container.disableCapacityCheck( portIndex );
                }
            }
        }
    }

    @Override
    public void disableCapacityCheck ( final int portIndex )
    {
        checkState( threadingPreference == MULTI_THREADED );

        synchronized ( monitor )
        {
            tupleQueueCapacityState.disableCapacityCheck( portIndex );
            for ( TupleQueueContainer container : tupleQueueContainers )
            {
                if ( container != null )
                {
                    container.disableCapacityCheck( portIndex );
                }
            }
        }
    }

    @Override
    public boolean isCapacityCheckEnabled ( final int portIndex )
    {
        checkState( threadingPreference == MULTI_THREADED );
        return tupleQueueCapacityState.isCapacityCheckEnabled( portIndex );
    }

    @Override
    public boolean isCapacityCheckDisabled ( final int portIndex )
    {
        checkState( threadingPreference == MULTI_THREADED );
        return tupleQueueCapacityState.isCapacityCheckDisabled( portIndex );
    }

}
