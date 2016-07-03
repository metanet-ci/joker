package cs.bilkent.zanza.engine.tuplequeue.impl.context;

import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkArgument;
import cs.bilkent.zanza.engine.partition.PartitionKeyFunction;
import static cs.bilkent.zanza.engine.partition.PartitionUtil.getPartitionId;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueue;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueueContext;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueueDrainer;
import cs.bilkent.zanza.engine.tuplequeue.impl.TupleQueueContainer;
import cs.bilkent.zanza.operator.Tuple;


public class PartitionedTupleQueueContext implements TupleQueueContext
{

    private static final Logger LOGGER = LoggerFactory.getLogger( PartitionedTupleQueueContext.class );

    private final String operatorId;

    private final int partitionCount;

    private final PartitionKeyFunction partitionKeyFunction;

    private final TupleQueueContainer[] tupleQueueContainers;

    private final int[] ownedPartitions;

    public PartitionedTupleQueueContext ( final String operatorId,
                                          final int partitionCount,
                                          final int replicaIndex, final PartitionKeyFunction partitionKeyFunction,
                                          final TupleQueueContainer[] tupleQueueContainers,
                                          final int[] partitions )
    {
        checkArgument( partitionCount == tupleQueueContainers.length );
        checkArgument( partitionCount == partitions.length );
        this.operatorId = operatorId;
        this.partitionCount = partitionCount;
        this.partitionKeyFunction = partitionKeyFunction;
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
    public void offer ( final int portIndex, final List<Tuple> tuples )
    {
        for ( Tuple tuple : tuples )
        {
            final TupleQueue[] tupleQueues = getTupleQueues( tuple );
            tupleQueues[ portIndex ].offerTuple( tuple );
        }
    }

    @Override
    public int tryOffer ( final int portIndex, final List<Tuple> tuples, final long timeoutInMillis )
    {
        if ( tuples == null )
        {
            return -1;
        }

        offer( portIndex, tuples );
        return tuples.size();
    }

    @Override
    public void forceOffer ( final int portIndex, final List<Tuple> tuples )
    {
        for ( Tuple tuple : tuples )
        {
            final TupleQueue[] tupleQueues = getTupleQueues( tuple );
            tupleQueues[ portIndex ].forceOffer( tuple );
        }
    }


    TupleQueue[] getTupleQueues ( final Tuple tuple )
    {
        final Object partitionKey = partitionKeyFunction.getPartitionKey( tuple );

        if ( partitionKey == null )
        {
            return null;
        }

        final int partitionId = getPartitionId( partitionKey, partitionCount );

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
        throw new UnsupportedOperationException( getOperatorId() );
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

}
