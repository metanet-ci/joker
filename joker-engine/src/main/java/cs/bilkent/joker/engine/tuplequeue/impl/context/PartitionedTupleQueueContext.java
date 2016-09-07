package cs.bilkent.joker.engine.tuplequeue.impl.context;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkArgument;
import cs.bilkent.joker.engine.partition.PartitionKeyFunction;
import static cs.bilkent.joker.engine.partition.PartitionUtil.getPartitionId;
import cs.bilkent.joker.engine.tuplequeue.TupleQueueContext;
import cs.bilkent.joker.engine.tuplequeue.TupleQueueDrainer;
import cs.bilkent.joker.engine.tuplequeue.impl.TupleQueueContainer;
import cs.bilkent.joker.operator.Tuple;
import cs.bilkent.joker.operator.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByPort;
import static java.util.Arrays.copyOf;
import static java.util.Arrays.fill;


public class PartitionedTupleQueueContext implements TupleQueueContext
{

    private static final Logger LOGGER = LoggerFactory.getLogger( PartitionedTupleQueueContext.class );

    private static final int NON_DRAINABLE = -1;


    private final String operatorId;

    private final int inputPortCount;

    private final int partitionCount;

    private final PartitionKeyFunction partitionKeyFunction;

    private final TupleQueueContainer[] tupleQueueContainers;

    private final int[] ownedPartitions;

    // TODO maintain an array instead of this. array would work fine since partitions are bounded by partition count.
    // TODO same method with randomly-selectable hashmap solution

    private final int[] drainIndices;

    private final int[] drainablePartitions;

    private int drainablePartitionCount;

    private int nextDrainIndex;

    public PartitionedTupleQueueContext ( final String operatorId,
                                          final int inputPortCount,
                                          final int partitionCount,
                                          final int replicaIndex,
                                          final PartitionKeyFunction partitionKeyFunction,
                                          final TupleQueueContainer[] tupleQueueContainers,
                                          final int[] partitions )
    {
        checkArgument( partitionCount == tupleQueueContainers.length,
                       "mismatching partition count %s and tuple queue container count %s partitioned tuple queue context of for operator"
                       + " %s",
                       partitionCount,
                       tupleQueueContainers.length,
                       operatorId );
        checkArgument( partitionCount == partitions.length,
                       "mismatching partition count %s and partition distribution count %s for partitioned tuple queue context of "
                       + "operator %s",
                       partitionCount,
                       partitions.length,
                       operatorId );
        checkArgument( inputPortCount >= 0,
                       "invalid input port count %s for partitioned tuple queue context of operator $s",
                       inputPortCount,
                       operatorId );
        this.operatorId = operatorId;
        this.inputPortCount = inputPortCount;
        this.partitionCount = partitionCount;
        this.partitionKeyFunction = partitionKeyFunction;
        this.tupleQueueContainers = copyOf( tupleQueueContainers, partitionCount );
        int ownedPartitionCount = 0;
        for ( int i = 0; i < partitionCount; i++ )
        {
            if ( partitions[ i ] == replicaIndex )
            {
                ownedPartitionCount++;
            }
            else
            {
                this.tupleQueueContainers[ i ] = null;
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
        this.drainIndices = new int[ partitionCount ];
        this.drainablePartitions = new int[ ownedPartitionCount ];
        clearDrainIndices();
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
    public void offer ( final int portIndex, final List<Tuple> tuples )
    {
        for ( Tuple tuple : tuples )
        {
            final Object partitionKey = partitionKeyFunction.getPartitionKey( tuple );
            final int partitionId = getPartitionId( partitionKey, partitionCount );
            final boolean drainable = tupleQueueContainers[ partitionId ].offer( portIndex, tuple, partitionKey );
            if ( drainable )
            {
                markDrainablePartition( partitionId );
            }
        }
    }

    @Override
    public int tryOffer ( final int portIndex, final List<Tuple> tuples, final long timeout, final TimeUnit unit )
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
            final Object partitionKey = partitionKeyFunction.getPartitionKey( tuple );
            final int partitionId = getPartitionId( partitionKey, partitionCount );
            final boolean drainable = tupleQueueContainers[ partitionId ].forceOffer( portIndex, tuple, partitionKey );
            if ( drainable )
            {
                markDrainablePartition( partitionId );
            }
        }
    }

    @Override
    public void drain ( final TupleQueueDrainer drainer )
    {
        while ( drainablePartitionCount > 0 )
        {
            final int partitionId = drainablePartitions[ nextDrainIndex ];
            tupleQueueContainers[ partitionId ].drain( drainer );

            if ( drainer.getResult() != null )
            {
                nextDrainIndex = ( nextDrainIndex + 1 ) % drainablePartitionCount;
                return;
            }
            else
            {
                unmarkNonDrainablePartition( partitionId );
            }
        }
    }

    private void markDrainablePartition ( final int partitionId )
    {
        if ( drainIndices[ partitionId ] == NON_DRAINABLE )
        {
            drainIndices[ partitionId ] = drainablePartitionCount;
            drainablePartitions[ drainablePartitionCount++ ] = partitionId;
            if ( nextDrainIndex == NON_DRAINABLE )
            {
                nextDrainIndex = 0;
            }
        }
    }

    private void unmarkNonDrainablePartition ( final int partitionId )
    {
        final int anotherDrainablePartition = drainablePartitions[ --drainablePartitionCount ];
        drainablePartitions[ nextDrainIndex ] = anotherDrainablePartition;
        drainIndices[ partitionId ] = NON_DRAINABLE;
        drainIndices[ anotherDrainablePartition ] = nextDrainIndex;
        nextDrainIndex = drainablePartitionCount == 0 ? NON_DRAINABLE : ( nextDrainIndex + 1 ) % drainablePartitionCount;
    }

    @Override
    public void ensureCapacity ( final int portIndex, final int capacity )
    {

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

        clearDrainIndices();
    }

    private void clearDrainIndices ()
    {
        fill( drainIndices, NON_DRAINABLE );
        fill( drainablePartitions, NON_DRAINABLE );
        drainablePartitionCount = 0;
    }

    @Override
    public void setTupleCounts ( final int[] tupleCounts, final TupleAvailabilityByPort tupleAvailabilityByPort )
    {
        LOGGER.info( "Setting tuple requirements {} , {} for partitioned tuple queues of operator: {}",
                     tupleCounts,
                     tupleAvailabilityByPort,
                     operatorId );

        clearDrainIndices();

        for ( TupleQueueContainer container : tupleQueueContainers )
        {
            if ( container != null )
            {
                if ( container.setTupleCounts( tupleCounts, tupleAvailabilityByPort ) )
                {
                    markDrainablePartition( container.getPartitionId() );
                }
            }
        }
    }

    @Override
    public void enableCapacityCheck ( final int portIndex )
    {

    }

    @Override
    public void disableCapacityCheck ( final int portIndex )
    {

    }

    @Override
    public boolean isCapacityCheckEnabled ( final int portIndex )
    {
        return false;
    }

}
