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

    private final int[] drainIndices;

    private final int[] drainablePartitions;

    private final int maxDrainableKeyCount;

    private int drainablePartitionCount;

    private int nextDrainIndex = NON_DRAINABLE;

    private int drainableKeyCount;

    public PartitionedTupleQueueContext ( final String operatorId,
                                          final int inputPortCount,
                                          final int partitionCount,
                                          final int replicaIndex,
                                          final PartitionKeyFunction partitionKeyFunction,
                                          final TupleQueueContainer[] tupleQueueContainers,
                                          final int[] partitions,
                                          final int maxDrainableKeyCount )
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
        this.maxDrainableKeyCount = maxDrainableKeyCount;
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
        this.drainIndices = new int[ partitionCount ];
        this.drainablePartitions = new int[ ownedPartitionCount ];
        resetDrainIndices();
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
            final boolean newDrainableKey = tupleQueueContainers[ partitionId ].offer( portIndex, tuple, partitionKey );
            if ( newDrainableKey )
            {
                markDrainablePartition( partitionId, 1 );
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
            final boolean newDrainableKey = tupleQueueContainers[ partitionId ].forceOffer( portIndex, tuple, partitionKey );
            if ( newDrainableKey )
            {
                markDrainablePartition( partitionId, 1 );
            }
        }
    }

    @Override
    public void drain ( final TupleQueueDrainer drainer )
    {
        while ( drainablePartitionCount > 0 )
        {
            final int partitionId = drainablePartitions[ nextDrainIndex ];
            final int nonDrainableKeyCount = tupleQueueContainers[ partitionId ].drain( drainer );

            drainableKeyCount -= nonDrainableKeyCount;

            if ( drainer.getResult() != null )
            {
                nextDrainIndex = ( nextDrainIndex + 1 ) % drainablePartitionCount;
                return;
            }
            else
            {
                unmarkDrainablePartition( partitionId, nextDrainIndex );
            }
        }
    }

    private void markDrainablePartition ( final int partitionId, final int newDrainableKeyCount )
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

        drainableKeyCount += newDrainableKeyCount;
    }

    private void unmarkDrainablePartition ( final int partitionId, final int i )
    {
        final int anotherDrainablePartition = drainablePartitions[ --drainablePartitionCount ];
        drainablePartitions[ i ] = anotherDrainablePartition;
        drainIndices[ anotherDrainablePartition ] = i;
        drainIndices[ partitionId ] = NON_DRAINABLE;
        nextDrainIndex = drainablePartitionCount == 0 ? NON_DRAINABLE : ( nextDrainIndex + 1 ) % drainablePartitionCount;
    }

    @Override
    public void ensureCapacity ( final int portIndex, final int capacity )
    {

    }

    @Override
    public void clear ()
    {
        LOGGER.info( "Clearing partitioned tuple queues of operator: {} with drainable key count: ", operatorId, drainableKeyCount );

        for ( TupleQueueContainer container : tupleQueueContainers )
        {
            if ( container != null )
            {
                final int p = container.clear();
                if ( p > 0 )
                {
                    LOGGER.debug( "operator: {} has cleared {} drainable keys in partitionId: {}",
                                  operatorId,
                                  p,
                                  container.getPartitionId() );
                }
            }
        }

        resetDrainIndices();
    }

    private void resetDrainIndices ()
    {
        fill( drainIndices, NON_DRAINABLE );
        fill( drainablePartitions, NON_DRAINABLE );
        nextDrainIndex = NON_DRAINABLE;
        drainablePartitionCount = 0;
        drainableKeyCount = 0;
    }

    @Override
    public void setTupleCounts ( final int[] tupleCounts, final TupleAvailabilityByPort tupleAvailabilityByPort )
    {
        LOGGER.info( "Setting tuple requirements {} , {} for partitioned tuple queues of operator: {}",
                     tupleCounts,
                     tupleAvailabilityByPort,
                     operatorId );

        resetDrainIndices();

        for ( TupleQueueContainer container : tupleQueueContainers )
        {
            if ( container != null )
            {
                final int drainableKeyCount = container.setTupleCounts( tupleCounts, tupleAvailabilityByPort );
                if ( drainableKeyCount > 0 )
                {
                    markDrainablePartition( container.getPartitionId(), drainableKeyCount );
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

    @Override
    public boolean isOverloaded ()
    {
        return drainableKeyCount >= maxDrainableKeyCount;
    }

    int getDrainableKeyCount ()
    {
        return drainableKeyCount;
    }

    boolean isPartitionDrainable ( final int partitionId )
    {
        return drainablePartitions[ partitionId ] != NON_DRAINABLE;
    }

}
