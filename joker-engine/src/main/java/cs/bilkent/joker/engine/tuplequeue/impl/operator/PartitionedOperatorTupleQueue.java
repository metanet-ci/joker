package cs.bilkent.joker.engine.tuplequeue.impl.operator;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkArgument;
import cs.bilkent.joker.engine.partition.PartitionKeyExtractor;
import static cs.bilkent.joker.engine.partition.PartitionUtil.getPartitionId;
import cs.bilkent.joker.engine.tuplequeue.OperatorTupleQueue;
import cs.bilkent.joker.engine.tuplequeue.TupleQueueDrainer;
import cs.bilkent.joker.engine.tuplequeue.impl.TupleQueueContainer;
import cs.bilkent.joker.operator.Tuple;
import cs.bilkent.joker.operator.impl.TuplesImpl;
import cs.bilkent.joker.operator.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByPort;
import cs.bilkent.joker.partition.impl.PartitionKey;
import static java.util.Arrays.copyOf;
import static java.util.Arrays.fill;


public class PartitionedOperatorTupleQueue implements OperatorTupleQueue
{

    private static final Logger LOGGER = LoggerFactory.getLogger( PartitionedOperatorTupleQueue.class );

    private static final int NON_DRAINABLE = -1;


    private final String operatorId;

    private final int replicaIndex;

    private final int inputPortCount;

    private final int partitionCount;

    private final PartitionKeyExtractor partitionKeyExtractor;

    private final TupleQueueContainer[] tupleQueueContainers;

    private final int[] drainIndices;

    private final int[] drainablePartitions;

    private int drainablePartitionCount;

    private int nextDrainIndex = NON_DRAINABLE;

    public PartitionedOperatorTupleQueue ( final String operatorId,
                                           final int inputPortCount,
                                           final int partitionCount,
                                           final int replicaIndex,
                                           final int tupleQueueCapacity,
                                           final PartitionKeyExtractor partitionKeyExtractor,
                                           final TupleQueueContainer[] tupleQueueContainers, final int[] partitions )
    {
        checkArgument( partitionCount == tupleQueueContainers.length,
                       "mismatching partition count %s and tuple queue container count %s partitioned tuple queue of for operator" + " %s",
                       partitionCount,
                       tupleQueueContainers.length,
                       operatorId );
        checkArgument( partitionCount == partitions.length,
                       "mismatching partition count %s and partition distribution count %s for partitioned tuple queue of " + "operator %s",
                       partitionCount,
                       partitions.length,
                       operatorId );
        checkArgument( inputPortCount >= 0, "invalid input port count %s for partitioned tuple queue of operator $s",
                       inputPortCount,
                       operatorId );
        checkArgument( tupleQueueCapacity > 0 );
        this.operatorId = operatorId;
        this.replicaIndex = replicaIndex;
        this.inputPortCount = inputPortCount;
        this.partitionCount = partitionCount;
        this.partitionKeyExtractor = partitionKeyExtractor;
        this.tupleQueueContainers = copyOf( tupleQueueContainers, partitionCount );
        for ( int i = 0; i < partitionCount; i++ )
        {
            if ( partitions[ i ] != replicaIndex )
            {
                this.tupleQueueContainers[ i ] = null;
            }
        }
        this.drainIndices = new int[ partitionCount ];
        this.drainablePartitions = new int[ partitionCount ];
        populateDrainIndices();
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
            final int partitionId = getPartitionId( partitionKey.partitionHashCode(), partitionCount );
            final boolean newDrainableKey = tupleQueueContainers[ partitionId ].offer( portIndex, tuple, partitionKey );
            if ( newDrainableKey )
            {
                markDrainablePartition( partitionId );
            }
        }

        return ( size - startIndex );
    }

    @Override
    public void drain ( final boolean maySkipBlocking,
                        final TupleQueueDrainer drainer,
                        final Function<PartitionKey, TuplesImpl> tuplesSupplier )
    {
        while ( drainablePartitionCount > 0 )
        {
            final int partitionId = drainablePartitions[ nextDrainIndex ];
            if ( tupleQueueContainers[ partitionId ].drain( maySkipBlocking, drainer, tuplesSupplier ) )
            {
                nextDrainIndex = ( nextDrainIndex + 1 ) % drainablePartitionCount;
            }
            else
            {
                unmarkDrainablePartition( partitionId, nextDrainIndex );
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

    private void unmarkDrainablePartition ( final int partitionId, final int i )
    {
        final int anotherDrainablePartition = drainablePartitions[ --drainablePartitionCount ];
        drainablePartitions[ i ] = anotherDrainablePartition;
        drainIndices[ anotherDrainablePartition ] = i;
        drainIndices[ partitionId ] = NON_DRAINABLE;
        nextDrainIndex = drainablePartitionCount == 0 ? NON_DRAINABLE : ( nextDrainIndex + 1 ) % drainablePartitionCount;
    }

    @Override
    public void clear ()
    {
        LOGGER.debug( "Clearing partitioned tuple queues of operator: {}", operatorId );

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

    @Override
    public void setTupleCounts ( final int[] tupleCounts, final TupleAvailabilityByPort tupleAvailabilityByPort )
    {
        LOGGER.info( "Setting tuple requirements {} , {} for partitioned tuple queues of operator: {}",
                     tupleCounts,
                     tupleAvailabilityByPort,
                     operatorId );

        for ( TupleQueueContainer container : tupleQueueContainers )
        {
            if ( container != null )
            {
                container.setTupleCounts( tupleCounts, tupleAvailabilityByPort );
            }
        }

        populateDrainIndices();
    }

    @Override
    public boolean isEmpty ()
    {
        for ( int portIndex = 0; portIndex < getInputPortCount(); portIndex++ )
        {
            for ( TupleQueueContainer container : tupleQueueContainers )
            {
                if ( !container.isEmpty() )
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

    public void acquirePartitions ( final List<TupleQueueContainer> partitions )
    {
        checkArgument( partitions != null );

        for ( TupleQueueContainer partition : partitions )
        {
            checkArgument( this.tupleQueueContainers[ partition.getPartitionId() ] == null,
                           "partitionId=% is already acquired. operatorId=%s replicaIndex=%s",
                           partition.getPartitionId(),
                           operatorId,
                           replicaIndex );
        }

        for ( TupleQueueContainer partition : partitions )
        {
            tupleQueueContainers[ partition.getPartitionId() ] = partition;
        }

        populateDrainIndices();

        final int[] partitionIds = partitions.stream().mapToInt( TupleQueueContainer::getPartitionId ).toArray();
        LOGGER.debug( "partitions={} are acquired by operatorId={} replicaIndex={}", partitionIds, operatorId, replicaIndex );
    }

    public List<TupleQueueContainer> releasePartitions ( final List<Integer> partitionIds )
    {
        checkArgument( partitionIds != null,
                       "cannot release null partition ids of operatorId=%s replicaIndex=%s",
                       operatorId,
                       replicaIndex );

        for ( int partitionId : partitionIds )
        {
            checkArgument( this.tupleQueueContainers[ partitionId ] != null, "partitionId=% is not acquired. operatorId=%s replicaIndex=%s",
                           partitionId,
                           operatorId,
                           replicaIndex );
        }

        List<TupleQueueContainer> released = new ArrayList<>( partitionIds.size() );
        for ( int partitionId : partitionIds )
        {
            released.add( tupleQueueContainers[ partitionId ] );
            tupleQueueContainers[ partitionId ] = null;
        }

        populateDrainIndices();

        LOGGER.debug( "partitions={} are released by operatorId={} replicaIndex={}", partitionIds, operatorId, replicaIndex );

        return released;
    }

    public PartitionKeyExtractor getPartitionKeyExtractor ()
    {
        return partitionKeyExtractor;
    }

    public int getDrainablePartitionCount ()
    {
        return drainablePartitionCount;
    }

    private void populateDrainIndices ()
    {
        resetDrainIndices();

        for ( TupleQueueContainer container : tupleQueueContainers )
        {
            if ( container != null )
            {
                if ( container.getDrainableKeyCount() > 0 )
                {
                    markDrainablePartition( container.getPartitionId() );
                }
            }
        }
    }

    private void resetDrainIndices ()
    {
        fill( drainIndices, NON_DRAINABLE );
        fill( drainablePartitions, NON_DRAINABLE );
        nextDrainIndex = NON_DRAINABLE;
        drainablePartitionCount = 0;
    }

}
