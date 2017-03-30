package cs.bilkent.joker.engine.tuplequeue.impl.operator;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkArgument;
import cs.bilkent.joker.engine.partition.PartitionKey;
import cs.bilkent.joker.engine.partition.PartitionKeyExtractor;
import static cs.bilkent.joker.engine.partition.PartitionUtil.getPartitionId;
import cs.bilkent.joker.engine.tuplequeue.OperatorTupleQueue;
import cs.bilkent.joker.engine.tuplequeue.TupleQueueDrainer;
import cs.bilkent.joker.engine.tuplequeue.impl.TupleQueueContainer;
import cs.bilkent.joker.operator.Tuple;
import cs.bilkent.joker.operator.impl.TuplesImpl;
import cs.bilkent.joker.operator.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByPort;
import static java.lang.Math.min;
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

    private final int tupleQueueCapacity;

    private final PartitionKeyExtractor partitionKeyExtractor;

    private final TupleQueueContainer[] tupleQueueContainers;

    private final int[] drainIndices;

    private final int[] drainablePartitions;

    private final int maxDrainableKeyCount;

    private final int drainHint;

    private final int availableTupleCounts[];

    private int drainablePartitionCount;

    private int nextDrainIndex = NON_DRAINABLE;

    private int totalDrainableKeyCount;

    public PartitionedOperatorTupleQueue ( final String operatorId,
                                           final int inputPortCount,
                                           final int partitionCount,
                                           final int replicaIndex,
                                           final int tupleQueueCapacity,
                                           final PartitionKeyExtractor partitionKeyExtractor,
                                           final TupleQueueContainer[] tupleQueueContainers,
                                           final int[] partitions,
                                           final int maxDrainableKeyCount,
                                           final int drainHint )
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
        checkArgument( drainHint > 0 );
        this.operatorId = operatorId;
        this.replicaIndex = replicaIndex;
        this.inputPortCount = inputPortCount;
        this.partitionCount = partitionCount;
        this.tupleQueueCapacity = tupleQueueCapacity;
        this.partitionKeyExtractor = partitionKeyExtractor;
        this.tupleQueueContainers = copyOf( tupleQueueContainers, partitionCount );
        this.availableTupleCounts = new int[ inputPortCount ];
        this.maxDrainableKeyCount = maxDrainableKeyCount;
        this.drainHint = drainHint;
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
                markDrainablePartition( partitionId, 1 );
            }
        }

        final int count = size - startIndex;
        if ( portIndex >= availableTupleCounts.length )
        {
            System.out.println( "ERROR " + operatorId + " -> rep: " + replicaIndex + " input count: " + inputPortCount + " len: "
                                + availableTupleCounts.length + " p: " + portIndex );
            if ( count == 0 )
            {
                return count;
            }
        }
        availableTupleCounts[ portIndex ] += count;
        return count;
    }

    @Override
    public void drain ( final boolean maySkipBlocking, final TupleQueueDrainer drainer )
    {
        while ( drainablePartitionCount > 0 )
        {
            final int partitionId = drainablePartitions[ nextDrainIndex ];
            final int nonDrainableKeyCount = tupleQueueContainers[ partitionId ].drain( maySkipBlocking, drainer );

            totalDrainableKeyCount -= nonDrainableKeyCount;

            final TuplesImpl result = drainer.getResult();
            if ( result != null )
            {
                for ( int portIndex = 0; portIndex < inputPortCount; portIndex++ )
                {
                    availableTupleCounts[ portIndex ] -= result.getTupleCount( portIndex );
                }

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

        totalDrainableKeyCount += newDrainableKeyCount;
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
        LOGGER.debug( "Clearing partitioned tuple queues of operator: {} with drainable key count: ", operatorId, totalDrainableKeyCount );

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
            if ( availableTupleCounts[ portIndex ] > 0 )
            {
                return false;
            }
        }

        return true;
    }

    @Override
    public void ensureCapacity ( final int capacity )
    {

    }

    @Override
    public int getDrainCountHint ()
    {
        //        if ( !tick )
        //        {
        //            return min( totalDrainableKeyCount, drainHint );
        //        }

        if ( totalDrainableKeyCount >= maxDrainableKeyCount )
        {
            return totalDrainableKeyCount;
        }

        for ( int portIndex = 0; portIndex < inputPortCount; portIndex++ )
        {
            if ( availableTupleCounts[ portIndex ] >= tupleQueueCapacity )
            {
                return totalDrainableKeyCount;
            }
        }

        return min( totalDrainableKeyCount, drainHint );
    }

    int getTotalDrainableKeyCount ()
    {
        return totalDrainableKeyCount;
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
        LOGGER.info( "partitions={} are acquired by operatorId={} replicaIndex={}", partitionIds, operatorId, replicaIndex );
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

        LOGGER.info( "partitions={} are released by operatorId={} replicaIndex={}", partitionIds, operatorId, replicaIndex );

        return released;
    }

    public PartitionKeyExtractor getPartitionKeyExtractor ()
    {
        return partitionKeyExtractor;
    }

    public int getAvailableTupleCount ( final int portIndex )
    {
        return availableTupleCounts[ portIndex ];
    }

    private void populateDrainIndices ()
    {
        resetDrainIndices();

        for ( TupleQueueContainer container : tupleQueueContainers )
        {
            if ( container != null )
            {
                final int drainableKeyCount = container.getDrainableKeyCount();
                if ( drainableKeyCount > 0 )
                {
                    markDrainablePartition( container.getPartitionId(), drainableKeyCount );
                }

                container.addAvailableTupleCounts( availableTupleCounts );
            }
        }
    }

    private void resetDrainIndices ()
    {
        fill( drainIndices, NON_DRAINABLE );
        fill( drainablePartitions, NON_DRAINABLE );
        fill( availableTupleCounts, 0 );
        nextDrainIndex = NON_DRAINABLE;
        drainablePartitionCount = 0;
        totalDrainableKeyCount = 0;
    }

}
