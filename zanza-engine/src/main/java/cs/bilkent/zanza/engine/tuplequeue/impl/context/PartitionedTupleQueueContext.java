package cs.bilkent.zanza.engine.tuplequeue.impl.context;

import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkArgument;
import cs.bilkent.zanza.engine.partition.PartitionKeyFunction;
import static cs.bilkent.zanza.engine.partition.PartitionUtil.getPartitionId;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueueContext;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueueDrainer;
import cs.bilkent.zanza.engine.tuplequeue.impl.TupleQueueContainer;
import cs.bilkent.zanza.operator.Tuple;
import cs.bilkent.zanza.operator.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByPort;
import gnu.trove.iterator.TIntIterator;
import gnu.trove.set.hash.TIntHashSet;


public class PartitionedTupleQueueContext implements TupleQueueContext
{

    private static final Logger LOGGER = LoggerFactory.getLogger( PartitionedTupleQueueContext.class );

    private final String operatorId;

    private final int inputPortCount;

    private final int partitionCount;

    private final PartitionKeyFunction partitionKeyFunction;

    private final TupleQueueContainer[] tupleQueueContainers;

    private final int[] ownedPartitions;

    private final TIntHashSet drainablePartitions = new TIntHashSet();

    public PartitionedTupleQueueContext ( final String operatorId,
                                          final int inputPortCount,
                                          final int partitionCount,
                                          final int replicaIndex,
                                          final PartitionKeyFunction partitionKeyFunction,
                                          final TupleQueueContainer[] tupleQueueContainers,
                                          final int[] partitions )
    {
        checkArgument( partitionCount == tupleQueueContainers.length );
        checkArgument( partitionCount == partitions.length );
        checkArgument( inputPortCount >= 0 );
        this.operatorId = operatorId;
        this.inputPortCount = inputPortCount;
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
                drainablePartitions.add( partitionId );
            }
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
            final Object partitionKey = partitionKeyFunction.getPartitionKey( tuple );
            final int partitionId = getPartitionId( partitionKey, partitionCount );
            final boolean drainable = tupleQueueContainers[ partitionId ].forceOffer( portIndex, tuple, partitionKey );
            if ( drainable )
            {
                drainablePartitions.add( partitionId );
            }
        }
    }

    @Override
    public void drain ( final TupleQueueDrainer drainer )
    {
        final TIntIterator it = drainablePartitions.iterator();
        while ( it.hasNext() )
        {
            final int partitionId = it.next();
            tupleQueueContainers[ partitionId ].drain( drainer );

            if ( drainer.getResult() != null )
            {
                return;
            }
            else
            {
                it.remove();
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
    }

    @Override
    public void prepareGreedyDraining ()
    {
        LOGGER.info( "Operator:{} prepares greedy draining for partitions: {}", operatorId, ownedPartitions );

        for ( final int partitionId : ownedPartitions )
        {
            drainablePartitions.add( partitionId );
        }
    }

}
