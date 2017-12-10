package cs.bilkent.joker.engine.tuplequeue.impl.operator;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

import org.junit.Before;
import org.junit.Test;

import static cs.bilkent.joker.engine.partition.PartitionUtil.getPartitionId;
import cs.bilkent.joker.engine.partition.impl.PartitionKeyExtractor1;
import cs.bilkent.joker.engine.tuplequeue.impl.TupleQueueContainer;
import cs.bilkent.joker.engine.tuplequeue.impl.drainer.GreedyDrainer;
import cs.bilkent.joker.operator.Tuple;
import cs.bilkent.joker.operator.Tuples;
import cs.bilkent.joker.operator.impl.TuplesImpl;
import cs.bilkent.joker.partition.impl.PartitionKey;
import cs.bilkent.joker.test.AbstractJokerTest;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class PartitionedOperatorTupleQueueRebalancingTest extends AbstractJokerTest
{

    private static final String OPERATOR_ID = "op1";

    private static final int INPUT_PORT_COUNT = 1;

    private static final int[] PARTITION_DISTRIBUTION = new int[] { 0, 0, 0, 0, 0, 1, 1, 1, 1, 1 };

    private static final int REPLICA_INDEX = 0;

    private static final List<Integer> ACQUIRED_PARTITIONS = asList( 0, 1, 2, 3, 4 );

    private static final List<Integer> NON_ACQUIRED_PARTITIONS = asList( 5, 6, 7, 8, 9 );

    private static final int PARTITION_COUNT = PARTITION_DISTRIBUTION.length;

    private static final String PARTITION_KEY_FIELD = "key";

    private static final PartitionKeyExtractor1 EXTRACTOR = new PartitionKeyExtractor1( singletonList( PARTITION_KEY_FIELD ) );

    private final TupleQueueContainer[] tupleQueueContainers = new TupleQueueContainer[ PARTITION_COUNT ];

    private final Set<Object> keys = new HashSet<>();

    private PartitionedOperatorTupleQueue operatorTupleQueue;

    @Before
    public void init ()
    {
        for ( int partitionId = 0; partitionId < PARTITION_COUNT; partitionId++ )
        {
            tupleQueueContainers[ partitionId ] = new TupleQueueContainer( OPERATOR_ID, INPUT_PORT_COUNT, partitionId );
        }

        operatorTupleQueue = new PartitionedOperatorTupleQueue( OPERATOR_ID,
                                                                INPUT_PORT_COUNT,
                                                                PARTITION_COUNT,
                                                                REPLICA_INDEX,
                                                                100,
                                                                EXTRACTOR,
                                                                tupleQueueContainers, PARTITION_DISTRIBUTION );

        for ( int partitionId : ACQUIRED_PARTITIONS )
        {
            operatorTupleQueue.offer( 0, singletonList( generateTuple( partitionId ) ) );
        }
    }

    @Test
    public void shouldAcquireNewPartitions ()
    {
        final List<TupleQueueContainer> newPartitions = new ArrayList<>();

        for ( int partitionId : NON_ACQUIRED_PARTITIONS )
        {
            final TupleQueueContainer container = tupleQueueContainers[ partitionId ];
            newPartitions.add( container );
            final Tuple tuple = generateTuple( partitionId );
            container.offer( 0, tuple, EXTRACTOR.getPartitionKey( tuple ) );
        }

        operatorTupleQueue.acquirePartitions( newPartitions );

        assertEquals( PARTITION_COUNT, operatorTupleQueue.getDrainablePartitionCount() );

        for ( int partitionId : PARTITION_DISTRIBUTION )
        {
            final Tuple tuple = generateTuple( partitionId );
            operatorTupleQueue.offer( 0, singletonList( tuple ) );
        }

        final int expectedKeyCount = PARTITION_COUNT * 2;

        final List<TuplesImpl> results = new ArrayList<>();
        final Function<PartitionKey, TuplesImpl> tuplesSupplier = key -> {
            final TuplesImpl tuples = new TuplesImpl( 1 );
            results.add( tuples );
            return tuples;
        };
        final GreedyDrainer drainer = new GreedyDrainer( INPUT_PORT_COUNT );
        operatorTupleQueue.drain( drainer, tuplesSupplier );

        int tupleCount = 0;
        for ( Tuples result : results )
        {
            tupleCount += result.getTupleCount( 0 );
        }

        assertEquals( expectedKeyCount, tupleCount );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotAcquireAlreadyAcquiredPartition ()
    {
        final List<TupleQueueContainer> newPartitions = new ArrayList<>();
        newPartitions.add( tupleQueueContainers[ ACQUIRED_PARTITIONS.get( 0 ) ] );

        operatorTupleQueue.acquirePartitions( newPartitions );
    }

    @Test
    public void shouldReleasePartitions ()
    {
        final int releasePartitionIndex = ACQUIRED_PARTITIONS.size() / 2;
        final List<Integer> releasePartitionIds = new ArrayList<>();
        for ( int i = 0; i < releasePartitionIndex; i++ )
        {
            releasePartitionIds.add( ACQUIRED_PARTITIONS.get( i ) );
        }

        final List<TupleQueueContainer> releasedPartitions = operatorTupleQueue.releasePartitions( releasePartitionIds );
        assertReleasedPartitions( releasePartitionIds, releasedPartitions );

        assertEquals( ACQUIRED_PARTITIONS.size() - releasePartitionIds.size(), operatorTupleQueue.getDrainablePartitionCount() );

        for ( int releasedPartitionId : releasePartitionIds )
        {
            try
            {
                operatorTupleQueue.offer( 0, singletonList( generateTuple( releasedPartitionId ) ) );
                fail();
            }
            catch ( NullPointerException ignored )
            {
            }
        }

        for ( int i = releasePartitionIndex; i < ACQUIRED_PARTITIONS.size(); i++ )
        {
            operatorTupleQueue.offer( 0, singletonList( generateTuple( ACQUIRED_PARTITIONS.get( i ) ) ) );
        }

        int expectedKeyCount = ACQUIRED_PARTITIONS.size();
        expectedKeyCount -= releasePartitionIds.size();
        expectedKeyCount += ( ACQUIRED_PARTITIONS.size() - releasePartitionIds.size() );

        final List<TuplesImpl> results = new ArrayList<>();
        final Function<PartitionKey, TuplesImpl> tuplesSupplier = key -> {
            final TuplesImpl tuples = new TuplesImpl( 1 );
            results.add( tuples );
            return tuples;
        };
        final GreedyDrainer drainer = new GreedyDrainer( INPUT_PORT_COUNT );
        operatorTupleQueue.drain( drainer, tuplesSupplier );

        int tupleCount = 0;
        for ( Tuples result : results )
        {
            tupleCount += result.getTupleCount( 0 );
        }

        assertEquals( expectedKeyCount, tupleCount );
    }

    @Test
    public void shouldReleaseAllPartitions ()
    {
        final List<Integer> releasePartitionIds = new ArrayList<>();
        for ( int partitionId : ACQUIRED_PARTITIONS )
        {
            releasePartitionIds.add( partitionId );

        }
        final List<TupleQueueContainer> releasedPartitions = operatorTupleQueue.releasePartitions( releasePartitionIds );
        assertReleasedPartitions( releasePartitionIds, releasedPartitions );

        assertEquals( 0, operatorTupleQueue.getDrainablePartitionCount() );
    }

    private void assertReleasedPartitions ( final List<Integer> releasePartitionIds, final List<TupleQueueContainer> releasedPartitions )
    {
        assertEquals( releasePartitionIds.size(), releasedPartitions.size() );
        for ( int releasePartitionId : releasePartitionIds )
        {
            boolean found = false;
            for ( TupleQueueContainer partition : releasedPartitions )
            {
                if ( partition.getPartitionId() == releasePartitionId )
                {
                    found = true;
                    break;
                }
            }

            assertTrue( found );
        }
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotReleaseNotAcquiredPartition ()
    {
        final List<Integer> releasedPartitionIds = NON_ACQUIRED_PARTITIONS.subList( 0, 1 );

        operatorTupleQueue.releasePartitions( releasedPartitionIds );
    }

    private Tuple generateTuple ( final int partitionId )
    {
        final Tuple tuple = new Tuple();
        int i = 0;
        while ( true )
        {
            if ( keys.contains( i ) )
            {
                i++;
                continue;
            }

            tuple.set( PARTITION_KEY_FIELD, i );
            final int partitionHash = EXTRACTOR.getPartitionHash( tuple );

            if ( getPartitionId( partitionHash, PARTITION_COUNT ) == partitionId )
            {
                keys.add( i );
                return tuple;
            }

            i++;
        }
    }

}
