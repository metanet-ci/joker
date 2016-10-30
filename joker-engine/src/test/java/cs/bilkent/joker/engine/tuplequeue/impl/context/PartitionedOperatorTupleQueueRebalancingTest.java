package cs.bilkent.joker.engine.tuplequeue.impl.context;

import java.util.HashSet;
import java.util.Set;
import java.util.function.BiFunction;

import org.junit.Before;
import org.junit.Test;

import static cs.bilkent.joker.engine.partition.PartitionUtil.getPartitionId;
import cs.bilkent.joker.engine.partition.impl.PartitionKeyExtractor1;
import cs.bilkent.joker.engine.tuplequeue.TupleQueue;
import cs.bilkent.joker.engine.tuplequeue.impl.TupleQueueContainer;
import cs.bilkent.joker.engine.tuplequeue.impl.drainer.GreedyDrainer;
import cs.bilkent.joker.engine.tuplequeue.impl.queue.SingleThreadedTupleQueue;
import cs.bilkent.joker.operator.Tuple;
import cs.bilkent.joker.operator.impl.TuplesImpl;
import cs.bilkent.joker.testutils.AbstractJokerTest;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class PartitionedOperatorTupleQueueRebalancingTest extends AbstractJokerTest
{

    private static final String OPERATOR_ID = "op1";

    private static final int INPUT_PORT_COUNT = 1;

    private static final int[] PARTITION_DISTRIBUTION = new int[] { 0, 0, 0, 0, 0, 1, 1, 1, 1, 1 };

    private static final int REPLICA_INDEX = 0;

    private static final int[] ACQUIRED_PARTITIONS = new int[] { 0, 1, 2, 3, 4 };

    private static final int[] NON_ACQUIRED_PARTITIONS = new int[] { 5, 6, 7, 8, 9 };

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
            final BiFunction<Integer, Boolean, TupleQueue> constructor = ( i, b ) -> new SingleThreadedTupleQueue( 100 );
            tupleQueueContainers[ partitionId ] = new TupleQueueContainer( OPERATOR_ID, INPUT_PORT_COUNT, partitionId, constructor );
        }

        operatorTupleQueue = new PartitionedOperatorTupleQueue( OPERATOR_ID,
                                                                INPUT_PORT_COUNT,
                                                                PARTITION_COUNT,
                                                                REPLICA_INDEX,
                                                                EXTRACTOR,
                                                                tupleQueueContainers,
                                                                PARTITION_DISTRIBUTION,
                                                                Integer.MAX_VALUE );

        for ( int i = 0; i < ACQUIRED_PARTITIONS.length; i++ )
        {
            final int partitionId = ACQUIRED_PARTITIONS[ i ];
            operatorTupleQueue.offer( 0, singletonList( generateTuple( partitionId, keys ) ) );
        }
    }

    @Test
    public void shouldAcquireNewPartitions ()
    {
        final TupleQueueContainer[] newPartitions = new TupleQueueContainer[ NON_ACQUIRED_PARTITIONS.length ];

        for ( int i = 0; i < newPartitions.length; i++ )
        {
            final int partitionId = NON_ACQUIRED_PARTITIONS[ i ];
            final TupleQueueContainer container = tupleQueueContainers[ partitionId ];
            newPartitions[ i ] = container;
            final Tuple tuple = generateTuple( partitionId, keys );
            container.offer( 0, tuple, EXTRACTOR.getPartitionKey( tuple ) );
        }

        operatorTupleQueue.acquirePartitions( newPartitions );

        assertEquals( PARTITION_COUNT, operatorTupleQueue.getTotalDrainableKeyCount() );

        for ( int partitionId : PARTITION_DISTRIBUTION )
        {
            final Tuple tuple = generateTuple( partitionId, keys );
            operatorTupleQueue.offer( 0, singletonList( tuple ) );
        }

        final int expectedKeyCount = PARTITION_COUNT * 2;
        assertEquals( expectedKeyCount, operatorTupleQueue.getTotalDrainableKeyCount() );

        final GreedyDrainer drainer = new GreedyDrainer( INPUT_PORT_COUNT );
        for ( int i = 0; i < expectedKeyCount; i++ )
        {
            drainer.reset();
            operatorTupleQueue.drain( drainer );
            final TuplesImpl result = drainer.getResult();
            assertNotNull( result );
            assertEquals( 1, result.getTupleCount( 0 ) );
        }

        assertEquals( 0, operatorTupleQueue.getTotalDrainableKeyCount() );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotAcquireAlreadyAcquiredPartition ()
    {
        final TupleQueueContainer[] newPartitions = new TupleQueueContainer[ 1 ];
        newPartitions[ 0 ] = tupleQueueContainers[ ACQUIRED_PARTITIONS[ 0 ] ];

        operatorTupleQueue.acquirePartitions( newPartitions );
    }

    @Test
    public void shouldReleasePartitions ()
    {
        final int releasePartitionIndex = ACQUIRED_PARTITIONS.length / 2;
        final int[] releasePartitionIds = new int[ releasePartitionIndex ];
        for ( int i = 0; i < releasePartitionIds.length; i++ )
        {
            releasePartitionIds[ i ] = ACQUIRED_PARTITIONS[ i ];
        }

        final TupleQueueContainer[] releasedPartitions = operatorTupleQueue.releasePartitions( releasePartitionIds );
        assertReleasedPartitions( releasePartitionIds, releasedPartitions );

        assertEquals( ACQUIRED_PARTITIONS.length - releasePartitionIds.length, operatorTupleQueue.getTotalDrainableKeyCount() );

        for ( int releasedPartitionId : releasePartitionIds )
        {
            try
            {
                operatorTupleQueue.offer( 0, singletonList( generateTuple( releasedPartitionId, keys ) ) );
                fail();
            }
            catch ( NullPointerException expected )
            {

            }
        }

        for ( int partitionId = releasePartitionIndex; partitionId < ACQUIRED_PARTITIONS.length; partitionId++ )
        {
            operatorTupleQueue.offer( 0, singletonList( generateTuple( partitionId, keys ) ) );
        }

        int expectedKeyCount = ACQUIRED_PARTITIONS.length;
        expectedKeyCount -= releasePartitionIds.length;
        expectedKeyCount += ( ACQUIRED_PARTITIONS.length - releasePartitionIds.length );

        assertEquals( expectedKeyCount, operatorTupleQueue.getTotalDrainableKeyCount() );
    }

    @Test
    public void shouldReleaseAllPartitions ()
    {
        final int[] releasePartitionIds = ACQUIRED_PARTITIONS;
        final TupleQueueContainer[] releasedPartitions = operatorTupleQueue.releasePartitions( releasePartitionIds );
        assertReleasedPartitions( releasePartitionIds, releasedPartitions );

        assertEquals( 0, operatorTupleQueue.getTotalDrainableKeyCount() );
    }

    private void assertReleasedPartitions ( final int[] releasePartitionIds, final TupleQueueContainer[] releasedPartitions )
    {
        assertEquals( releasePartitionIds.length, releasedPartitions.length );
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
        final int[] releasedPartitionIds = new int[] { 1 };
        releasedPartitionIds[ 0 ] = NON_ACQUIRED_PARTITIONS[ 0 ];

        operatorTupleQueue.releasePartitions( releasedPartitionIds );
    }

    private Tuple generateTuple ( final int partitionId, final Set<Object> existingKeys )
    {
        final Tuple tuple = new Tuple();
        int i = 0;
        while ( true )
        {
            if ( existingKeys.contains( i ) )
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
