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

public class PartitionedTupleQueueContextRebalancingTest extends AbstractJokerTest
{

    private static final String OPERATOR_ID = "op1";

    private static final int INPUT_PORT_COUNT = 1;

    private static final int[] PARTITION_DISTRIBUTION = new int[] { 0, 0, 0, 0, 0, 1, 1, 1, 1, 1 };

    private static final int REPLICA_INDEX = 0;

    private static final int[] OWNED_PARTITIONS = new int[] { 0, 1, 2, 3, 4 };

    private static final int[] NON_OWNED_PARTITIONS = new int[] { 5, 6, 7, 8, 9 };

    private static final int PARTITION_COUNT = PARTITION_DISTRIBUTION.length;

    private static final String PARTITION_KEY_FIELD = "key";

    private static final PartitionKeyExtractor1 EXTRACTOR = new PartitionKeyExtractor1( singletonList( PARTITION_KEY_FIELD ) );

    private final TupleQueueContainer[] tupleQueueContainers = new TupleQueueContainer[ PARTITION_COUNT ];

    private final Set<Object> keys = new HashSet<>();

    private PartitionedTupleQueueContext tupleQueueContext;

    @Before
    public void init ()
    {
        for ( int partitionId = 0; partitionId < PARTITION_COUNT; partitionId++ )
        {
            final BiFunction<Integer, Boolean, TupleQueue> constructor = ( i, b ) -> new SingleThreadedTupleQueue( 100 );
            tupleQueueContainers[ partitionId ] = new TupleQueueContainer( OPERATOR_ID, INPUT_PORT_COUNT, partitionId, constructor );
        }

        tupleQueueContext = new PartitionedTupleQueueContext( OPERATOR_ID,
                                                              INPUT_PORT_COUNT,
                                                              PARTITION_COUNT,
                                                              REPLICA_INDEX,
                                                              EXTRACTOR,
                                                              tupleQueueContainers,
                                                              PARTITION_DISTRIBUTION,
                                                              Integer.MAX_VALUE );

        for ( int i = 0; i < OWNED_PARTITIONS.length; i++ )
        {
            final int partitionId = OWNED_PARTITIONS[ i ];
            tupleQueueContext.offer( 0, singletonList( generateTuple( partitionId, keys ) ) );
        }
    }

    @Test
    public void shouldOwnNewPartitions ()
    {
        final TupleQueueContainer[] newPartitions = new TupleQueueContainer[ NON_OWNED_PARTITIONS.length ];

        for ( int i = 0; i < newPartitions.length; i++ )
        {
            final int partitionId = NON_OWNED_PARTITIONS[ i ];
            final TupleQueueContainer container = tupleQueueContainers[ partitionId ];
            newPartitions[ i ] = container;
            final Tuple tuple = generateTuple( partitionId, keys );
            container.offer( 0, tuple, EXTRACTOR.getPartitionKey( tuple ) );
        }

        tupleQueueContext.ownPartitions( newPartitions );

        assertEquals( PARTITION_COUNT, tupleQueueContext.getTotalDrainableKeyCount() );

        for ( int partitionId : PARTITION_DISTRIBUTION )
        {
            final Tuple tuple = generateTuple( partitionId, keys );
            tupleQueueContext.offer( 0, singletonList( tuple ) );
        }

        final int expectedKeyCount = PARTITION_COUNT * 2;
        assertEquals( expectedKeyCount, tupleQueueContext.getTotalDrainableKeyCount() );

        final GreedyDrainer drainer = new GreedyDrainer( INPUT_PORT_COUNT );
        for ( int i = 0; i < expectedKeyCount; i++ )
        {
            drainer.reset();
            tupleQueueContext.drain( drainer );
            final TuplesImpl result = drainer.getResult();
            assertNotNull( result );
            assertEquals( 1, result.getTupleCount( 0 ) );
        }

        assertEquals( 0, tupleQueueContext.getTotalDrainableKeyCount() );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotOwnAlreadyOwnedPartition ()
    {
        final TupleQueueContainer[] newPartitions = new TupleQueueContainer[ 1 ];
        newPartitions[ 0 ] = tupleQueueContainers[ OWNED_PARTITIONS[ 0 ] ];

        tupleQueueContext.ownPartitions( newPartitions );
    }

    @Test
    public void shouldLeavePartitions ()
    {
        final int leavePartitionIndex = OWNED_PARTITIONS.length / 2;
        final int[] leavePartitionIds = new int[ leavePartitionIndex ];
        for ( int i = 0; i < leavePartitionIds.length; i++ )
        {
            leavePartitionIds[ i ] = OWNED_PARTITIONS[ i ];
        }

        final TupleQueueContainer[] leftPartitions = tupleQueueContext.leavePartitions( leavePartitionIds );
        assertLeftPartitions( leavePartitionIds, leftPartitions );

        assertEquals( OWNED_PARTITIONS.length - leavePartitionIds.length, tupleQueueContext.getTotalDrainableKeyCount() );

        for ( int leftPartitionId : leavePartitionIds )
        {
            try
            {
                tupleQueueContext.offer( 0, singletonList( generateTuple( leftPartitionId, keys ) ) );
                fail();
            }
            catch ( NullPointerException expected )
            {

            }
        }

        for ( int partitionId = leavePartitionIndex; partitionId < OWNED_PARTITIONS.length; partitionId++ )
        {
            tupleQueueContext.offer( 0, singletonList( generateTuple( partitionId, keys ) ) );
        }

        int expectedKeyCount = OWNED_PARTITIONS.length;
        expectedKeyCount -= leavePartitionIds.length;
        expectedKeyCount += ( OWNED_PARTITIONS.length - leavePartitionIds.length );

        assertEquals( expectedKeyCount, tupleQueueContext.getTotalDrainableKeyCount() );
    }

    @Test
    public void shouldLeaveAllPartitions ()
    {
        final int[] leavePartitionIds = OWNED_PARTITIONS;
        final TupleQueueContainer[] leftPartitions = tupleQueueContext.leavePartitions( leavePartitionIds );
        assertLeftPartitions( leavePartitionIds, leftPartitions );

        assertEquals( 0, tupleQueueContext.getTotalDrainableKeyCount() );
    }

    private void assertLeftPartitions ( final int[] leavePartitionIds, final TupleQueueContainer[] leftPartitions )
    {
        assertEquals( leavePartitionIds.length, leftPartitions.length );
        for ( int leavePartitionId : leavePartitionIds )
        {
            boolean found = false;
            for ( TupleQueueContainer partition : leftPartitions )
            {
                if ( partition.getPartitionId() == leavePartitionId )
                {
                    found = true;
                    break;
                }
            }

            assertTrue( found );
        }
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotLeaveNotOwnedPartition ()
    {
        final int[] leavePartitionIds = new int[] { 1 };
        leavePartitionIds[ 0 ] = NON_OWNED_PARTITIONS[ 0 ];

        tupleQueueContext.leavePartitions( leavePartitionIds );
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
