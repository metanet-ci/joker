package cs.bilkent.joker.engine.tuplequeue.impl.operator;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import org.junit.Before;
import org.junit.Test;

import static cs.bilkent.joker.engine.partition.PartitionUtil.getPartitionId;
import cs.bilkent.joker.engine.partition.impl.PartitionKeyExtractor1;
import cs.bilkent.joker.engine.tuplequeue.TupleQueue;
import cs.bilkent.joker.engine.tuplequeue.TupleQueueDrainer;
import static cs.bilkent.joker.engine.tuplequeue.impl.drainer.NonBlockingMultiPortDisjunctiveDrainer.newGreedyDrainer;
import cs.bilkent.joker.engine.tuplequeue.impl.queue.SingleThreadedTupleQueue;
import cs.bilkent.joker.operator.Tuple;
import cs.bilkent.joker.operator.Tuples;
import cs.bilkent.joker.operator.impl.TuplesImpl;
import cs.bilkent.joker.partition.impl.PartitionKey;
import cs.bilkent.joker.test.AbstractJokerTest;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class PartitionedOperatorQueueRebalancingTest extends AbstractJokerTest
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

    private final Set<Object> keys = new HashSet<>();

    private PartitionedOperatorQueue operatorQueue;

    @Before
    public void init ()
    {
        operatorQueue = new PartitionedOperatorQueue( OPERATOR_ID, INPUT_PORT_COUNT, PARTITION_COUNT, REPLICA_INDEX, 100, EXTRACTOR );

        for ( int partitionId : ACQUIRED_PARTITIONS )
        {
            operatorQueue.offer( 0, singletonList( generateTuple( partitionId ) ) );
        }
    }

    @Test
    public void shouldAcquireNewPartitions ()
    {
        for ( int partitionId : NON_ACQUIRED_PARTITIONS )
        {
            final Tuple tuple = generateTuple( partitionId );
            final PartitionKey partitionKey = EXTRACTOR.getPartitionKey( tuple );
            final TupleQueue[] tupleQueues = new TupleQueue[] { new SingleThreadedTupleQueue( 100 ) };
            tupleQueues[ 0 ].offer( tuple );
            operatorQueue.acquireKeys( partitionId, singletonMap( partitionKey, tupleQueues ) );
        }

        assertEquals( PARTITION_COUNT, operatorQueue.getDrainableKeyCount() );

        for ( int partitionId : PARTITION_DISTRIBUTION )
        {
            final Tuple tuple = generateTuple( partitionId );
            operatorQueue.offer( 0, singletonList( tuple ) );
        }

        final int expectedKeyCount = PARTITION_COUNT * 2;

        final List<TuplesImpl> results = new ArrayList<>();
        final Function<PartitionKey, TuplesImpl> tuplesSupplier = key -> {
            final TuplesImpl tuples = new TuplesImpl( 1 );
            results.add( tuples );
            return tuples;
        };
        final TupleQueueDrainer drainer = newGreedyDrainer( operatorQueue.getOperatorId(), INPUT_PORT_COUNT, Integer.MAX_VALUE );
        operatorQueue.drain( drainer, tuplesSupplier );

        int tupleCount = 0;
        for ( Tuples result : results )
        {
            tupleCount += result.getTupleCount( 0 );
        }

        assertEquals( expectedKeyCount, tupleCount );
    }

    @Test
    public void shouldReleasePartitions ()
    {
        final int releasePartitionIndex = ACQUIRED_PARTITIONS.size() / 2;
        final Set<Integer> releasePartitionIds = new HashSet<>();
        for ( int i = 0; i < releasePartitionIndex; i++ )
        {
            releasePartitionIds.add( ACQUIRED_PARTITIONS.get( i ) );
        }

        final Map<Integer, Map<PartitionKey, TupleQueue[]>> releasedPartitions = operatorQueue.releasePartitions( releasePartitionIds );
        assertReleasedPartitions( releasePartitionIds, releasedPartitions );

        assertEquals( ACQUIRED_PARTITIONS.size() - releasePartitionIds.size(), operatorQueue.getDrainableKeyCount() );

        for ( int i = releasePartitionIndex; i < ACQUIRED_PARTITIONS.size(); i++ )
        {
            operatorQueue.offer( 0, singletonList( generateTuple( ACQUIRED_PARTITIONS.get( i ) ) ) );
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
        final TupleQueueDrainer drainer = newGreedyDrainer( operatorQueue.getOperatorId(), INPUT_PORT_COUNT, Integer.MAX_VALUE );
        operatorQueue.drain( drainer, tuplesSupplier );

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
        final Set<Integer> releasePartitionIds = new HashSet<>( ACQUIRED_PARTITIONS );

        final Map<Integer, Map<PartitionKey, TupleQueue[]>> releasedPartitions = operatorQueue.releasePartitions( releasePartitionIds );
        assertReleasedPartitions( releasePartitionIds, releasedPartitions );

        assertEquals( 0, operatorQueue.getDrainableKeyCount() );
    }

    private void assertReleasedPartitions ( final Set<Integer> releasePartitionIds,
                                            final Map<Integer, Map<PartitionKey, TupleQueue[]>> releasedPartitions )
    {
        assertEquals( releasePartitionIds.size(), releasedPartitions.size() );
        for ( int releasePartitionId : releasePartitionIds )
        {
            final Map<PartitionKey, TupleQueue[]> keys = releasedPartitions.get( releasePartitionId );
            assertNotNull( keys );
            assertEquals( 1, keys.size() );
        }
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
