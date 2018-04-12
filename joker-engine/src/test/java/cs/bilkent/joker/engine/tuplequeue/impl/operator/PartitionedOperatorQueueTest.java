package cs.bilkent.joker.engine.tuplequeue.impl.operator;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import org.junit.Before;
import org.junit.Test;

import cs.bilkent.joker.engine.partition.impl.PartitionKeyExtractor1;
import cs.bilkent.joker.engine.tuplequeue.impl.drainer.GreedyDrainer;
import cs.bilkent.joker.engine.tuplequeue.impl.drainer.NonBlockingMultiPortDisjunctiveDrainer;
import cs.bilkent.joker.operator.Tuple;
import cs.bilkent.joker.operator.impl.TuplesImpl;
import static cs.bilkent.joker.operator.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByCount.AT_LEAST;
import static cs.bilkent.joker.operator.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByCount.EXACT;
import static cs.bilkent.joker.operator.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByPort.ANY_PORT;
import cs.bilkent.joker.partition.impl.PartitionKey;
import cs.bilkent.joker.test.AbstractJokerTest;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;

public class PartitionedOperatorQueueTest extends AbstractJokerTest
{

    private static final int PARTITION_COUNT = 1;

    private static final int INPUT_PORT_COUNT = 2;

    private static final int TUPLE_QUEUE_CAPACITY = 3;

    private static final String PARTITION_KEY_FIELD = "key";


    private PartitionedOperatorQueue operatorQueue;

    @Before
    public void init ()
    {
        operatorQueue = new PartitionedOperatorQueue( "op1",
                                                      INPUT_PORT_COUNT,
                                                      PARTITION_COUNT,
                                                      0,
                                                      TUPLE_QUEUE_CAPACITY,
                                                      new PartitionKeyExtractor1( singletonList( PARTITION_KEY_FIELD ) ) );
    }

    @Test
    public void testOfferedTuplesDrained ()
    {
        final Tuple tuple = Tuple.of( PARTITION_KEY_FIELD, "key1" );
        final List<Tuple> input = singletonList( tuple );
        operatorQueue.offer( 0, input );

        final NonBlockingMultiPortDisjunctiveDrainer drainer = new NonBlockingMultiPortDisjunctiveDrainer( INPUT_PORT_COUNT, 100 );
        drainer.setParameters( AT_LEAST, new int[] { 0, 1 }, new int[] { 1, 1 } );

        final TuplesImpl result = new TuplesImpl( INPUT_PORT_COUNT );
        operatorQueue.drain( drainer, key -> result );

        assertEquals( input, result.getTuples( 0 ) );
    }

    @Test
    public void testMultipleKeysDrainedAtOnce ()
    {
        final Tuple tuple1 = Tuple.of( PARTITION_KEY_FIELD, "key1" );
        operatorQueue.offer( 0, singletonList( tuple1 ) );
        final Tuple tuple2 = Tuple.of( PARTITION_KEY_FIELD, "key2" );
        operatorQueue.offer( 0, singletonList( tuple2 ) );
        final Tuple tuple3 = Tuple.of( PARTITION_KEY_FIELD, "key3" );
        operatorQueue.offer( 0, singletonList( tuple3 ) );

        final NonBlockingMultiPortDisjunctiveDrainer drainer = new NonBlockingMultiPortDisjunctiveDrainer( INPUT_PORT_COUNT, 100 );
        drainer.setParameters( AT_LEAST, new int[] { 0, 1 }, new int[] { 1, 1 } );

        final List<TuplesImpl> results = new ArrayList<>();
        final Function<PartitionKey, TuplesImpl> tuplesSupplier = key -> {
            final TuplesImpl tuples = new TuplesImpl( INPUT_PORT_COUNT );
            results.add( tuples );
            return tuples;
        };

        operatorQueue.drain( drainer, tuplesSupplier );

        assertEquals( 3, results.size() );
        assertEquals( tuple1, results.get( 0 ).getTupleOrFail( 0, 0 ) );
        assertEquals( tuple2, results.get( 1 ).getTupleOrFail( 0, 0 ) );
        assertEquals( tuple3, results.get( 2 ).getTupleOrFail( 0, 0 ) );
    }

    @Test
    public void testSingleKeyDrainedMultipleTimesAtOnce ()
    {
        final Tuple tuple1 = Tuple.of( PARTITION_KEY_FIELD, "key1" );
        operatorQueue.offer( 0, singletonList( tuple1 ) );
        final Tuple tuple2 = Tuple.of( PARTITION_KEY_FIELD, "key2" );
        operatorQueue.offer( 0, singletonList( tuple2 ) );
        final Tuple tuple3 = Tuple.of( PARTITION_KEY_FIELD, "key1" );
        operatorQueue.offer( 0, singletonList( tuple3 ) );

        final NonBlockingMultiPortDisjunctiveDrainer drainer = new NonBlockingMultiPortDisjunctiveDrainer( INPUT_PORT_COUNT, 100 );
        drainer.setParameters( EXACT, new int[] { 0, 1 }, new int[] { 1, 1 } );

        final List<TuplesImpl> results = new ArrayList<>();
        final Function<PartitionKey, TuplesImpl> tuplesSupplier = key -> {
            final TuplesImpl tuples = new TuplesImpl( INPUT_PORT_COUNT );
            results.add( tuples );
            return tuples;
        };

        operatorQueue.drain( drainer, tuplesSupplier );

        assertEquals( 3, results.size() );
        assertEquals( tuple1, results.get( 0 ).getTupleOrFail( 0, 0 ) );
        assertEquals( tuple3, results.get( 1 ).getTupleOrFail( 0, 0 ) );
        assertEquals( tuple2, results.get( 2 ).getTupleOrFail( 0, 0 ) );
    }

    @Test
    public void testOfferedTuplesDrainedGreedilyWhenTupleCountsUpdated ()
    {
        operatorQueue.setTupleCounts( new int[] { 2, 2 }, ANY_PORT );
        final Tuple tuple = Tuple.of( PARTITION_KEY_FIELD, "key1" );
        final List<Tuple> tuples = singletonList( tuple );
        operatorQueue.offer( 0, tuples );

        operatorQueue.setTupleCounts( new int[] { 1, 1 }, ANY_PORT );

        final TuplesImpl result = new TuplesImpl( INPUT_PORT_COUNT );
        final GreedyDrainer drainer = new GreedyDrainer( INPUT_PORT_COUNT );
        operatorQueue.drain( drainer, key -> result );

        assertEquals( tuples, result.getTuples( 0 ) );
    }

    // tuple counts are already satisfied.
    @Test
    public void testOfferedTuplesDrainedGreedilyWhenTupleCountsNotUpdated ()
    {
        final Tuple tuple = Tuple.of( PARTITION_KEY_FIELD, "key1" );
        final List<Tuple> tuples = singletonList( tuple );
        operatorQueue.offer( 0, tuples );

        final TuplesImpl result = new TuplesImpl( INPUT_PORT_COUNT );
        final GreedyDrainer drainer = new GreedyDrainer( INPUT_PORT_COUNT );
        operatorQueue.drain( drainer, key -> result );

        assertEquals( tuples, result.getTuples( 0 ) );
    }

}
