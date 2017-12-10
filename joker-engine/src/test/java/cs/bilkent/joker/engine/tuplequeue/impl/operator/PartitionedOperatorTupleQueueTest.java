package cs.bilkent.joker.engine.tuplequeue.impl.operator;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import org.junit.Before;
import org.junit.Test;

import cs.bilkent.joker.engine.partition.impl.PartitionKeyExtractor1;
import cs.bilkent.joker.engine.tuplequeue.impl.TupleQueueContainer;
import cs.bilkent.joker.engine.tuplequeue.impl.drainer.GreedyDrainer;
import cs.bilkent.joker.engine.tuplequeue.impl.drainer.NonBlockingMultiPortDisjunctiveDrainer;
import cs.bilkent.joker.operator.Tuple;
import cs.bilkent.joker.operator.impl.TuplesImpl;
import static cs.bilkent.joker.operator.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByCount.AT_LEAST;
import static cs.bilkent.joker.operator.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByCount.EXACT;
import static cs.bilkent.joker.operator.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByPort.ANY_PORT;
import cs.bilkent.joker.partition.impl.PartitionKey;
import cs.bilkent.joker.test.AbstractJokerTest;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class PartitionedOperatorTupleQueueTest extends AbstractJokerTest
{

    private static final int PARTITION_COUNT = 1;

    private static final int INPUT_PORT_COUNT = 2;

    private static final int TUPLE_QUEUE_CAPACITY = 3;

    private static final String PARTITION_KEY_FIELD = "key";


    private PartitionedOperatorTupleQueue operatorTupleQueue;

    @Before
    public void init ()
    {
        final TupleQueueContainer container = new TupleQueueContainer( "op1", INPUT_PORT_COUNT, 0 );
        operatorTupleQueue = new PartitionedOperatorTupleQueue( "op1",
                                                                INPUT_PORT_COUNT,
                                                                PARTITION_COUNT,
                                                                0,
                                                                TUPLE_QUEUE_CAPACITY,
                                                                new PartitionKeyExtractor1( singletonList( PARTITION_KEY_FIELD ) ),
                                                                new TupleQueueContainer[] { container }, new int[] { 0 } );
    }

    @Test
    public void testOfferedTuplesDrained ()
    {
        final Tuple tuple = new Tuple();
        tuple.set( PARTITION_KEY_FIELD, "key1" );
        final List<Tuple> input = singletonList( tuple );
        operatorTupleQueue.offer( 0, input );

        final NonBlockingMultiPortDisjunctiveDrainer drainer = new NonBlockingMultiPortDisjunctiveDrainer( INPUT_PORT_COUNT, 100 );
        drainer.setParameters( AT_LEAST, new int[] { 0, 1 }, new int[] { 1, 1 } );

        final TuplesImpl result = new TuplesImpl( INPUT_PORT_COUNT );
        operatorTupleQueue.drain( drainer, key -> result );

        assertEquals( input, result.getTuples( 0 ) );
    }

    @Test
    public void testMultipleKeysDrainedAtOnce ()
    {
        final Tuple tuple1 = new Tuple();
        tuple1.set( PARTITION_KEY_FIELD, "key1" );
        operatorTupleQueue.offer( 0, singletonList( tuple1 ) );
        final Tuple tuple2 = new Tuple();
        tuple2.set( PARTITION_KEY_FIELD, "key2" );
        operatorTupleQueue.offer( 0, singletonList( tuple2 ) );
        final Tuple tuple3 = new Tuple();
        tuple3.set( PARTITION_KEY_FIELD, "key3" );
        operatorTupleQueue.offer( 0, singletonList( tuple3 ) );

        final NonBlockingMultiPortDisjunctiveDrainer drainer = new NonBlockingMultiPortDisjunctiveDrainer( INPUT_PORT_COUNT, 100 );
        drainer.setParameters( AT_LEAST, new int[] { 0, 1 }, new int[] { 1, 1 } );

        final List<TuplesImpl> results = new ArrayList<>();
        final Function<PartitionKey, TuplesImpl> tuplesSupplier = key -> {
            final TuplesImpl tuples = new TuplesImpl( INPUT_PORT_COUNT );
            results.add( tuples );
            return tuples;
        };

        operatorTupleQueue.drain( drainer, tuplesSupplier );

        assertEquals( 3, results.size() );

        List<Tuple> tuples = new ArrayList<>( asList( tuple1, tuple2, tuple3 ) );
        for ( TuplesImpl result : results )
        {
            tuples.remove( result.getTupleOrFail( 0, 0 ) );
        }

        assertTrue( tuples.isEmpty() );
    }

    @Test
    public void testSingleKeyDrainedMultipleTimesAtOnce ()
    {
        final Tuple tuple1 = new Tuple();
        tuple1.set( PARTITION_KEY_FIELD, "key1" );
        operatorTupleQueue.offer( 0, singletonList( tuple1 ) );
        final Tuple tuple2 = new Tuple();
        tuple2.set( PARTITION_KEY_FIELD, "key1" );
        operatorTupleQueue.offer( 0, singletonList( tuple2 ) );
        final Tuple tuple3 = new Tuple();
        tuple3.set( PARTITION_KEY_FIELD, "key1" );
        operatorTupleQueue.offer( 0, singletonList( tuple3 ) );

        final NonBlockingMultiPortDisjunctiveDrainer drainer = new NonBlockingMultiPortDisjunctiveDrainer( INPUT_PORT_COUNT, 100 );
        drainer.setParameters( EXACT, new int[] { 0, 1 }, new int[] { 1, 1 } );

        final List<TuplesImpl> results = new ArrayList<>();
        final Function<PartitionKey, TuplesImpl> tuplesSupplier = key -> {
            final TuplesImpl tuples = new TuplesImpl( INPUT_PORT_COUNT );
            results.add( tuples );
            return tuples;
        };

        operatorTupleQueue.setTupleCounts( new int[] { 1, 1 }, ANY_PORT );
        operatorTupleQueue.drain( drainer, tuplesSupplier );

        assertEquals( 3, results.size() );

        List<Tuple> tuples = new ArrayList<>( asList( tuple1, tuple2, tuple3 ) );
        for ( TuplesImpl result : results )
        {
            tuples.remove( result.getTupleOrFail( 0, 0 ) );
        }

        assertTrue( tuples.isEmpty() );
    }

    @Test
    public void testOfferedTuplesDrainedGreedilyWhenTupleCountsUpdated ()
    {
        operatorTupleQueue.setTupleCounts( new int[] { 2, 2 }, ANY_PORT );
        final Tuple tuple = new Tuple();
        tuple.set( PARTITION_KEY_FIELD, "key1" );
        final List<Tuple> tuples = singletonList( tuple );
        operatorTupleQueue.offer( 0, tuples );

        operatorTupleQueue.setTupleCounts( new int[] { 1, 1 }, ANY_PORT );

        final TuplesImpl result = new TuplesImpl( INPUT_PORT_COUNT );
        final GreedyDrainer drainer = new GreedyDrainer( INPUT_PORT_COUNT );
        operatorTupleQueue.drain( drainer, key -> result );

        assertEquals( tuples, result.getTuples( 0 ) );
    }

    // tuple counts are not satisfied.
    @Test
    public void testOfferedTuplesNotDrainedGreedilyWhenTupleCountsNotUpdated ()
    {
        operatorTupleQueue.setTupleCounts( new int[] { 2, 2 }, ANY_PORT );
        final Tuple tuple = new Tuple();
        tuple.set( PARTITION_KEY_FIELD, "key1" );
        final List<Tuple> tuples = singletonList( tuple );
        operatorTupleQueue.offer( 0, tuples );

        final TuplesImpl result = new TuplesImpl( INPUT_PORT_COUNT );
        final GreedyDrainer drainer = new GreedyDrainer( INPUT_PORT_COUNT );
        operatorTupleQueue.drain( drainer, key -> result );

        assertTrue( result.isEmpty() );
    }

    // tuple counts are already satisfied.
    @Test
    public void testOfferedTuplesDrainedGreedilyWhenTupleCountsNotUpdated ()
    {
        final Tuple tuple = new Tuple();
        tuple.set( PARTITION_KEY_FIELD, "key1" );
        final List<Tuple> tuples = singletonList( tuple );
        operatorTupleQueue.offer( 0, tuples );

        final TuplesImpl result = new TuplesImpl( INPUT_PORT_COUNT );
        final GreedyDrainer drainer = new GreedyDrainer( INPUT_PORT_COUNT );
        operatorTupleQueue.drain( drainer, key -> result );

        assertEquals( tuples, result.getTuples( 0 ) );
    }

}
