package cs.bilkent.joker.engine.tuplequeue.impl.operator;

import java.util.List;

import org.junit.Before;
import org.junit.Test;

import cs.bilkent.joker.engine.partition.impl.PartitionKeyExtractor1;
import cs.bilkent.joker.engine.tuplequeue.impl.TupleQueueContainer;
import cs.bilkent.joker.engine.tuplequeue.impl.drainer.GreedyDrainer;
import cs.bilkent.joker.engine.tuplequeue.impl.drainer.NonBlockingMultiPortDisjunctiveDrainer;
import cs.bilkent.joker.operator.Tuple;
import static cs.bilkent.joker.operator.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByCount.AT_LEAST;
import static cs.bilkent.joker.operator.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByCount.EXACT;
import static cs.bilkent.joker.operator.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByPort.ALL_PORTS;
import static cs.bilkent.joker.operator.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByPort.ANY_PORT;
import cs.bilkent.joker.test.AbstractJokerTest;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class PartitionedOperatorTupleQueueTest extends AbstractJokerTest
{

    private static final int PARTITION_COUNT = 1;

    private static final int INPUT_PORT_COUNT = 2;

    private static final int TUPLE_QUEUE_CAPACITY = 3;

    private static final int MAX_DRAINABLE_KEY_COUNT = 2;

    private static final String PARTITION_KEY_FIELD = "key";


    private PartitionedOperatorTupleQueue operatorTupleQueue;

    @Before
    public void init ()
    {
        final TupleQueueContainer container = new TupleQueueContainer( "op1", INPUT_PORT_COUNT, 0 );
        operatorTupleQueue = new PartitionedOperatorTupleQueue( "op1",
                                                                INPUT_PORT_COUNT,
                                                                PARTITION_COUNT,
                                                                0, TUPLE_QUEUE_CAPACITY,
                                                                new PartitionKeyExtractor1( singletonList( PARTITION_KEY_FIELD ) ),
                                                                new TupleQueueContainer[] { container },
                                                                new int[] { 0 }, MAX_DRAINABLE_KEY_COUNT );
    }

    @Test
    public void testOfferedTuplesDrained ()
    {
        final Tuple tuple = new Tuple();
        tuple.set( PARTITION_KEY_FIELD, "key1" );
        final List<Tuple> tuples = singletonList( tuple );
        operatorTupleQueue.offer( 0, tuples );

        final NonBlockingMultiPortDisjunctiveDrainer drainer = new NonBlockingMultiPortDisjunctiveDrainer( INPUT_PORT_COUNT, 100 );
        drainer.setParameters( AT_LEAST, new int[] { 0, 1 }, new int[] { 1, 1 } );

        assertEquals( 1, operatorTupleQueue.getTotalDrainableKeyCount() );
        operatorTupleQueue.drain( drainer );
        assertEquals( 0, operatorTupleQueue.getTotalDrainableKeyCount() );
        assertEquals( tuples, drainer.getResult().getTuples( 0 ) );
    }

    @Test
    public void testOfferedTuplesCounted ()
    {
        final Tuple tuple1 = new Tuple();
        tuple1.set( PARTITION_KEY_FIELD, "key1" );
        final Tuple tuple2 = new Tuple();
        tuple2.set( PARTITION_KEY_FIELD, "key2" );
        final Tuple tuple3 = new Tuple();
        tuple3.set( PARTITION_KEY_FIELD, "key3" );
        final List<Tuple> tuples = asList( tuple1, tuple2, tuple3 );

        final int offered0 = operatorTupleQueue.offer( 0, tuples, 1 );
        final int offered1 = operatorTupleQueue.offer( 1, tuples, 2 );

        assertEquals( 2, offered0 );
        assertEquals( 1, offered1 );
        assertEquals( 2, operatorTupleQueue.getAvailableTupleCount( 0 ) );
        assertEquals( 1, operatorTupleQueue.getAvailableTupleCount( 1 ) );
        assertEquals( 2, operatorTupleQueue.getTotalDrainableKeyCount() );
    }

    @Test
    public void testDrainedTuplesCounted ()
    {
        final Tuple tuple1 = new Tuple();
        tuple1.set( PARTITION_KEY_FIELD, "key1" );
        final Tuple tuple2 = new Tuple();
        tuple2.set( PARTITION_KEY_FIELD, "key1" );
        final List<Tuple> tuples = asList( tuple1, tuple2 );

        operatorTupleQueue.offer( 0, tuples );
        operatorTupleQueue.offer( 1, tuples, 1 );

        final NonBlockingMultiPortDisjunctiveDrainer drainer = new NonBlockingMultiPortDisjunctiveDrainer( INPUT_PORT_COUNT, 100 );
        drainer.setParameters( EXACT, new int[] { 0, 1 }, new int[] { 1, 1 } );

        operatorTupleQueue.drain( drainer );

        assertEquals( 1, operatorTupleQueue.getAvailableTupleCount( 0 ) );
        assertEquals( 0, operatorTupleQueue.getAvailableTupleCount( 1 ) );
    }

    @Test
    public void testOfferedTuplesDrainedGreedilyWhenTupleCountsUpdated ()
    {
        operatorTupleQueue.setTupleCounts( new int[] { 2, 2 }, ANY_PORT );
        final Tuple tuple = new Tuple();
        tuple.set( PARTITION_KEY_FIELD, "key1" );
        final List<Tuple> tuples = singletonList( tuple );
        operatorTupleQueue.offer( 0, tuples );
        assertEquals( 0, operatorTupleQueue.getTotalDrainableKeyCount() );
        operatorTupleQueue.setTupleCounts( new int[] { 1, 1 }, ANY_PORT );
        assertEquals( 1, operatorTupleQueue.getTotalDrainableKeyCount() );

        final GreedyDrainer drainer = new GreedyDrainer( INPUT_PORT_COUNT );
        operatorTupleQueue.drain( drainer );
        assertEquals( 0, operatorTupleQueue.getTotalDrainableKeyCount() );
        assertEquals( tuples, drainer.getResult().getTuples( 0 ) );
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

        final GreedyDrainer drainer = new GreedyDrainer( INPUT_PORT_COUNT );
        operatorTupleQueue.drain( drainer );
        assertNull( drainer.getResult() );
    }

    // tuple counts are already satisfied.
    @Test
    public void testOfferedTuplesDrainedGreedilyWhenTupleCountsNotUpdated ()
    {
        final Tuple tuple = new Tuple();
        tuple.set( PARTITION_KEY_FIELD, "key1" );
        final List<Tuple> tuples = singletonList( tuple );
        operatorTupleQueue.offer( 0, tuples );

        final GreedyDrainer drainer = new GreedyDrainer( INPUT_PORT_COUNT );
        operatorTupleQueue.drain( drainer );
        assertEquals( tuples, drainer.getResult().getTuples( 0 ) );
    }

    @Test
    public void testOverloadedWhenTupleCountsExceedMaxDrainableTupleCount ()
    {
        final Tuple tuple1 = new Tuple();
        tuple1.set( PARTITION_KEY_FIELD, "key1" );
        final Tuple tuple2 = new Tuple();
        tuple2.set( PARTITION_KEY_FIELD, "key2" );
        final List<Tuple> tuples = asList( tuple1, tuple2 );

        operatorTupleQueue.offer( 0, tuples );

        assertTrue( operatorTupleQueue.isOverloaded() );
    }

    @Test
    public void testOverloadedWhenTupleCountsExceedTupleQueueCapacity ()
    {
        final Tuple tuple1 = new Tuple();
        tuple1.set( PARTITION_KEY_FIELD, "key1" );
        final Tuple tuple2 = new Tuple();
        tuple2.set( PARTITION_KEY_FIELD, "key1" );
        final Tuple tuple3 = new Tuple();
        tuple3.set( PARTITION_KEY_FIELD, "key1" );
        final List<Tuple> tuples = asList( tuple1, tuple2, tuple3 );

        operatorTupleQueue.offer( 0, tuples );

        assertTrue( operatorTupleQueue.isOverloaded() );
    }

    @Test
    public void testNotOverloadedWhenTupleCountsExceedTupleQueueCapacityButNoDrainableKeyExists ()
    {
        operatorTupleQueue.setTupleCounts( new int[] { 1, 1 }, ALL_PORTS );

        final Tuple tuple1 = new Tuple();
        tuple1.set( PARTITION_KEY_FIELD, "key1" );
        final Tuple tuple2 = new Tuple();
        tuple2.set( PARTITION_KEY_FIELD, "key1" );
        final Tuple tuple3 = new Tuple();
        tuple3.set( PARTITION_KEY_FIELD, "key1" );
        final List<Tuple> tuples = asList( tuple1, tuple2, tuple3 );

        operatorTupleQueue.offer( 0, tuples );

        assertFalse( operatorTupleQueue.isOverloaded() );
    }

}
