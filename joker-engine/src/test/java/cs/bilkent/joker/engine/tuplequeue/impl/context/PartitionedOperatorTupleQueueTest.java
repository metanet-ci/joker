package cs.bilkent.joker.engine.tuplequeue.impl.context;

import java.util.List;
import java.util.function.BiFunction;

import org.junit.Before;
import org.junit.Test;

import cs.bilkent.joker.engine.partition.impl.PartitionKeyExtractor1;
import cs.bilkent.joker.engine.tuplequeue.TupleQueue;
import cs.bilkent.joker.engine.tuplequeue.impl.TupleQueueContainer;
import cs.bilkent.joker.engine.tuplequeue.impl.drainer.GreedyDrainer;
import cs.bilkent.joker.engine.tuplequeue.impl.drainer.NonBlockingMultiPortDisjunctiveDrainer;
import cs.bilkent.joker.engine.tuplequeue.impl.queue.SingleThreadedTupleQueue;
import cs.bilkent.joker.operator.Tuple;
import static cs.bilkent.joker.operator.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByCount.AT_LEAST;
import static cs.bilkent.joker.operator.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByPort.ANY_PORT;
import cs.bilkent.joker.testutils.AbstractJokerTest;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class PartitionedOperatorTupleQueueTest extends AbstractJokerTest
{

    private static final int INPUT_PORT_COUNT = 2;

    private static final int PARTITION_COUNT = 1;

    private static final String PARTITION_KEY_FIELD = "key";


    private PartitionedOperatorTupleQueue operatorTupleQueue;

    @Before
    public void init ()
    {
        final BiFunction<Integer, Boolean, TupleQueue> tupleQueueConstructor = ( portIndex, capacityCheckEnabled ) -> new SingleThreadedTupleQueue( 10 );

        final TupleQueueContainer container = new TupleQueueContainer( "op1", INPUT_PORT_COUNT, 0, tupleQueueConstructor );
        operatorTupleQueue = new PartitionedOperatorTupleQueue( "op1",
                                                                INPUT_PORT_COUNT,
                                                                PARTITION_COUNT,
                                                                0,
                                                                new PartitionKeyExtractor1( singletonList( PARTITION_KEY_FIELD ) ),
                                                                new TupleQueueContainer[] { container },
                                                                new int[] { 0 },
                                                                Integer.MAX_VALUE );
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

}
