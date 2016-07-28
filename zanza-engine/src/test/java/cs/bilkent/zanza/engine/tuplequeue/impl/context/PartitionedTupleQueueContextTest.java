package cs.bilkent.zanza.engine.tuplequeue.impl.context;

import java.util.List;
import java.util.function.Function;

import org.junit.Before;
import org.junit.Test;

import cs.bilkent.zanza.engine.partition.impl.PartitionKeyFunction1;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueue;
import cs.bilkent.zanza.engine.tuplequeue.impl.TupleQueueContainer;
import cs.bilkent.zanza.engine.tuplequeue.impl.drainer.GreedyDrainer;
import cs.bilkent.zanza.engine.tuplequeue.impl.drainer.NonBlockingMultiPortDisjunctiveDrainer;
import cs.bilkent.zanza.engine.tuplequeue.impl.queue.SingleThreadedTupleQueue;
import cs.bilkent.zanza.operator.Tuple;
import static cs.bilkent.zanza.operator.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByCount.AT_LEAST;
import static cs.bilkent.zanza.operator.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByPort.ANY_PORT;
import cs.bilkent.zanza.testutils.ZanzaTest;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class PartitionedTupleQueueContextTest extends ZanzaTest
{

    private static final int INPUT_PORT_COUNT = 2;

    private static final String PARTITION_KEY_FIELD = "key";


    private PartitionedTupleQueueContext tupleQueueContext;

    @Before
    public void init ()
    {
        final Function<Boolean, TupleQueue> tupleQueueConstructor = capacityCheckEnabled -> new SingleThreadedTupleQueue( 10 );

        final TupleQueueContainer container = new TupleQueueContainer( "op1", INPUT_PORT_COUNT, 0, tupleQueueConstructor );
        tupleQueueContext = new PartitionedTupleQueueContext( "op1",
                                                              INPUT_PORT_COUNT,
                                                              1,
                                                              0,
                                                              new PartitionKeyFunction1( singletonList( PARTITION_KEY_FIELD ) ),
                                                              new TupleQueueContainer[] { container },
                                                              new int[] { 0 } );
    }

    @Test
    public void testOfferedTuplesDrained ()
    {
        final List<Tuple> tuples = singletonList( new Tuple( PARTITION_KEY_FIELD, "key1" ) );
        tupleQueueContext.offer( 0, tuples );

        final NonBlockingMultiPortDisjunctiveDrainer drainer = new NonBlockingMultiPortDisjunctiveDrainer( INPUT_PORT_COUNT, 100 );
        drainer.setParameters( AT_LEAST, new int[] { 0, 1 }, new int[] { 1, 1 } );
        tupleQueueContext.drain( drainer );
        assertEquals( tuples, drainer.getResult().getTuples( 0 ) );
    }

    @Test
    public void testOfferedTuplesDrainedGreedilyWithGreedyPreparation ()
    {
        tupleQueueContext.setTupleCounts( new int[] { 2, 2 }, ANY_PORT );
        final List<Tuple> tuples = singletonList( new Tuple( PARTITION_KEY_FIELD, "key1" ) );
        tupleQueueContext.offer( 0, tuples );
        tupleQueueContext.prepareGreedyDraining();

        final GreedyDrainer drainer = new GreedyDrainer( INPUT_PORT_COUNT );
        tupleQueueContext.drain( drainer );
        assertEquals( tuples, drainer.getResult().getTuples( 0 ) );
    }

    @Test
    public void testOfferedTuplesNotDrainedGreedilyWithoutGreedyPreparation ()
    {
        tupleQueueContext.setTupleCounts( new int[] { 2, 2 }, ANY_PORT );
        final List<Tuple> tuples = singletonList( new Tuple( PARTITION_KEY_FIELD, "key1" ) );
        tupleQueueContext.offer( 0, tuples );

        final GreedyDrainer drainer = new GreedyDrainer( INPUT_PORT_COUNT );
        tupleQueueContext.drain( drainer );
        assertNull( drainer.getResult() );
    }

    @Test
    public void testOfferedTuplesDrainedGreedilyWithoutGreedyPreparation ()
    {
        final List<Tuple> tuples = singletonList( new Tuple( PARTITION_KEY_FIELD, "key1" ) );
        tupleQueueContext.offer( 0, tuples );

        final GreedyDrainer drainer = new GreedyDrainer( INPUT_PORT_COUNT );
        tupleQueueContext.drain( drainer );
        assertEquals( tuples, drainer.getResult().getTuples( 0 ) );
    }

}
