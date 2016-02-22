package cs.bilkent.zanza.engine.tuplequeue.impl.drainer;

import org.junit.Test;

import static cs.bilkent.zanza.engine.TestUtils.spawnThread;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueue;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueueDrainer;
import cs.bilkent.zanza.engine.tuplequeue.impl.queue.BoundedTupleQueue;
import static cs.bilkent.zanza.engine.tuplequeue.impl.queue.BoundedTupleQueueTest.offerTuples;
import static cs.bilkent.zanza.flow.Port.DEFAULT_PORT_INDEX;
import cs.bilkent.zanza.operator.PortsToTuples;
import cs.bilkent.zanza.operator.Tuple;
import cs.bilkent.zanza.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByCount;
import static cs.bilkent.zanza.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByCount.AT_LEAST;
import static cs.bilkent.zanza.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByCount.AT_LEAST_BUT_SAME_ON_ALL_PORTS;
import static cs.bilkent.zanza.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByCount.EXACT;
import static java.util.Arrays.asList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class BlockingSinglePortDrainerTest
{

    private static final int TIMEOUT_IN_MILLIS = 5000;

    @Test( expected = IllegalArgumentException.class )
    public void shouldFailWithNullTupleQueues ()
    {
        final TupleQueueDrainer drainer = new BlockingSinglePortDrainer( 1, AT_LEAST, TIMEOUT_IN_MILLIS );
        drainer.drain( null, null );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldFailWithEmptyTupleQueues ()
    {
        final TupleQueueDrainer drainer = new BlockingSinglePortDrainer( 1, AT_LEAST, TIMEOUT_IN_MILLIS );
        drainer.drain( null, new TupleQueue[] {} );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldFailWithMultipleTupleQueues ()
    {
        final TupleQueueDrainer drainer = new BlockingSinglePortDrainer( 1, AT_LEAST, TIMEOUT_IN_MILLIS );
        drainer.drain( null, new TupleQueue[] { new BoundedTupleQueue( 1 ), new BoundedTupleQueue( 1 ) } );
    }

    @Test
    public void shouldDrainAllTuplesWithAtLeastTupleAvailabilityByCountSatisfiesAlready ()
    {
        final TupleQueue tupleQueue = new BoundedTupleQueue( 2 );
        final Tuple tuple1 = new Tuple();
        tupleQueue.offerTuple( tuple1 );
        final Tuple tuple2 = new Tuple();
        tupleQueue.offerTuple( tuple2 );

        final TupleQueueDrainer drainer = new BlockingSinglePortDrainer( 1, AT_LEAST, TIMEOUT_IN_MILLIS );
        drainer.drain( null, new TupleQueue[] { tupleQueue } );

        final PortsToTuples portsToTuples = drainer.getResult();
        assertNotNull( portsToTuples );
        assertThat( portsToTuples.getPortCount(), equalTo( 1 ) );
        assertThat( portsToTuples.getTupleCount( DEFAULT_PORT_INDEX ), equalTo( 2 ) );
        assertTrue( tuple1 == portsToTuples.getTuple( DEFAULT_PORT_INDEX, 0 ) );
        assertTrue( tuple2 == portsToTuples.getTuple( DEFAULT_PORT_INDEX, 1 ) );
        assertThat( tupleQueue.size(), equalTo( 0 ) );
    }

    @Test
    public void shouldDrainAllTuplesWithAtLeastTupleAvailabilityByCountSatisfiesAfterwards ()
    {
        final TupleQueue tupleQueue = new BoundedTupleQueue( 2 );
        final Tuple tuple1 = new Tuple();
        final Tuple tuple2 = new Tuple();

        spawnThread( offerTuples( Thread.currentThread(), tupleQueue, asList( tuple1, tuple2 ) ) );
        final TupleQueueDrainer drainer = new BlockingSinglePortDrainer( 1, AT_LEAST, TIMEOUT_IN_MILLIS );
        drainer.drain( null, new TupleQueue[] { tupleQueue } );

        final PortsToTuples portsToTuples = drainer.getResult();
        assertNotNull( portsToTuples );
        assertThat( portsToTuples.getPortCount(), equalTo( 1 ) );
        assertThat( portsToTuples.getTupleCount( DEFAULT_PORT_INDEX ), equalTo( 2 ) );
        assertTrue( tuple1 == portsToTuples.getTuple( DEFAULT_PORT_INDEX, 0 ) );
        assertTrue( tuple2 == portsToTuples.getTuple( DEFAULT_PORT_INDEX, 1 ) );
        assertThat( tupleQueue.size(), equalTo( 0 ) );
    }

    @Test
    public void shouldNotDrainAnyTupleWithAtLeastTupleAvailabilityByCountUnsatisfied ()
    {
        testNoDrain( AT_LEAST );
    }

    @Test
    public void shouldDrainTuplesWithExactButSameOnAllPortsTupleAvailabilityByCountSatisfiedAlready ()
    {
        final TupleQueue tupleQueue = new BoundedTupleQueue( 2 );
        final Tuple tuple1 = new Tuple();
        tupleQueue.offerTuple( tuple1 );
        final Tuple tuple2 = new Tuple();
        tupleQueue.offerTuple( tuple2 );

        final TupleQueueDrainer drainer = new BlockingSinglePortDrainer( 1, AT_LEAST_BUT_SAME_ON_ALL_PORTS, TIMEOUT_IN_MILLIS );
        drainer.drain( null, new TupleQueue[] { tupleQueue } );

        final PortsToTuples portsToTuples = drainer.getResult();
        assertNotNull( portsToTuples );
        assertThat( portsToTuples.getPortCount(), equalTo( 1 ) );
        assertThat( portsToTuples.getTupleCount( DEFAULT_PORT_INDEX ), equalTo( 2 ) );
        assertTrue( tuple1 == portsToTuples.getTuple( DEFAULT_PORT_INDEX, 0 ) );
        assertTrue( tuple2 == portsToTuples.getTuple( DEFAULT_PORT_INDEX, 1 ) );
        assertThat( tupleQueue.size(), equalTo( 0 ) );
    }

    @Test
    public void shouldDrainTuplesWithExactButSameOnAllPortsTupleAvailabilityByCountSatisfiedAfterwards ()
    {
        final TupleQueue tupleQueue = new BoundedTupleQueue( 2 );
        final Tuple tuple1 = new Tuple();
        final Tuple tuple2 = new Tuple();

        spawnThread( offerTuples( Thread.currentThread(), tupleQueue, asList( tuple1, tuple2 ) ) );
        final TupleQueueDrainer drainer = new BlockingSinglePortDrainer( 1, AT_LEAST_BUT_SAME_ON_ALL_PORTS, TIMEOUT_IN_MILLIS );
        drainer.drain( null, new TupleQueue[] { tupleQueue } );

        final PortsToTuples portsToTuples = drainer.getResult();
        assertNotNull( portsToTuples );
        assertThat( portsToTuples.getPortCount(), equalTo( 1 ) );
        assertThat( portsToTuples.getTupleCount( DEFAULT_PORT_INDEX ), equalTo( 2 ) );
        assertTrue( tuple1 == portsToTuples.getTuple( DEFAULT_PORT_INDEX, 0 ) );
        assertTrue( tuple2 == portsToTuples.getTuple( DEFAULT_PORT_INDEX, 1 ) );
        assertThat( tupleQueue.size(), equalTo( 0 ) );
    }

    @Test
    public void shouldNotDrainAnyTupleWithExactButSameOnAllPortsTupleAvailabilityByCountUnsatisfied ()
    {
        testNoDrain( AT_LEAST_BUT_SAME_ON_ALL_PORTS );
    }

    @Test
    public void shouldDrainTuplesWithExactTupleAvailabilityByCountSatisfiedAlready ()
    {
        final TupleQueue tupleQueue = new BoundedTupleQueue( 2 );
        final Tuple tuple1 = new Tuple();
        tupleQueue.offerTuple( tuple1 );
        tupleQueue.offerTuple( new Tuple() );

        final TupleQueueDrainer drainer = new BlockingSinglePortDrainer( 1, EXACT, TIMEOUT_IN_MILLIS );
        drainer.drain( null, new TupleQueue[] { tupleQueue } );

        final PortsToTuples portsToTuples = drainer.getResult();
        assertNotNull( portsToTuples );
        assertThat( portsToTuples.getPortCount(), equalTo( 1 ) );
        assertThat( portsToTuples.getTupleCount( DEFAULT_PORT_INDEX ), equalTo( 1 ) );
        assertTrue( tuple1 == portsToTuples.getTuple( DEFAULT_PORT_INDEX, 0 ) );

        assertThat( tupleQueue.size(), equalTo( 1 ) );
    }

    @Test
    public void shouldDrainTuplesWithExactTupleAvailabilityByCountSatisfiedAfterwards ()
    {
        final TupleQueue tupleQueue = new BoundedTupleQueue( 2 );
        final Tuple tuple1 = new Tuple();

        spawnThread( offerTuples( Thread.currentThread(), tupleQueue, asList( tuple1, new Tuple() ) ) );

        final TupleQueueDrainer drainer = new BlockingSinglePortDrainer( 1, EXACT, TIMEOUT_IN_MILLIS );
        drainer.drain( null, new TupleQueue[] { tupleQueue } );

        final PortsToTuples portsToTuples = drainer.getResult();
        assertNotNull( portsToTuples );
        assertThat( portsToTuples.getPortCount(), equalTo( 1 ) );
        assertThat( portsToTuples.getTupleCount( DEFAULT_PORT_INDEX ), equalTo( 1 ) );
        assertTrue( tuple1 == portsToTuples.getTuple( DEFAULT_PORT_INDEX, 0 ) );

        assertThat( tupleQueue.size(), equalTo( 1 ) );
    }

    @Test
    public void shouldNotDrainAnyTupleWithExactTupleAvailabilityByCountUnsatisfied ()
    {
        testNoDrain( EXACT );
    }

    private void testNoDrain ( final TupleAvailabilityByCount tupleAvailabilityByCount )
    {
        final TupleQueue tupleQueue = new BoundedTupleQueue( 2 );

        final TupleQueueDrainer drainer = new BlockingSinglePortDrainer( 1, tupleAvailabilityByCount, TIMEOUT_IN_MILLIS );
        drainer.drain( null, new TupleQueue[] { tupleQueue } );

        assertNull( drainer.getResult() );
    }

}
