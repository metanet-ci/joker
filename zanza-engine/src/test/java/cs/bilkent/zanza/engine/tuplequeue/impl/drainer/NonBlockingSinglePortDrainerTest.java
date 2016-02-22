package cs.bilkent.zanza.engine.tuplequeue.impl.drainer;

import org.junit.Test;

import cs.bilkent.zanza.engine.tuplequeue.TupleQueue;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueueDrainer;
import cs.bilkent.zanza.engine.tuplequeue.impl.queue.UnboundedTupleQueue;
import static cs.bilkent.zanza.flow.Port.DEFAULT_PORT_INDEX;
import cs.bilkent.zanza.operator.PortsToTuples;
import cs.bilkent.zanza.operator.Tuple;
import static cs.bilkent.zanza.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByCount.AT_LEAST;
import static cs.bilkent.zanza.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByCount.AT_LEAST_BUT_SAME_ON_ALL_PORTS;
import static cs.bilkent.zanza.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByCount.EXACT;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;


public class NonBlockingSinglePortDrainerTest
{

    @Test( expected = IllegalArgumentException.class )
    public void shouldFailWithNullTupleQueues ()
    {
        final TupleQueueDrainer drainer = new NonBlockingSinglePortDrainer( 1, AT_LEAST );
        drainer.drain( null, null );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldFailWithEmptyTupleQueues ()
    {
        final TupleQueueDrainer drainer = new NonBlockingSinglePortDrainer( 1, AT_LEAST );
        drainer.drain( null, new TupleQueue[] {} );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldFailWithMultipleTupleQueues ()
    {
        final TupleQueueDrainer drainer = new NonBlockingSinglePortDrainer( 1, AT_LEAST );
        drainer.drain( null, new TupleQueue[] { new UnboundedTupleQueue( 1 ), new UnboundedTupleQueue( 1 ) } );
    }

    @Test
    public void shouldDrainAllTuplesWithAtLeastTupleAvailabilityByCountSatisfied ()
    {
        final TupleQueue tupleQueue = new UnboundedTupleQueue( 2 );
        final Tuple tuple1 = new Tuple();
        tupleQueue.offerTuple( tuple1 );
        final Tuple tuple2 = new Tuple();
        tupleQueue.offerTuple( tuple2 );

        final TupleQueueDrainer drainer = new NonBlockingSinglePortDrainer( 1, AT_LEAST );
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
        final TupleQueue tupleQueue = new UnboundedTupleQueue( 2 );

        final TupleQueueDrainer drainer = new NonBlockingSinglePortDrainer( 1, AT_LEAST );
        drainer.drain( null, new TupleQueue[] { tupleQueue } );

        final PortsToTuples portsToTuples = drainer.getResult();
        assertNull( portsToTuples );
    }

    @Test
    public void shouldDrainTuplesWithExactButSameOnAllPortsTupleAvailabilityByCountSatisfied ()
    {
        final TupleQueue tupleQueue = new UnboundedTupleQueue( 2 );
        final Tuple tuple1 = new Tuple();
        tupleQueue.offerTuple( tuple1 );
        final Tuple tuple2 = new Tuple();
        tupleQueue.offerTuple( tuple2 );

        final TupleQueueDrainer drainer = new NonBlockingSinglePortDrainer( 1, AT_LEAST_BUT_SAME_ON_ALL_PORTS );
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
        final TupleQueue tupleQueue = new UnboundedTupleQueue( 2 );

        final TupleQueueDrainer drainer = new NonBlockingSinglePortDrainer( 1, AT_LEAST_BUT_SAME_ON_ALL_PORTS );
        drainer.drain( null, new TupleQueue[] { tupleQueue } );

        final PortsToTuples portsToTuples = drainer.getResult();
        assertNull( portsToTuples );
    }

    @Test
    public void shouldDrainTuplesWithExactTupleAvailabilityByCountSatisfied ()
    {
        final TupleQueue tupleQueue = new UnboundedTupleQueue( 2 );
        final Tuple tuple1 = new Tuple();
        tupleQueue.offerTuple( tuple1 );
        tupleQueue.offerTuple( new Tuple() );

        final TupleQueueDrainer drainer = new NonBlockingSinglePortDrainer( 1, EXACT );
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
        final TupleQueue tupleQueue = new UnboundedTupleQueue( 2 );

        final TupleQueueDrainer drainer = new NonBlockingSinglePortDrainer( 1, EXACT );
        drainer.drain( null, new TupleQueue[] { tupleQueue } );

        final PortsToTuples portsToTuples = drainer.getResult();
        assertNull( portsToTuples );
    }

}
