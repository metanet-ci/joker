package cs.bilkent.zanza.engine.tuplequeue.impl.consumer;

import org.junit.Test;

import cs.bilkent.zanza.engine.tuplequeue.TupleQueue;
import cs.bilkent.zanza.engine.tuplequeue.impl.queue.SingleThreadedTupleQueue;
import static cs.bilkent.zanza.flow.Port.DEFAULT_PORT_INDEX;
import cs.bilkent.zanza.operator.PortsToTuples;
import cs.bilkent.zanza.operator.Tuple;
import static cs.bilkent.zanza.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByCount.AT_LEAST;
import static cs.bilkent.zanza.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByCount.AT_LEAST_BUT_SAME_ON_ALL_PORTS;
import static cs.bilkent.zanza.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByCount.EXACT;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;


public class DrainSinglePortTuplesTest
{

    @Test( expected = IllegalArgumentException.class )
    public void shouldFailWithNullTupleQueues ()
    {
        final DrainSinglePortTuples drainSinglePortTuples = new DrainSinglePortTuples( 1, AT_LEAST );
        drainSinglePortTuples.accept( null );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldFailWithEmptyTupleQueues ()
    {
        final DrainSinglePortTuples drainSinglePortTuples = new DrainSinglePortTuples( 1, AT_LEAST );
        drainSinglePortTuples.accept( new TupleQueue[] {} );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldFailWithMultipleTupleQueues ()
    {
        final DrainSinglePortTuples drainSinglePortTuples = new DrainSinglePortTuples( 1, AT_LEAST );
        drainSinglePortTuples.accept( new TupleQueue[] { new SingleThreadedTupleQueue( 1 ), new SingleThreadedTupleQueue( 1 ) } );
    }

    @Test
    public void shouldDrainAllTuples ()
    {
        final TupleQueue tupleQueue = new SingleThreadedTupleQueue( 2 );
        final Tuple tuple1 = new Tuple();
        tupleQueue.offerTuple( tuple1 );
        final Tuple tuple2 = new Tuple();
        tupleQueue.offerTuple( tuple2 );

        final DrainSinglePortTuples drainSinglePortTuples = new DrainSinglePortTuples( 1, AT_LEAST );
        drainSinglePortTuples.accept( new TupleQueue[] { tupleQueue } );

        final PortsToTuples portsToTuples = drainSinglePortTuples.getPortsToTuples();
        assertNotNull( portsToTuples );
        assertThat( portsToTuples.getPortCount(), equalTo( 1 ) );
        assertThat( portsToTuples.getTupleCount( DEFAULT_PORT_INDEX ), equalTo( 2 ) );
        assertTrue( tuple1 == portsToTuples.getTuple( DEFAULT_PORT_INDEX, 0 ) );
        assertTrue( tuple2 == portsToTuples.getTuple( DEFAULT_PORT_INDEX, 1 ) );
        assertThat( tupleQueue.size(), equalTo( 0 ) );
    }

    @Test
    public void shouldDrainExactNumberOfTuplesWithExactButSameOnAllPortsTupleAvailabilityByCount ()
    {
        final TupleQueue tupleQueue = new SingleThreadedTupleQueue( 2 );
        final Tuple tuple1 = new Tuple();
        tupleQueue.offerTuple( tuple1 );
        final Tuple tuple2 = new Tuple();
        tupleQueue.offerTuple( tuple2 );

        final DrainSinglePortTuples drainSinglePortTuples = new DrainSinglePortTuples( 1, AT_LEAST_BUT_SAME_ON_ALL_PORTS );
        drainSinglePortTuples.accept( new TupleQueue[] { tupleQueue } );

        final PortsToTuples portsToTuples = drainSinglePortTuples.getPortsToTuples();
        assertNotNull( portsToTuples );
        assertThat( portsToTuples.getPortCount(), equalTo( 1 ) );
        assertThat( portsToTuples.getTupleCount( DEFAULT_PORT_INDEX ), equalTo( 2 ) );
        assertTrue( tuple1 == portsToTuples.getTuple( DEFAULT_PORT_INDEX, 0 ) );
        assertTrue( tuple2 == portsToTuples.getTuple( DEFAULT_PORT_INDEX, 1 ) );
        assertThat( tupleQueue.size(), equalTo( 0 ) );
    }

    @Test
    public void shouldDrainExactNumberOfTuplesWithExactTupleAvailabilityByCount ()
    {
        final TupleQueue tupleQueue = new SingleThreadedTupleQueue( 2 );
        final Tuple tuple1 = new Tuple();
        tupleQueue.offerTuple( tuple1 );
        tupleQueue.offerTuple( new Tuple() );

        final DrainSinglePortTuples drainSinglePortTuples = new DrainSinglePortTuples( 1, EXACT );
        drainSinglePortTuples.accept( new TupleQueue[] { tupleQueue } );

        final PortsToTuples portsToTuples = drainSinglePortTuples.getPortsToTuples();
        assertNotNull( portsToTuples );
        assertThat( portsToTuples.getPortCount(), equalTo( 1 ) );
        assertThat( portsToTuples.getTupleCount( DEFAULT_PORT_INDEX ), equalTo( 1 ) );
        assertTrue( tuple1 == portsToTuples.getTuple( DEFAULT_PORT_INDEX, 0 ) );

        assertThat( tupleQueue.size(), equalTo( 1 ) );
    }

}
