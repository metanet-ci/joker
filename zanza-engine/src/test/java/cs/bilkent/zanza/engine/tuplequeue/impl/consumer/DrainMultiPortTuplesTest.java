package cs.bilkent.zanza.engine.tuplequeue.impl.consumer;

import org.junit.Test;

import cs.bilkent.zanza.engine.tuplequeue.TupleQueue;
import cs.bilkent.zanza.engine.tuplequeue.impl.queue.SingleThreadedTupleQueue;
import static cs.bilkent.zanza.flow.Port.DEFAULT_PORT_INDEX;
import cs.bilkent.zanza.operator.PortsToTuples;
import cs.bilkent.zanza.operator.Tuple;
import static cs.bilkent.zanza.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByCount.AT_LEAST_BUT_SAME_ON_ALL_PORTS;
import static cs.bilkent.zanza.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByCount.EXACT;
import static cs.bilkent.zanza.scheduling.ScheduleWhenTuplesAvailable.scheduleWhenTuplesAvailableOnAll;
import static cs.bilkent.zanza.scheduling.ScheduleWhenTuplesAvailable.scheduleWhenTuplesAvailableOnAny;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;


public class DrainMultiPortTuplesTest
{

    private static final int PORT_0 = DEFAULT_PORT_INDEX;

    private static final int PORT_1 = DEFAULT_PORT_INDEX + 1;

    @Test( expected = IllegalArgumentException.class )
    public void test_input_nullTupleQueues ()
    {
        final DrainMultiPortTuples drainMultiPortTuples = new DrainMultiPortTuples( scheduleWhenTuplesAvailableOnAll( 1, PORT_0, PORT_1 ) );
        drainMultiPortTuples.accept( null );
    }

    @Test( expected = IllegalArgumentException.class )
    public void test_input_emptyTupleQueues ()
    {
        final DrainMultiPortTuples drainMultiPortTuples = new DrainMultiPortTuples( scheduleWhenTuplesAvailableOnAll( 1, PORT_0, PORT_1 ) );
        drainMultiPortTuples.accept( new TupleQueue[] {} );
    }

    @Test( expected = IllegalArgumentException.class )
    public void test_input_singleTupleQueue ()
    {
        final DrainMultiPortTuples drainMultiPortTuples = new DrainMultiPortTuples( scheduleWhenTuplesAvailableOnAll( 1, PORT_0, PORT_1 ) );
        drainMultiPortTuples.accept( new TupleQueue[] { new SingleThreadedTupleQueue( 1 ) } );
    }

    @Test
    public void test_TupleAvailabilityByPort_ALL_PORTS_TupleAvailabilityByCount_AT_LEAST_allQueuesSatisfy ()
    {
        final DrainMultiPortTuples drainMultiPortTuples = new DrainMultiPortTuples( scheduleWhenTuplesAvailableOnAll( 1, PORT_0, PORT_1 ) );
        final TupleQueue tupleQueue1 = new SingleThreadedTupleQueue( 1 );
        final TupleQueue tupleQueue2 = new SingleThreadedTupleQueue( 2 );
        tupleQueue1.offerTuple( new Tuple() );
        tupleQueue2.offerTuple( new Tuple() );
        tupleQueue2.offerTuple( new Tuple() );

        drainMultiPortTuples.accept( new TupleQueue[] { tupleQueue1, tupleQueue2 } );

        final PortsToTuples portsToTuples = drainMultiPortTuples.getPortsToTuples();
        assertNotNull( portsToTuples );
        assertThat( portsToTuples.getTupleCount( PORT_0 ), equalTo( 1 ) );
        assertThat( portsToTuples.getTupleCount( PORT_1 ), equalTo( 2 ) );
        assertThat( tupleQueue1.size(), equalTo( 0 ) );
        assertThat( tupleQueue2.size(), equalTo( 0 ) );
    }

    @Test
    public void test_TupleAvailabilityByPort_ALL_PORTS_TupleAvailabilityByCount_AT_LEAST_allQueuesDoNotSatisfy ()
    {
        final DrainMultiPortTuples drainMultiPortTuples = new DrainMultiPortTuples( scheduleWhenTuplesAvailableOnAll( 2, PORT_0, PORT_1 ) );
        final TupleQueue tupleQueue1 = new SingleThreadedTupleQueue( 1 );
        final TupleQueue tupleQueue2 = new SingleThreadedTupleQueue( 2 );
        tupleQueue1.offerTuple( new Tuple() );
        tupleQueue2.offerTuple( new Tuple() );
        tupleQueue2.offerTuple( new Tuple() );

        drainMultiPortTuples.accept( new TupleQueue[] { tupleQueue1, tupleQueue2 } );

        assertNull( drainMultiPortTuples.getPortsToTuples() );
        assertThat( tupleQueue1.size(), equalTo( 1 ) );
        assertThat( tupleQueue2.size(), equalTo( 2 ) );
    }

    @Test
    public void test_TupleAvailabilityByPort_ANY_PORT_TupleAvailabilityByCount_AT_LEAST_allQueuesSatisfy ()
    {
        final DrainMultiPortTuples drainMultiPortTuples = new DrainMultiPortTuples( scheduleWhenTuplesAvailableOnAny( 2, PORT_0, PORT_1 ) );
        final TupleQueue tupleQueue1 = new SingleThreadedTupleQueue( 2 );
        final TupleQueue tupleQueue2 = new SingleThreadedTupleQueue( 2 );
        tupleQueue1.offerTuple( new Tuple() );
        tupleQueue1.offerTuple( new Tuple() );
        tupleQueue2.offerTuple( new Tuple() );
        tupleQueue2.offerTuple( new Tuple() );

        drainMultiPortTuples.accept( new TupleQueue[] { tupleQueue1, tupleQueue2 } );

        final PortsToTuples portsToTuples = drainMultiPortTuples.getPortsToTuples();
        assertNotNull( portsToTuples );
        assertThat( portsToTuples.getPortCount(), equalTo( 2 ) );
        assertThat( portsToTuples.getTupleCount( PORT_0 ), equalTo( 2 ) );
        assertThat( portsToTuples.getTupleCount( PORT_1 ), equalTo( 2 ) );
        assertThat( tupleQueue1.size(), equalTo( 0 ) );
        assertThat( tupleQueue2.size(), equalTo( 0 ) );
    }

    @Test
    public void test_TupleAvailabilityByPort_ANY_PORT_TupleAvailabilityByCount_AT_LEAST_allQueuesDoNotSatisfy ()
    {
        final DrainMultiPortTuples drainMultiPortTuples = new DrainMultiPortTuples( scheduleWhenTuplesAvailableOnAny( 2, PORT_0, PORT_1 ) );
        final TupleQueue tupleQueue1 = new SingleThreadedTupleQueue( 2 );
        final TupleQueue tupleQueue2 = new SingleThreadedTupleQueue( 2 );
        tupleQueue1.offerTuple( new Tuple() );
        tupleQueue1.offerTuple( new Tuple() );
        tupleQueue2.offerTuple( new Tuple() );

        drainMultiPortTuples.accept( new TupleQueue[] { tupleQueue1, tupleQueue2 } );

        final PortsToTuples portsToTuples = drainMultiPortTuples.getPortsToTuples();
        assertNotNull( portsToTuples );
        assertThat( portsToTuples.getPortCount(), equalTo( 1 ) );
        assertThat( portsToTuples.getTupleCount( PORT_0 ), equalTo( 2 ) );
        assertThat( tupleQueue1.size(), equalTo( 0 ) );
        assertThat( tupleQueue2.size(), equalTo( 1 ) );
    }


    @Test
    public void test_TupleAvailabilityByPort_ALL_PORTS_TupleAvailabilityByCount_EXACT_allQueuesSatisfy ()
    {
        final DrainMultiPortTuples drainMultiPortTuples = new DrainMultiPortTuples( scheduleWhenTuplesAvailableOnAll( EXACT,
                                                                                                                      2,
                                                                                                                      PORT_0,
                                                                                                                      PORT_1 ) );
        final TupleQueue tupleQueue1 = new SingleThreadedTupleQueue( 2 );
        final TupleQueue tupleQueue2 = new SingleThreadedTupleQueue( 2 );
        tupleQueue1.offerTuple( new Tuple() );
        tupleQueue1.offerTuple( new Tuple() );
        tupleQueue1.offerTuple( new Tuple() );
        tupleQueue2.offerTuple( new Tuple() );
        tupleQueue2.offerTuple( new Tuple() );

        drainMultiPortTuples.accept( new TupleQueue[] { tupleQueue1, tupleQueue2 } );

        final PortsToTuples portsToTuples = drainMultiPortTuples.getPortsToTuples();
        assertThat( portsToTuples.getPortCount(), equalTo( 2 ) );
        assertThat( portsToTuples.getTupleCount( PORT_0 ), equalTo( 2 ) );
        assertThat( portsToTuples.getTupleCount( PORT_1 ), equalTo( 2 ) );
        assertThat( tupleQueue1.size(), equalTo( 1 ) );
        assertThat( tupleQueue2.size(), equalTo( 0 ) );
    }

    @Test
    public void test_TupleAvailabilityByPort_ALL_PORTS_TupleAvailabilityByCount_EXACT_allQueuesDoNotSatisfy ()
    {
        final DrainMultiPortTuples drainMultiPortTuples = new DrainMultiPortTuples( scheduleWhenTuplesAvailableOnAll( EXACT,
                                                                                                                      2,
                                                                                                                      PORT_0,
                                                                                                                      PORT_1 ) );
        final TupleQueue tupleQueue1 = new SingleThreadedTupleQueue( 2 );
        final TupleQueue tupleQueue2 = new SingleThreadedTupleQueue( 2 );
        tupleQueue1.offerTuple( new Tuple() );
        tupleQueue1.offerTuple( new Tuple() );
        tupleQueue2.offerTuple( new Tuple() );

        drainMultiPortTuples.accept( new TupleQueue[] { tupleQueue1, tupleQueue2 } );

        assertNull( drainMultiPortTuples.getPortsToTuples() );
        assertThat( tupleQueue1.size(), equalTo( 2 ) );
        assertThat( tupleQueue2.size(), equalTo( 1 ) );
    }

    @Test
    public void test_TupleAvailabilityByPort_ANY_PORT_TupleAvailabilityByCount_EXACT_allQueuesSatisfy ()
    {
        final DrainMultiPortTuples drainMultiPortTuples = new DrainMultiPortTuples( scheduleWhenTuplesAvailableOnAny( EXACT,
                                                                                                                      2,
                                                                                                                      PORT_0,
                                                                                                                      PORT_1 ) );
        final TupleQueue tupleQueue1 = new SingleThreadedTupleQueue( 2 );
        final TupleQueue tupleQueue2 = new SingleThreadedTupleQueue( 2 );
        tupleQueue1.offerTuple( new Tuple() );
        tupleQueue1.offerTuple( new Tuple() );
        tupleQueue1.offerTuple( new Tuple() );
        tupleQueue2.offerTuple( new Tuple() );
        tupleQueue2.offerTuple( new Tuple() );

        drainMultiPortTuples.accept( new TupleQueue[] { tupleQueue1, tupleQueue2 } );

        final PortsToTuples portsToTuples = drainMultiPortTuples.getPortsToTuples();
        assertNotNull( portsToTuples );
        assertThat( portsToTuples.getPortCount(), equalTo( 2 ) );
        assertThat( portsToTuples.getTupleCount( PORT_0 ), equalTo( 2 ) );
        assertThat( portsToTuples.getTupleCount( PORT_0 ), equalTo( 2 ) );
        assertThat( tupleQueue1.size(), equalTo( 1 ) );
        assertThat( tupleQueue2.size(), equalTo( 0 ) );
    }

    @Test
    public void test_TupleAvailabilityByPort_ANY_PORT_TupleAvailabilityByCount_EXACT_allQueuesDoNotSatisfy ()
    {
        final DrainMultiPortTuples drainMultiPortTuples = new DrainMultiPortTuples( scheduleWhenTuplesAvailableOnAny( EXACT,
                                                                                                                      2,
                                                                                                                      PORT_0,
                                                                                                                      PORT_1 ) );
        final TupleQueue tupleQueue1 = new SingleThreadedTupleQueue( 2 );
        final TupleQueue tupleQueue2 = new SingleThreadedTupleQueue( 2 );
        tupleQueue1.offerTuple( new Tuple() );
        tupleQueue1.offerTuple( new Tuple() );
        tupleQueue1.offerTuple( new Tuple() );
        tupleQueue2.offerTuple( new Tuple() );

        drainMultiPortTuples.accept( new TupleQueue[] { tupleQueue1, tupleQueue2 } );

        final PortsToTuples portsToTuples = drainMultiPortTuples.getPortsToTuples();
        assertNotNull( portsToTuples );
        assertThat( portsToTuples.getPortCount(), equalTo( 1 ) );
        assertThat( portsToTuples.getTupleCount( PORT_0 ), equalTo( 2 ) );
        assertThat( tupleQueue1.size(), equalTo( 1 ) );
        assertThat( tupleQueue2.size(), equalTo( 1 ) );
    }

    @Test
    public void test_TupleAvailabilityByPort_ALL_PORTS_TupleAvailabilityByCount_AT_LEAST_BUT_SAME_ON_ALL_PORTS_allQueuesSatisfy ()
    {
        final DrainMultiPortTuples drainMultiPortTuples = new DrainMultiPortTuples( scheduleWhenTuplesAvailableOnAll(
                AT_LEAST_BUT_SAME_ON_ALL_PORTS,
                2,
                PORT_0,
                PORT_1 ) );
        final TupleQueue tupleQueue1 = new SingleThreadedTupleQueue( 2 );
        final TupleQueue tupleQueue2 = new SingleThreadedTupleQueue( 2 );
        tupleQueue1.offerTuple( new Tuple() );
        tupleQueue1.offerTuple( new Tuple() );
        tupleQueue1.offerTuple( new Tuple() );
        tupleQueue2.offerTuple( new Tuple() );
        tupleQueue2.offerTuple( new Tuple() );

        drainMultiPortTuples.accept( new TupleQueue[] { tupleQueue1, tupleQueue2 } );

        final PortsToTuples portsToTuples = drainMultiPortTuples.getPortsToTuples();
        assertNotNull( portsToTuples );
        assertThat( portsToTuples.getPortCount(), equalTo( 2 ) );
        assertThat( portsToTuples.getTupleCount( PORT_0 ), equalTo( 2 ) );
        assertThat( portsToTuples.getTupleCount( PORT_0 ), equalTo( 2 ) );
        assertThat( tupleQueue1.size(), equalTo( 1 ) );
        assertThat( tupleQueue2.size(), equalTo( 0 ) );
    }

    @Test
    public void test_TupleAvailabilityByPort_ALL_PORTS_TupleAvailabilityByCount_AT_LEAST_BUT_SAME_ON_ALL_PORTS_allQueuesDoNotSatisfy ()
    {
        final DrainMultiPortTuples drainMultiPortTuples = new DrainMultiPortTuples( scheduleWhenTuplesAvailableOnAll(
                AT_LEAST_BUT_SAME_ON_ALL_PORTS,
                2,
                PORT_0,
                PORT_1 ) );
        final TupleQueue tupleQueue1 = new SingleThreadedTupleQueue( 2 );
        final TupleQueue tupleQueue2 = new SingleThreadedTupleQueue( 2 );
        tupleQueue1.offerTuple( new Tuple() );
        tupleQueue1.offerTuple( new Tuple() );
        tupleQueue2.offerTuple( new Tuple() );

        drainMultiPortTuples.accept( new TupleQueue[] { tupleQueue1, tupleQueue2 } );

        assertNull( drainMultiPortTuples.getPortsToTuples() );
        assertThat( tupleQueue1.size(), equalTo( 2 ) );
        assertThat( tupleQueue2.size(), equalTo( 1 ) );
    }

    @Test( expected = IllegalArgumentException.class )
    public void test_TupleAvailabilityByPort_ANY_PORT_TupleAvailabilityByCount_AT_LEAST_BUT_SAME_ON_ALL_PORTS ()
    {
        new DrainMultiPortTuples( scheduleWhenTuplesAvailableOnAny( AT_LEAST_BUT_SAME_ON_ALL_PORTS, 2, PORT_0, PORT_1 ) );
    }

}
