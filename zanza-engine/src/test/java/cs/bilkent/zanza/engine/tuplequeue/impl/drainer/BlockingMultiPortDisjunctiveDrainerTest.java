package cs.bilkent.zanza.engine.tuplequeue.impl.drainer;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import static cs.bilkent.zanza.engine.TestUtils.spawnThread;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueue;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueueDrainer;
import cs.bilkent.zanza.engine.tuplequeue.impl.queue.MultiThreadedTupleQueue;
import static cs.bilkent.zanza.engine.tuplequeue.impl.queue.MultiThreadedTupleQueueTest.offerTuples;
import cs.bilkent.zanza.operator.PortsToTuples;
import cs.bilkent.zanza.operator.Tuple;
import cs.bilkent.zanza.operator.scheduling.ScheduleWhenTuplesAvailable.PortToTupleCount;
import cs.bilkent.zanza.operator.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByCount;
import static java.util.Arrays.asList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertNotNull;

public class BlockingMultiPortDisjunctiveDrainerTest
{

    private static final int TIMEOUT_IN_MILLIS = 5000;

    @Test
    public void test_TupleAvailabilityByCount_AT_LEAST_allQueuesSatisfiesAlready ()
    {
        final TupleQueueDrainer drainer = new BlockingMultiPortDisjunctiveDrainer( TupleAvailabilityByCount.AT_LEAST,
                                                                                   getPortToTupleCounts( 2, 2 ),
                                                                                   TIMEOUT_IN_MILLIS );
        final TupleQueue tupleQueue1 = new MultiThreadedTupleQueue( 2 );
        final TupleQueue tupleQueue2 = new MultiThreadedTupleQueue( 2 );
        tupleQueue1.offerTuple( new Tuple() );
        tupleQueue1.offerTuple( new Tuple() );
        tupleQueue2.offerTuple( new Tuple() );
        tupleQueue2.offerTuple( new Tuple() );

        drainer.drain( null, new TupleQueue[] { tupleQueue1, tupleQueue2 } );

        final PortsToTuples portsToTuples = drainer.getResult();
        assertNotNull( portsToTuples );
        assertThat( portsToTuples.getPortCount(), equalTo( 2 ) );
        assertThat( portsToTuples.getTupleCount( 0 ), equalTo( 2 ) );
        assertThat( portsToTuples.getTupleCount( 1 ), equalTo( 2 ) );
        assertThat( tupleQueue1.size(), equalTo( 0 ) );
        assertThat( tupleQueue2.size(), equalTo( 0 ) );
    }

    @Test
    public void test_TupleAvailabilityByCount_AT_LEAST_singleQueueSatisfiesAfterwards ()
    {
        final TupleQueueDrainer drainer = new BlockingMultiPortDisjunctiveDrainer( TupleAvailabilityByCount.AT_LEAST,
                                                                                   getPortToTupleCounts( 2, 2 ),
                                                                                   TIMEOUT_IN_MILLIS );
        final TupleQueue tupleQueue1 = new MultiThreadedTupleQueue( 2 );
        final TupleQueue tupleQueue2 = new MultiThreadedTupleQueue( 2 );
        tupleQueue2.offerTuple( new Tuple() );
        spawnThread( offerTuples( Thread.currentThread(), tupleQueue1, asList( new Tuple(), new Tuple() ) ) );

        drainer.drain( null, new TupleQueue[] { tupleQueue1, tupleQueue2 } );

        final PortsToTuples portsToTuples = drainer.getResult();
        assertNotNull( portsToTuples );
        assertThat( portsToTuples.getPortCount(), equalTo( 1 ) );
        assertThat( portsToTuples.getTupleCount( 0 ), equalTo( 2 ) );
        assertThat( tupleQueue1.size(), equalTo( 0 ) );
        assertThat( tupleQueue2.size(), equalTo( 1 ) );
    }

    @Test
    public void test_TupleAvailabilityByCount_EXACT_allQueuesSatisfy ()
    {
        final TupleQueueDrainer drainer = new BlockingMultiPortDisjunctiveDrainer( TupleAvailabilityByCount.EXACT,
                                                                                   getPortToTupleCounts( 2, 2 ),
                                                                                   TIMEOUT_IN_MILLIS );
        final TupleQueue tupleQueue1 = new MultiThreadedTupleQueue( 3 );
        final TupleQueue tupleQueue2 = new MultiThreadedTupleQueue( 2 );
        tupleQueue1.offerTuple( new Tuple() );
        tupleQueue1.offerTuple( new Tuple() );
        tupleQueue1.offerTuple( new Tuple() );
        tupleQueue2.offerTuple( new Tuple() );
        tupleQueue2.offerTuple( new Tuple() );

        drainer.drain( null, new TupleQueue[] { tupleQueue1, tupleQueue2 } );

        final PortsToTuples portsToTuples = drainer.getResult();
        assertNotNull( portsToTuples );
        assertThat( portsToTuples.getPortCount(), equalTo( 2 ) );
        assertThat( portsToTuples.getTupleCount( 0 ), equalTo( 2 ) );
        assertThat( portsToTuples.getTupleCount( 1 ), equalTo( 2 ) );
        assertThat( tupleQueue1.size(), equalTo( 1 ) );
        assertThat( tupleQueue2.size(), equalTo( 0 ) );
    }

    @Test
    public void test_TupleAvailabilityByCount_EXACT_allQueuesDoNotSatisfy ()
    {
        final TupleQueueDrainer drainer = new BlockingMultiPortDisjunctiveDrainer( TupleAvailabilityByCount.EXACT,
                                                                                   getPortToTupleCounts( 2, 2 ),
                                                                                   TIMEOUT_IN_MILLIS );
        final TupleQueue tupleQueue1 = new MultiThreadedTupleQueue( 3 );
        final TupleQueue tupleQueue2 = new MultiThreadedTupleQueue( 2 );
        tupleQueue1.offerTuple( new Tuple() );
        tupleQueue1.offerTuple( new Tuple() );
        tupleQueue1.offerTuple( new Tuple() );
        tupleQueue2.offerTuple( new Tuple() );

        drainer.drain( null, new TupleQueue[] { tupleQueue1, tupleQueue2 } );

        final PortsToTuples portsToTuples = drainer.getResult();
        assertNotNull( portsToTuples );
        assertThat( portsToTuples.getPortCount(), equalTo( 1 ) );
        assertThat( portsToTuples.getTupleCount( 0 ), equalTo( 2 ) );
        assertThat( tupleQueue1.size(), equalTo( 1 ) );
        assertThat( tupleQueue2.size(), equalTo( 1 ) );
    }

    private List<PortToTupleCount> getPortToTupleCounts ( final int port0Count, final int port1Count )
    {
        final List<PortToTupleCount> counts = new ArrayList<>();
        counts.add( new PortToTupleCount( 0, port0Count ) );
        counts.add( new PortToTupleCount( 1, port1Count ) );
        return counts;
    }

}
