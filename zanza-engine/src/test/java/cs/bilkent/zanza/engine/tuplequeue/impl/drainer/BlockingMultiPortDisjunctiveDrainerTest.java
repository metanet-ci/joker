package cs.bilkent.zanza.engine.tuplequeue.impl.drainer;

import org.junit.Test;

import static cs.bilkent.zanza.engine.TestUtils.spawnThread;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueue;
import cs.bilkent.zanza.engine.tuplequeue.impl.queue.MultiThreadedTupleQueue;
import static cs.bilkent.zanza.engine.tuplequeue.impl.queue.MultiThreadedTupleQueueTest.offerTuples;
import cs.bilkent.zanza.operator.Tuple;
import cs.bilkent.zanza.operator.impl.TuplesImpl;
import cs.bilkent.zanza.operator.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByCount;
import static java.util.Arrays.asList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertNotNull;

public class BlockingMultiPortDisjunctiveDrainerTest
{

    private static final long TIMEOUT_IN_MILLIS = 5000;

    private final BlockingMultiPortDisjunctiveDrainer drainer = new BlockingMultiPortDisjunctiveDrainer( 2,
                                                                                                         Integer.MAX_VALUE,
                                                                                                         TIMEOUT_IN_MILLIS );

    @Test
    public void test_TupleAvailabilityByCount_AT_LEAST_allQueuesSatisfiesAlready ()
    {
        drainer.setParameters( TupleAvailabilityByCount.AT_LEAST, new int[] { 0, 1 }, new int[] { 2, 2 } );
        final TupleQueue tupleQueue1 = new MultiThreadedTupleQueue( 2 );
        final TupleQueue tupleQueue2 = new MultiThreadedTupleQueue( 2 );
        tupleQueue1.offerTuple( new Tuple() );
        tupleQueue1.offerTuple( new Tuple() );
        tupleQueue2.offerTuple( new Tuple() );
        tupleQueue2.offerTuple( new Tuple() );

        drainer.drain( null, new TupleQueue[] { tupleQueue1, tupleQueue2 } );

        final TuplesImpl tuples = drainer.getResult();
        assertNotNull( tuples );
        assertThat( tuples.getNonEmptyPortCount(), equalTo( 2 ) );
        assertThat( tuples.getTupleCount( 0 ), equalTo( 2 ) );
        assertThat( tuples.getTupleCount( 1 ), equalTo( 2 ) );
        assertThat( tupleQueue1.size(), equalTo( 0 ) );
        assertThat( tupleQueue2.size(), equalTo( 0 ) );
    }

    @Test
    public void test_TupleAvailabilityByCount_AT_LEAST_singleQueueSatisfiesAfterwards () throws InterruptedException
    {
        drainer.setParameters( TupleAvailabilityByCount.AT_LEAST, new int[] { 0, 1 }, new int[] { 2, 2 } );
        final TupleQueue tupleQueue1 = new MultiThreadedTupleQueue( 2 );
        final TupleQueue tupleQueue2 = new MultiThreadedTupleQueue( 2 );
        tupleQueue2.offerTuple( new Tuple() );
        final Thread thread = spawnThread( offerTuples( Thread.currentThread(), tupleQueue1, asList( new Tuple(), new Tuple() ) ) );

        drainer.drain( null, new TupleQueue[] { tupleQueue1, tupleQueue2 } );

        final TuplesImpl tuples = drainer.getResult();
        assertNotNull( tuples );
        assertThat( tuples.getNonEmptyPortCount(), equalTo( 1 ) );
        assertThat( tuples.getTupleCount( 0 ), equalTo( 2 ) );
        assertThat( tupleQueue1.size(), equalTo( 0 ) );
        assertThat( tupleQueue2.size(), equalTo( 1 ) );

        thread.join();
    }

    @Test
    public void test_TupleAvailabilityByCount_EXACT_allQueuesSatisfy ()
    {
        drainer.setParameters( TupleAvailabilityByCount.EXACT, new int[] { 0, 1 }, new int[] { 2, 2 } );
        final TupleQueue tupleQueue1 = new MultiThreadedTupleQueue( 3 );
        final TupleQueue tupleQueue2 = new MultiThreadedTupleQueue( 2 );
        tupleQueue1.offerTuple( new Tuple() );
        tupleQueue1.offerTuple( new Tuple() );
        tupleQueue1.offerTuple( new Tuple() );
        tupleQueue2.offerTuple( new Tuple() );
        tupleQueue2.offerTuple( new Tuple() );

        drainer.drain( null, new TupleQueue[] { tupleQueue1, tupleQueue2 } );

        final TuplesImpl tuples = drainer.getResult();
        assertNotNull( tuples );
        assertThat( tuples.getNonEmptyPortCount(), equalTo( 2 ) );
        assertThat( tuples.getTupleCount( 0 ), equalTo( 2 ) );
        assertThat( tuples.getTupleCount( 1 ), equalTo( 2 ) );
        assertThat( tupleQueue1.size(), equalTo( 1 ) );
        assertThat( tupleQueue2.size(), equalTo( 0 ) );
    }

    @Test
    public void test_TupleAvailabilityByCount_EXACT_allQueuesDoNotSatisfy ()
    {
        drainer.setParameters( TupleAvailabilityByCount.EXACT, new int[] { 0, 1 }, new int[] { 2, 2 } );
        final TupleQueue tupleQueue1 = new MultiThreadedTupleQueue( 3 );
        final TupleQueue tupleQueue2 = new MultiThreadedTupleQueue( 2 );
        tupleQueue1.offerTuple( new Tuple() );
        tupleQueue1.offerTuple( new Tuple() );
        tupleQueue1.offerTuple( new Tuple() );
        tupleQueue2.offerTuple( new Tuple() );

        drainer.drain( null, new TupleQueue[] { tupleQueue1, tupleQueue2 } );

        final TuplesImpl tuples = drainer.getResult();
        assertNotNull( tuples );
        assertThat( tuples.getNonEmptyPortCount(), equalTo( 1 ) );
        assertThat( tuples.getTupleCount( 0 ), equalTo( 2 ) );
        assertThat( tuples.getTupleCount( 1 ), equalTo( 0 ) );
        assertThat( tupleQueue1.size(), equalTo( 1 ) );
        assertThat( tupleQueue2.size(), equalTo( 1 ) );
    }

}
