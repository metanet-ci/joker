package cs.bilkent.zanza.engine.tuplequeue.impl.drainer;

import org.junit.Test;

import cs.bilkent.zanza.engine.tuplequeue.TupleQueue;
import cs.bilkent.zanza.engine.tuplequeue.impl.queue.SingleThreadedTupleQueue;
import cs.bilkent.zanza.operator.Tuple;
import cs.bilkent.zanza.operator.impl.TuplesImpl;
import static cs.bilkent.zanza.operator.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByCount.AT_LEAST;
import static cs.bilkent.zanza.operator.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByCount.AT_LEAST_BUT_SAME_ON_ALL_PORTS;
import static cs.bilkent.zanza.operator.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByCount.EXACT;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class NonBlockingMultiPortConjunctiveDrainerTest
{

    @Test( expected = IllegalArgumentException.class )
    public void test_input_nullTupleQueues ()
    {
        final NonBlockingMultiPortConjunctiveDrainer drainer = new NonBlockingMultiPortConjunctiveDrainer( 1 );
        drainer.drain( null, null );
    }

    @Test( expected = IllegalArgumentException.class )
    public void test_input_emptyTupleQueues ()
    {
        final NonBlockingMultiPortConjunctiveDrainer drainer = new NonBlockingMultiPortConjunctiveDrainer( 1 );
        drainer.drain( null, new TupleQueue[] {} );
    }

    @Test( expected = IllegalArgumentException.class )
    public void test_input_singleTupleQueue ()
    {
        final NonBlockingMultiPortConjunctiveDrainer drainer = new NonBlockingMultiPortConjunctiveDrainer( 2 );
        drainer.setParameters( AT_LEAST, new int[] { 1, 1 } );

        drainer.drain( null, new TupleQueue[] { new SingleThreadedTupleQueue( 1 ) } );
    }

    @Test
    public void test_TupleAvailabilityByCount_AT_LEAST_allQueuesSatisfy ()
    {
        final NonBlockingMultiPortConjunctiveDrainer drainer = new NonBlockingMultiPortConjunctiveDrainer( 2 );
        drainer.setParameters( AT_LEAST, new int[] { 1, 1 } );
        final TupleQueue tupleQueue1 = new SingleThreadedTupleQueue( 1 );
        final TupleQueue tupleQueue2 = new SingleThreadedTupleQueue( 2 );
        tupleQueue1.offerTuple( new Tuple() );
        tupleQueue2.offerTuple( new Tuple() );
        tupleQueue2.offerTuple( new Tuple() );

        drainer.drain( null, new TupleQueue[] { tupleQueue1, tupleQueue2 } );

        final TuplesImpl tuples = drainer.getResult();
        assertNotNull( tuples );
        assertThat( tuples.getTupleCount( 0 ), equalTo( 1 ) );
        assertThat( tuples.getTupleCount( 1 ), equalTo( 2 ) );
        assertThat( tupleQueue1.size(), equalTo( 0 ) );
        assertThat( tupleQueue2.size(), equalTo( 0 ) );
    }

    @Test
    public void test_TupleAvailabilityByCount_AT_LEAST_allQueuesDoNotSatisfy ()
    {
        final NonBlockingMultiPortConjunctiveDrainer drainer = new NonBlockingMultiPortConjunctiveDrainer( 2 );
        drainer.setParameters( AT_LEAST, new int[] { 2, 2 } );
        final TupleQueue tupleQueue1 = new SingleThreadedTupleQueue( 1 );
        final TupleQueue tupleQueue2 = new SingleThreadedTupleQueue( 2 );
        tupleQueue1.offerTuple( new Tuple() );
        tupleQueue2.offerTuple( new Tuple() );
        tupleQueue2.offerTuple( new Tuple() );

        drainer.drain( null, new TupleQueue[] { tupleQueue1, tupleQueue2 } );

        assertNull( drainer.getResult() );
        assertThat( tupleQueue1.size(), equalTo( 1 ) );
        assertThat( tupleQueue2.size(), equalTo( 2 ) );
    }

    @Test
    public void test_TupleAvailabilityByCount_EXACT_allQueuesSatisfy ()
    {
        final NonBlockingMultiPortConjunctiveDrainer drainer = new NonBlockingMultiPortConjunctiveDrainer( 2 );
        drainer.setParameters( EXACT, new int[] { 2, 2 } );
        final TupleQueue tupleQueue1 = new SingleThreadedTupleQueue( 2 );
        final TupleQueue tupleQueue2 = new SingleThreadedTupleQueue( 2 );
        tupleQueue1.offerTuple( new Tuple() );
        tupleQueue1.offerTuple( new Tuple() );
        tupleQueue1.offerTuple( new Tuple() );
        tupleQueue2.offerTuple( new Tuple() );
        tupleQueue2.offerTuple( new Tuple() );

        drainer.drain( null, new TupleQueue[] { tupleQueue1, tupleQueue2 } );

        final TuplesImpl tuples = drainer.getResult();
        assertThat( tuples.getNonEmptyPortCount(), equalTo( 2 ) );
        assertThat( tuples.getTupleCount( 0 ), equalTo( 2 ) );
        assertThat( tuples.getTupleCount( 1 ), equalTo( 2 ) );
        assertThat( tupleQueue1.size(), equalTo( 1 ) );
        assertThat( tupleQueue2.size(), equalTo( 0 ) );
    }

    @Test
    public void test_TupleAvailabilityByCount_EXACT_allQueuesDoNotSatisfy ()
    {
        final NonBlockingMultiPortConjunctiveDrainer drainer = new NonBlockingMultiPortConjunctiveDrainer( 2 );
        drainer.setParameters( EXACT, new int[] { 2, 2 } );
        final TupleQueue tupleQueue1 = new SingleThreadedTupleQueue( 2 );
        final TupleQueue tupleQueue2 = new SingleThreadedTupleQueue( 2 );
        tupleQueue1.offerTuple( new Tuple() );
        tupleQueue1.offerTuple( new Tuple() );
        tupleQueue2.offerTuple( new Tuple() );

        drainer.drain( null, new TupleQueue[] { tupleQueue1, tupleQueue2 } );

        assertNull( drainer.getResult() );
        assertThat( tupleQueue1.size(), equalTo( 2 ) );
        assertThat( tupleQueue2.size(), equalTo( 1 ) );
    }

    @Test
    public void test_TupleAvailabilityByCount_AT_LEAST_BUT_SAME_ON_ALL_PORTS_allQueuesSatisfy ()
    {
        final NonBlockingMultiPortConjunctiveDrainer drainer = new NonBlockingMultiPortConjunctiveDrainer( 2 );
        drainer.setParameters( AT_LEAST_BUT_SAME_ON_ALL_PORTS, new int[] { 2, 2 } );
        final TupleQueue tupleQueue1 = new SingleThreadedTupleQueue( 2 );
        final TupleQueue tupleQueue2 = new SingleThreadedTupleQueue( 2 );
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
    public void test_TupleAvailabilityByCount_AT_LEAST_BUT_SAME_ON_ALL_PORTS_allQueuesDoNotSatisfy ()
    {
        final NonBlockingMultiPortConjunctiveDrainer drainer = new NonBlockingMultiPortConjunctiveDrainer( 2 );
        drainer.setParameters( AT_LEAST_BUT_SAME_ON_ALL_PORTS, new int[] { 2, 2 } );
        final TupleQueue tupleQueue1 = new SingleThreadedTupleQueue( 2 );
        final TupleQueue tupleQueue2 = new SingleThreadedTupleQueue( 2 );
        tupleQueue1.offerTuple( new Tuple() );
        tupleQueue1.offerTuple( new Tuple() );
        tupleQueue2.offerTuple( new Tuple() );

        drainer.drain( null, new TupleQueue[] { tupleQueue1, tupleQueue2 } );

        assertNull( drainer.getResult() );
        assertThat( tupleQueue1.size(), equalTo( 2 ) );
        assertThat( tupleQueue2.size(), equalTo( 1 ) );
    }

}
