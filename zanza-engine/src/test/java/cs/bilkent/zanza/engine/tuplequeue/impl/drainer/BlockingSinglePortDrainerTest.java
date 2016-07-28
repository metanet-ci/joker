package cs.bilkent.zanza.engine.tuplequeue.impl.drainer;

import org.junit.Test;

import static cs.bilkent.zanza.engine.TestUtils.spawnThread;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueue;
import cs.bilkent.zanza.engine.tuplequeue.impl.queue.MultiThreadedTupleQueue;
import static cs.bilkent.zanza.engine.tuplequeue.impl.queue.MultiThreadedTupleQueueTest.offerTuples;
import static cs.bilkent.zanza.flow.Port.DEFAULT_PORT_INDEX;
import cs.bilkent.zanza.operator.Tuple;
import cs.bilkent.zanza.operator.impl.TuplesImpl;
import cs.bilkent.zanza.operator.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByCount;
import static cs.bilkent.zanza.operator.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByCount.AT_LEAST;
import static cs.bilkent.zanza.operator.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByCount.AT_LEAST_BUT_SAME_ON_ALL_PORTS;
import static cs.bilkent.zanza.operator.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByCount.EXACT;
import static java.util.Arrays.asList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;


public class BlockingSinglePortDrainerTest
{

    private static final long TIMEOUT_IN_MILLIS = 5000;

    private final BlockingSinglePortDrainer drainer = new BlockingSinglePortDrainer( Integer.MAX_VALUE, TIMEOUT_IN_MILLIS );

    @Test( expected = IllegalArgumentException.class )
    public void shouldFailWithNullTupleQueues ()
    {
        drainer.setParameters( AT_LEAST, 1 );
        drainer.drain( null, null );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldFailWithEmptyTupleQueues ()
    {
        drainer.setParameters( AT_LEAST, 1 );
        drainer.drain( null, new TupleQueue[] {} );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldFailWithMultipleTupleQueues ()
    {
        drainer.setParameters( AT_LEAST, 1 );
        drainer.drain( null, new TupleQueue[] { new MultiThreadedTupleQueue( 1 ), new MultiThreadedTupleQueue( 1 ) } );
    }

    @Test
    public void shouldDrainAllTuplesWithAtLeastTupleAvailabilityByCountSatisfiesAlready ()
    {
        final TupleQueue tupleQueue = new MultiThreadedTupleQueue( 2 );
        final Tuple tuple1 = new Tuple();
        tupleQueue.offerTuple( tuple1 );
        final Tuple tuple2 = new Tuple();
        tupleQueue.offerTuple( tuple2 );

        drainer.setParameters( AT_LEAST, 1 );
        drainer.drain( null, new TupleQueue[] { tupleQueue } );

        final TuplesImpl tuples = drainer.getResult();
        assertNotNull( tuples );
        assertThat( tuples.getNonEmptyPortCount(), equalTo( 1 ) );
        assertThat( tuples.getTupleCount( DEFAULT_PORT_INDEX ), equalTo( 2 ) );
        assertTrue( tuple1 == tuples.getTupleOrFail( DEFAULT_PORT_INDEX, 0 ) );
        assertTrue( tuple2 == tuples.getTupleOrFail( DEFAULT_PORT_INDEX, 1 ) );
        assertThat( tupleQueue.size(), equalTo( 0 ) );
    }

    @Test
    public void shouldDrainAllTuplesWithAtLeastTupleAvailabilityByCountSatisfiesAfterwards () throws InterruptedException
    {
        final TupleQueue tupleQueue = new MultiThreadedTupleQueue( 2 );
        final Tuple tuple1 = new Tuple();
        final Tuple tuple2 = new Tuple();

        final Thread thread = spawnThread( offerTuples( Thread.currentThread(), tupleQueue, asList( tuple1, tuple2 ) ) );
        drainer.setParameters( AT_LEAST, 1 );
        drainer.drain( null, new TupleQueue[] { tupleQueue } );

        final TuplesImpl tuples = drainer.getResult();
        assertNotNull( tuples );
        assertThat( tuples.getNonEmptyPortCount(), equalTo( 1 ) );
        assertThat( tuples.getTupleCount( DEFAULT_PORT_INDEX ), equalTo( 2 ) );
        assertTrue( tuple1 == tuples.getTupleOrFail( DEFAULT_PORT_INDEX, 0 ) );
        assertTrue( tuple2 == tuples.getTupleOrFail( DEFAULT_PORT_INDEX, 1 ) );
        assertThat( tupleQueue.size(), equalTo( 0 ) );

        thread.join();
    }

    @Test
    public void shouldNotDrainAnyTupleWithAtLeastTupleAvailabilityByCountUnsatisfied ()
    {
        testNoDrain( AT_LEAST );
    }

    @Test
    public void shouldDrainTuplesWithExactButSameOnAllPortsTupleAvailabilityByCountSatisfiedAlready ()
    {
        final TupleQueue tupleQueue = new MultiThreadedTupleQueue( 2 );
        final Tuple tuple1 = new Tuple();
        tupleQueue.offerTuple( tuple1 );
        final Tuple tuple2 = new Tuple();
        tupleQueue.offerTuple( tuple2 );

        drainer.setParameters( AT_LEAST_BUT_SAME_ON_ALL_PORTS, 1 );
        drainer.drain( null, new TupleQueue[] { tupleQueue } );

        final TuplesImpl tuples = drainer.getResult();
        assertNotNull( tuples );
        assertThat( tuples.getNonEmptyPortCount(), equalTo( 1 ) );
        assertThat( tuples.getTupleCount( DEFAULT_PORT_INDEX ), equalTo( 2 ) );
        assertTrue( tuple1 == tuples.getTupleOrFail( DEFAULT_PORT_INDEX, 0 ) );
        assertTrue( tuple2 == tuples.getTupleOrFail( DEFAULT_PORT_INDEX, 1 ) );
        assertThat( tupleQueue.size(), equalTo( 0 ) );
    }

    @Test
    public void shouldDrainTuplesWithExactButSameOnAllPortsTupleAvailabilityByCountSatisfiedAfterwards () throws InterruptedException
    {
        final TupleQueue tupleQueue = new MultiThreadedTupleQueue( 2 );
        final Tuple tuple1 = new Tuple();
        final Tuple tuple2 = new Tuple();

        final Thread thread = spawnThread( offerTuples( Thread.currentThread(), tupleQueue, asList( tuple1, tuple2 ) ) );
        drainer.setParameters( AT_LEAST_BUT_SAME_ON_ALL_PORTS, 1 );
        drainer.drain( null, new TupleQueue[] { tupleQueue } );

        final TuplesImpl tuples = drainer.getResult();
        assertNotNull( tuples );
        assertThat( tuples.getNonEmptyPortCount(), equalTo( 1 ) );
        assertThat( tuples.getTupleCount( DEFAULT_PORT_INDEX ), equalTo( 2 ) );
        assertTrue( tuple1 == tuples.getTupleOrFail( DEFAULT_PORT_INDEX, 0 ) );
        assertTrue( tuple2 == tuples.getTupleOrFail( DEFAULT_PORT_INDEX, 1 ) );
        assertThat( tupleQueue.size(), equalTo( 0 ) );

        thread.join();
    }

    @Test
    public void shouldNotDrainAnyTupleWithExactButSameOnAllPortsTupleAvailabilityByCountUnsatisfied ()
    {
        testNoDrain( AT_LEAST_BUT_SAME_ON_ALL_PORTS );
    }

    @Test
    public void shouldDrainTuplesWithExactTupleAvailabilityByCountSatisfiedAlready ()
    {
        final TupleQueue tupleQueue = new MultiThreadedTupleQueue( 2 );
        final Tuple tuple1 = new Tuple();
        tupleQueue.offerTuple( tuple1 );
        tupleQueue.offerTuple( new Tuple() );

        drainer.setParameters( EXACT, 1 );
        drainer.drain( null, new TupleQueue[] { tupleQueue } );

        final TuplesImpl tuples = drainer.getResult();
        assertNotNull( tuples );
        assertThat( tuples.getNonEmptyPortCount(), equalTo( 1 ) );
        assertThat( tuples.getTupleCount( DEFAULT_PORT_INDEX ), equalTo( 1 ) );
        assertTrue( tuple1 == tuples.getTupleOrFail( DEFAULT_PORT_INDEX, 0 ) );

        assertThat( tupleQueue.size(), equalTo( 1 ) );
    }

    @Test
    public void shouldDrainTuplesWithExactTupleAvailabilityByCountSatisfiedAfterwards () throws InterruptedException
    {
        final TupleQueue tupleQueue = new MultiThreadedTupleQueue( 2 );
        final Tuple tuple1 = new Tuple();

        final Thread thread = spawnThread( offerTuples( Thread.currentThread(), tupleQueue, asList( tuple1, new Tuple() ) ) );

        drainer.setParameters( EXACT, 1 );
        drainer.drain( null, new TupleQueue[] { tupleQueue } );

        final TuplesImpl tuples = drainer.getResult();
        assertNotNull( tuples );
        assertThat( tuples.getNonEmptyPortCount(), equalTo( 1 ) );
        assertThat( tuples.getTupleCount( DEFAULT_PORT_INDEX ), equalTo( 1 ) );
        assertTrue( tuple1 == tuples.getTupleOrFail( DEFAULT_PORT_INDEX, 0 ) );

        assertThat( tupleQueue.size(), equalTo( 1 ) );

        thread.join();
    }

    @Test
    public void shouldNotDrainAnyTupleWithExactTupleAvailabilityByCountUnsatisfied ()
    {
        testNoDrain( EXACT );
    }

    private void testNoDrain ( final TupleAvailabilityByCount tupleAvailabilityByCount )
    {
        final TupleQueue tupleQueue = new MultiThreadedTupleQueue( 2 );

        drainer.setParameters( tupleAvailabilityByCount, 1 );
        drainer.drain( null, new TupleQueue[] { tupleQueue } );

        assertNull( drainer.getResult() );
    }

}
