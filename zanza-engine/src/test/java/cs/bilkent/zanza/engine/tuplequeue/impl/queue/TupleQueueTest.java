package cs.bilkent.zanza.engine.tuplequeue.impl.queue;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import cs.bilkent.zanza.engine.tuplequeue.TupleQueue;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueueManager.TupleQueueThreading;
import static cs.bilkent.zanza.engine.tuplequeue.TupleQueueManager.TupleQueueThreading.MULTI_THREADED;
import static cs.bilkent.zanza.engine.tuplequeue.TupleQueueManager.TupleQueueThreading.SINGLE_THREADED;
import cs.bilkent.zanza.operator.Tuple;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertTrue;

@RunWith( Parameterized.class )
public class TupleQueueTest
{

    @Parameters
    public static Collection<TupleQueueThreading> data ()
    {
        return Arrays.asList( SINGLE_THREADED, MULTI_THREADED );
    }


    private final TupleQueue queue;

    public TupleQueueTest ( TupleQueueThreading tupleQueueThreading )
    {
        this.queue = tupleQueueThreading == SINGLE_THREADED ? new SingleThreadedTupleQueue( 1 ) : new MultiThreadedTupleQueue( 1 );
    }

    @Test
    public void shouldOfferSingleTuple ()
    {
        queue.offerTuple( new Tuple() );

        assertThat( queue.size(), equalTo( 1 ) );
    }

    @Test
    public void shouldResizeQueue ()
    {
        queue.offerTuple( new Tuple() );
        queue.offerTuple( new Tuple() );
        queue.offerTuple( new Tuple() );

        assertThat( queue.size(), equalTo( 3 ) );
    }

    @Test
    public void shouldPollExactNumberOfTuples ()
    {
        final Tuple tuple = new Tuple();
        queue.offerTuple( tuple );

        final List<Tuple> tuples = queue.pollTuples( 1 );

        assertThat( tuples, hasSize( 1 ) );
        assertTrue( tuple == tuples.get( 0 ) );
    }

    @Test
    public void shouldPollExactNumberOfTuplesAndLeaveExtrasInQueue ()
    {
        final Tuple tuple1 = new Tuple();
        final Tuple tuple2 = new Tuple();
        final Tuple tuple3 = new Tuple();
        queue.offerTuple( tuple1 );
        queue.offerTuple( tuple2 );
        queue.offerTuple( tuple3 );

        final List<Tuple> tuples = queue.pollTuples( 2 );

        assertThat( tuples, hasSize( 2 ) );
        assertTrue( tuple1 == tuples.get( 0 ) );
        assertTrue( tuple2 == tuples.get( 1 ) );
        assertThat( queue.size(), equalTo( 1 ) );
    }

    @Test
    public void shouldNotPollExactNumberOfTuplesWhenSizeIsSmaller ()
    {
        queue.offerTuple( new Tuple() );

        final List<Tuple> tuples = queue.pollTuples( 2 );

        assertThat( tuples, hasSize( 0 ) );
        assertThat( queue.size(), equalTo( 1 ) );
    }

    @Test
    public void shouldPollAllTuples ()
    {
        final Tuple tuple1 = new Tuple();
        final Tuple tuple2 = new Tuple();
        queue.offerTuple( tuple1 );
        queue.offerTuple( tuple2 );

        assertThat( queue.size(), equalTo( 2 ) );

        final List<Tuple> tuples = queue.pollTuplesAtLeast( 1 );

        assertThat( tuples, hasSize( 2 ) );
        assertTrue( tuple1 == tuples.get( 0 ) );
        assertTrue( tuple2 == tuples.get( 1 ) );
        assertThat( queue.size(), equalTo( 0 ) );
    }

    @Test
    public void shouldNotPollTuplesWithAtLeastWhenSizeIsSmaller ()
    {
        queue.offerTuple( new Tuple() );

        final List<Tuple> tuples = queue.pollTuplesAtLeast( 2 );

        assertThat( tuples, hasSize( 0 ) );
        assertThat( queue.size(), equalTo( 1 ) );
    }

}
