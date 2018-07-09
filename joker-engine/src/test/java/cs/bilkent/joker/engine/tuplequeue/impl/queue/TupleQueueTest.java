package cs.bilkent.joker.engine.tuplequeue.impl.queue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import cs.bilkent.joker.engine.tuplequeue.TupleQueue;
import cs.bilkent.joker.operator.Tuple;
import cs.bilkent.joker.test.AbstractJokerTest;
import static java.util.Arrays.asList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertTrue;

@RunWith( value = Parameterized.class )
public class TupleQueueTest extends AbstractJokerTest
{

    private static final int QUEUE_CAPACITY = 3;

    @Parameters( name = "queue={0}" )
    public static Collection<Object[]> data ()
    {
        return asList( new Object[][] { { new SingleThreadedTupleQueue( QUEUE_CAPACITY ) },
                                        { new MultiThreadedTupleQueue( QUEUE_CAPACITY ) } } );
    }


    private final TupleQueue queue;

    public TupleQueueTest ( final TupleQueue queue )
    {
        this.queue = queue;
    }

    @Before
    public void init ()
    {
        queue.clear();
    }

    @Test
    public void shouldOfferSingleTuple ()
    {
        final boolean success = queue.offer( new Tuple() );

        assertTrue( success );
        assertThat( queue.size(), equalTo( 1 ) );
    }

    @Test
    public void shouldGrowQueue ()
    {
        queue.offer( new Tuple() );
        queue.offer( new Tuple() );
        queue.offer( new Tuple() );

        assertThat( queue.size(), equalTo( 3 ) );
    }

    @Test
    public void shouldOfferTupleFromIndex ()
    {
        final Tuple tuple = new Tuple();
        queue.offer( asList( new Tuple(), tuple ), 1 );

        final List<Tuple> tuples = queue.poll( 1 );

        assertThat( tuples, hasSize( 1 ) );
        assertTrue( tuple == tuples.get( 0 ) );
    }

    @Test
    public void shouldNotOfferTupleFromInvalidIndex ()
    {
        final int offered = queue.offer( asList( new Tuple(), new Tuple() ), 2 );
        assertThat( offered, equalTo( 0 ) );
    }

    @Test
    public void shouldPollTuples ()
    {
        final Tuple tuple0 = new Tuple();
        final Tuple tuple1 = new Tuple();
        queue.offer( tuple0 );
        queue.offer( tuple1 );

        final List<Tuple> tuples = queue.poll( 2 );

        assertThat( tuples, hasSize( 2 ) );
        assertTrue( tuple0 == tuples.get( 0 ) );
        assertTrue( tuple1 == tuples.get( 1 ) );
        assertThat( queue.size(), equalTo( 0 ) );
    }

    @Test
    public void shouldPollTuplesWithLimit ()
    {
        final Tuple tuple1 = new Tuple();
        final Tuple tuple2 = new Tuple();
        final Tuple tuple3 = new Tuple();
        queue.offer( tuple1 );
        queue.offer( tuple2 );
        queue.offer( tuple3 );

        final List<Tuple> tuples = queue.poll( 2 );

        assertThat( tuples, hasSize( 2 ) );
        assertTrue( tuple1 == tuples.get( 0 ) );
        assertTrue( tuple2 == tuples.get( 1 ) );
        assertThat( queue.size(), equalTo( 1 ) );
    }

    @Test
    public void shouldPollOnlyAvailableTuples ()
    {
        final Tuple tuple0 = new Tuple();
        final Tuple tuple1 = new Tuple();
        queue.offer( tuple0 );
        queue.offer( tuple1 );

        final List<Tuple> tuples = queue.poll( 4 );

        assertThat( tuples, hasSize( 2 ) );
        assertTrue( tuple0 == tuples.get( 0 ) );
        assertTrue( tuple1 == tuples.get( 1 ) );
        assertThat( queue.size(), equalTo( 0 ) );
    }

    @Test
    public void shouldDrainTuples ()
    {
        final Tuple tuple1 = new Tuple();
        final Tuple tuple2 = new Tuple();
        queue.offer( tuple1 );
        queue.offer( tuple2 );

        final List<Tuple> tuples = new ArrayList<>();
        final int polled = queue.drainTo( 2, tuples );

        assertThat( polled, equalTo( 2 ) );
        assertThat( tuples, hasSize( 2 ) );
        assertTrue( tuple1 == tuples.get( 0 ) );
        assertTrue( tuple2 == tuples.get( 1 ) );
        assertThat( queue.size(), equalTo( 0 ) );
    }

    @Test
    public void shouldDrainTuplesWithLimit ()
    {
        final Tuple tuple1 = new Tuple();
        final Tuple tuple2 = new Tuple();
        final Tuple tuple3 = new Tuple();
        queue.offer( tuple1 );
        queue.offer( tuple2 );
        queue.offer( tuple3 );

        final List<Tuple> tuples = new ArrayList<>();
        final int polled = queue.drainTo( 2, tuples );

        assertThat( polled, equalTo( 2 ) );
        assertThat( tuples, hasSize( 2 ) );
        assertTrue( tuple1 == tuples.get( 0 ) );
        assertTrue( tuple2 == tuples.get( 1 ) );
        assertThat( queue.size(), equalTo( 1 ) );
    }

    @Test
    public void shouldDrainOnlyAvailableTuples ()
    {
        final Tuple tuple0 = new Tuple();
        final Tuple tuple1 = new Tuple();
        queue.offer( tuple0 );
        queue.offer( tuple1 );

        final List<Tuple> tuples = new ArrayList<>();
        final int polled = queue.drainTo( 3, tuples );

        assertThat( polled, equalTo( 2 ) );
        assertThat( tuples, hasSize( 2 ) );
        assertTrue( tuple0 == tuples.get( 0 ) );
        assertTrue( tuple1 == tuples.get( 1 ) );
        assertThat( queue.size(), equalTo( 0 ) );
    }

}
