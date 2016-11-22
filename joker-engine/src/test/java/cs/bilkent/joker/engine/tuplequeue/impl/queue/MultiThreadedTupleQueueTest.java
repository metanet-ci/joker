package cs.bilkent.joker.engine.tuplequeue.impl.queue;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import cs.bilkent.joker.engine.tuplequeue.TupleQueue;
import cs.bilkent.joker.operator.Tuple;
import cs.bilkent.joker.test.AbstractJokerTest;
import static java.lang.Thread.State.TIMED_WAITING;
import static java.lang.Thread.State.WAITING;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class MultiThreadedTupleQueueTest extends AbstractJokerTest
{

    private static final int TIMEOUT_IN_MILLIS = 5000;

    private final TupleQueue queue = new MultiThreadedTupleQueue( 3 );

    @Test
    public void shouldOfferSingleTuple ()
    {
        final boolean success = queue.offerTuple( newTuple( 1 ) );

        assertTrue( success );
        assertQueueContent( 1 );
    }

    @Test
    public void shouldFillQueue ()
    {
        queue.offerTuple( newTuple( 1 ) );
        queue.offerTuple( newTuple( 2 ) );
        queue.offerTuple( newTuple( 3 ) );

        assertQueueContent( 3 );
    }

    @Test
    public void shouldNotOfferTupleOnceFilled ()
    {
        queue.offerTuple( newTuple( 1 ) );
        queue.offerTuple( newTuple( 2 ) );
        queue.offerTuple( newTuple( 3 ) );

        final boolean offered = queue.offerTuple( newTuple( 4 ) );
        assertFalse( offered );

        assertQueueContent( 3 );
    }

    @Test
    public void shouldNotOfferTupleExceedinglyWithTimeout ()
    {
        queue.offerTuple( newTuple( 1 ) );
        queue.offerTuple( newTuple( 2 ) );
        queue.offerTuple( newTuple( 3 ) );

        final boolean result = queue.offerTuple( newTuple( 4 ), TIMEOUT_IN_MILLIS, MILLISECONDS );

        assertFalse( result );
        assertQueueContent( 3 );
    }

    @Test
    public void shouldOfferTuplesWithTimeout ()
    {
        final int offered = queue.offerTuples( asList( newTuple( 1 ), newTuple( 2 ) ), TIMEOUT_IN_MILLIS, MILLISECONDS );

        assertEquals( 2, offered );
        assertQueueContent( 2 );
    }

    @Test
    public void shouldOfferTuplesFromIndexWithTimeout ()
    {
        final int offered = queue.offerTuples( asList( newTuple( 0 ), newTuple( 1 ), newTuple( 2 ) ), 1, TIMEOUT_IN_MILLIS, MILLISECONDS );

        assertEquals( 2, offered );
        assertQueueContent( 2 );
    }

    @Test
    public void shouldOfferEmptyTuplesWithTimeout ()
    {
        final int offered = queue.offerTuples( asList( newTuple( 1 ), newTuple( 2 ) ), 2, TIMEOUT_IN_MILLIS, MILLISECONDS );

        assertEquals( 0, offered );
    }

    @Test
    public void shouldOfferTuplesPartiallyWithTimeout ()
    {
        final int offered = queue.offerTuples( asList( newTuple( 1 ), newTuple( 2 ), newTuple( 3 ), newTuple( 4 ) ),
                                               TIMEOUT_IN_MILLIS,
                                               MILLISECONDS );

        assertEquals( 3, offered );
        assertQueueContent( 3 );
    }

    @Test
    public void shouldOfferTuplesFromIndexPartiallyWithTimeout ()
    {
        final int offered = queue.offerTuples( asList( newTuple( 0 ), newTuple( 1 ), newTuple( 2 ), newTuple( 3 ), newTuple( 4 ) ),
                                               1,
                                               TIMEOUT_IN_MILLIS,
                                               MILLISECONDS );

        assertEquals( 3, offered );
        assertQueueContent( 3 );
    }

    @Test
    public void shouldOfferTuplesExceedinglyWhenTheyArePolledInParallel () throws InterruptedException
    {
        final Thread thread = spawnThread( () -> queue.pollTuplesAtLeast( 1, TIMEOUT_IN_MILLIS, MILLISECONDS ) );

        final int offered = queue.offerTuples( asList( newTuple( 1 ), newTuple( 2 ), newTuple( 3 ), newTuple( 4 ) ),
                                               TIMEOUT_IN_MILLIS,
                                               MILLISECONDS );

        assertEquals( 4, offered );
        thread.join();
    }

    @Test
    public void shouldOfferTuplesFromIndexExceedinglyWhenTheyArePolledInParallel () throws InterruptedException
    {
        final Thread thread = spawnThread( () -> queue.pollTuplesAtLeast( 2, TIMEOUT_IN_MILLIS, MILLISECONDS ) );

        final int offered = queue.offerTuples( asList( newTuple( 1 ), newTuple( 2 ), newTuple( 3 ), newTuple( 4 ), newTuple( 5 ) ),
                                               1,
                                               TIMEOUT_IN_MILLIS,
                                               MILLISECONDS );

        assertEquals( 4, offered );
        thread.join();
    }

    @Test
    public void shouldOfferTuplesWhenCapacityAvailable ()
    {
        final int offeredCount = queue.offerTuples( asList( newTuple( 1 ), newTuple( 2 ) ) );

        assertEquals( 2, offeredCount );
        assertQueueContent( 2 );
    }

    @Test
    public void shouldOfferTuplesFromIndexWhenCapacityAvailable ()
    {
        final int offeredCount = queue.offerTuples( asList( newTuple( 0 ), newTuple( 1 ), newTuple( 2 ) ), 1 );

        assertEquals( 2, offeredCount );
        assertQueueContent( 2 );
    }

    @Test
    public void shouldOfferTuplesPartiallyWhenCapacityNotEnough ()
    {
        final int offeredCount = queue.offerTuples( asList( newTuple( 1 ), newTuple( 2 ), newTuple( 3 ), newTuple( 4 ) ) );

        assertEquals( 3, offeredCount );
        assertQueueContent( 3 );
    }

    @Test
    public void shouldOfferTuplesFromIndexPartiallyWhenCapacityNotEnough ()
    {
        final int offeredCount = queue.offerTuples( asList( newTuple( 0 ), newTuple( 1 ), newTuple( 2 ), newTuple( 3 ), newTuple( 4 ) ),
                                                    1 );

        assertEquals( 3, offeredCount );
        assertQueueContent( 3 );
    }

    @Test
    public void shouldNotOfferEmptyTuples ()
    {
        final int offered = queue.offerTuples( asList( newTuple( 1 ), newTuple( 2 ) ), 2 );

        assertEquals( 0, offered );
    }

    @Test
    public void shouldAwaitSizeWithTimeoutSucceedWhenExpectedSizeIsAlreadyAvailable ()
    {
        queue.offerTuples( asList( newTuple( 1 ), newTuple( 2 ), newTuple( 3 ) ) );
        assertTrue( queue.awaitMinimumSize( 3, TIMEOUT_IN_MILLIS, MILLISECONDS ) );
    }

    @Test
    public void shouldAwaitSizeWithTimeoutSucceedWhenExpectedSizeIsEventuallySatisfied () throws InterruptedException
    {
        final Thread thread = spawnThread( offerTuples( Thread.currentThread(),
                                                        queue,
                                                        asList( newTuple( 1 ), newTuple( 2 ), newTuple( 3 ) ) ) );
        assertTrue( queue.awaitMinimumSize( 3, TIMEOUT_IN_MILLIS, MILLISECONDS ) );

        thread.join();
    }

    @Test
    public void shouldPollExactNumberOfTuples () throws InterruptedException
    {
        queue.offerTuples( asList( newTuple( 1 ), newTuple( 2 ), newTuple( 3 ) ) );

        final List<Tuple> tuples = queue.pollTuples( 2 );
        assertEquals( 2, tuples.size() );
    }

    @Test
    public void shouldPollAllTuples () throws InterruptedException
    {
        queue.offerTuples( asList( newTuple( 1 ), newTuple( 2 ), newTuple( 3 ) ) );
        final List<Tuple> tuples = queue.pollTuplesAtLeast( 2 );
        assertEquals( 3, tuples.size() );
    }

    @Test
    public void shouldDrainAllTuples () throws InterruptedException
    {
        queue.offerTuples( asList( newTuple( 1 ), newTuple( 2 ), newTuple( 3 ) ) );

        final List<Tuple> tuples = new ArrayList<>();
        queue.pollTuplesAtLeast( 2, tuples );
        assertEquals( 3, tuples.size() );
    }

    @Test
    public void shouldPollTuplesWithLimit () throws InterruptedException
    {
        queue.offerTuples( asList( newTuple( 1 ), newTuple( 2 ), newTuple( 3 ) ) );
        final List<Tuple> tuples = queue.pollTuplesAtLeast( 1, 2 );
        assertEquals( 2, tuples.size() );
    }

    @Test
    public void shouldDrainTuplesWithLimit () throws InterruptedException
    {
        queue.offerTuples( asList( newTuple( 1 ), newTuple( 2 ), newTuple( 3 ) ) );
        final List<Tuple> tuples = new ArrayList<>();
        queue.pollTuplesAtLeast( 1, 2, tuples );
        assertEquals( 2, tuples.size() );
    }

    private Tuple newTuple ( final int val )
    {
        final Tuple tuple = new Tuple();
        tuple.set( "k", val );
        return tuple;
    }

    private void assertQueueContent ( final int size )
    {
        final List<Tuple> expected = new ArrayList<>( size );
        for ( int i = 1; i <= size; i++ )
        {
            final Tuple tuple = new Tuple();
            tuple.set( "k", i );
            expected.add( tuple );
        }

        assertEquals( size, queue.size() );
        assertEquals( expected, queue.pollTuples( size ) );
    }

    public static Runnable offerTuples ( final Thread testThread, final TupleQueue queue, final List<Tuple> tuples )
    {
        return () ->
        {
            while ( !( testThread.getState() == WAITING || testThread.getState() == TIMED_WAITING ) )
            {
                sleepUninterruptibly( 1, MILLISECONDS );
            }

            queue.offerTuples( tuples );
        };
    }

}
