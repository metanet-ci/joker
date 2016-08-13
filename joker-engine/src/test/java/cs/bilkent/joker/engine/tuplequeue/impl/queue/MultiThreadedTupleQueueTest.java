package cs.bilkent.joker.engine.tuplequeue.impl.queue;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import cs.bilkent.joker.engine.tuplequeue.TupleQueue;
import cs.bilkent.joker.operator.Tuple;
import cs.bilkent.joker.testutils.AbstractJokerTest;
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
        queue.offerTuple( newTuple( 1 ) );

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
    public void shouldOfferTupleExceedinglyWhenCapacityIncreases () throws InterruptedException
    {
        queue.offerTuple( newTuple( 1 ) );
        queue.offerTuple( newTuple( 2 ) );
        queue.offerTuple( newTuple( 3 ) );

        final Thread thread = spawnThread( increaseCapacity( Thread.currentThread(), 4 ) );

        queue.offerTuple( newTuple( 4 ) );

        assertQueueContent( 4 );

        thread.join();
    }

    @Test
    public void shouldOfferTupleExceedinglyWithTimeoutWhenCapacityIncreases () throws InterruptedException
    {
        queue.offerTuple( newTuple( 1 ) );
        queue.offerTuple( newTuple( 2 ) );
        queue.offerTuple( newTuple( 3 ) );

        final Thread thread = spawnThread( increaseCapacity( Thread.currentThread(), 4 ) );

        final boolean result = queue.tryOfferTuple( newTuple( 4 ), Integer.MAX_VALUE, MILLISECONDS );

        assertTrue( result );
        assertQueueContent( 4 );

        thread.join();
    }

    @Test
    public void shouldNotOfferTupleExceedinglyWithTimeout ()
    {
        queue.offerTuple( newTuple( 1 ) );
        queue.offerTuple( newTuple( 2 ) );
        queue.offerTuple( newTuple( 3 ) );

        final boolean result = queue.tryOfferTuple( newTuple( 4 ), 500, MILLISECONDS );

        assertFalse( result );
        assertQueueContent( 3 );
    }

    @Test
    public void shouldTryOfferTuples ()
    {
        final int offered = queue.tryOfferTuples( asList( newTuple( 1 ), newTuple( 2 ) ), TIMEOUT_IN_MILLIS, MILLISECONDS );

        assertEquals( 2, offered );
        assertQueueContent( 2 );
    }

    @Test
    public void shouldOfferTuplesPartiallyWithTimeoutWhenCapacityIncreases () throws InterruptedException
    {
        queue.offerTuple( newTuple( 1 ) );
        queue.offerTuple( newTuple( 2 ) );
        final Thread thread = spawnThread( increaseCapacity( Thread.currentThread(), 4 ) );

        final int offeredCount = queue.tryOfferTuples( asList( newTuple( 3 ), newTuple( 4 ), newTuple( 5 ) ),
                                                       TIMEOUT_IN_MILLIS,
                                                       MILLISECONDS );

        assertEquals( 2, offeredCount );
        assertQueueContent( 4 );

        thread.join();
    }

    @Test
    public void shouldOfferTuplesCompletelyWithTimeoutWhenCapacityIncreases () throws InterruptedException
    {
        queue.offerTuple( newTuple( 1 ) );
        queue.offerTuple( newTuple( 2 ) );
        final Thread thread = spawnThread( increaseCapacity( Thread.currentThread(), 5 ) );

        final int offeredCount = queue.tryOfferTuples( asList( newTuple( 3 ), newTuple( 4 ), newTuple( 5 ) ),
                                                       TIMEOUT_IN_MILLIS,
                                                       MILLISECONDS );

        assertEquals( 3, offeredCount );
        assertQueueContent( 5 );

        thread.join();
    }

    @Test
    public void shouldOfferTuplesWhenCapacityAvailable ()
    {
        queue.offerTuples( asList( newTuple( 1 ), newTuple( 2 ) ) );

        assertQueueContent( 2 );
    }

    @Test
    public void shouldOfferTuplesExceedinglyWhenCapacityIncreases () throws InterruptedException
    {
        final Thread testThread = Thread.currentThread();
        final Thread thread = spawnThread( increaseCapacity( testThread, 4 ) );

        queue.offerTuples( asList( newTuple( 1 ), newTuple( 2 ), newTuple( 3 ), newTuple( 4 ) ) );

        assertQueueContent( 4 );

        thread.join();
    }

    @Test
    public void shouldAwaitSizeSucceedWhenExpectedSizeIsAlreadyAvailable ()
    {
        queue.offerTuples( asList( newTuple( 1 ), newTuple( 2 ), newTuple( 3 ) ) );
        assertTrue( queue.awaitMinimumSize( 3 ) );
    }

    @Test
    public void shouldAwaitSizeWithTimeoutSucceedWhenExpectedSizeIsAlreadyAvailable ()
    {
        queue.offerTuples( asList( newTuple( 1 ), newTuple( 2 ), newTuple( 3 ) ) );
        assertTrue( queue.awaitMinimumSize( 3, TIMEOUT_IN_MILLIS, MILLISECONDS ) );
    }

    @Test
    public void shouldAwaitSizeWithTimeoutSucceedWhenExpectedSizeIsSatisfiedTuplesAreOfferedAfterwards () throws InterruptedException
    {
        final Thread thread = spawnThread( offerTuples( Thread.currentThread(),
                                                        queue,
                                                        asList( newTuple( 1 ), newTuple( 2 ), newTuple( 3 ) ) ) );
        assertTrue( queue.awaitMinimumSize( 3, TIMEOUT_IN_MILLIS, MILLISECONDS ) );

        thread.join();
    }

    @Test
    public void shouldOfferExceedingTuplesWhenCapacityCheckDisabled () throws InterruptedException
    {
        final Thread thread = spawnThread( offerTuples( Thread.currentThread(),
                                                        queue,
                                                        asList( newTuple( 1 ), newTuple( 2 ), newTuple( 3 ), newTuple( 4 ) ) ) );
        queue.disableCapacityCheck();

        assertTrueEventually( () -> assertQueueContent( 4 ) );

        thread.join();
    }

    @Test
    public void shouldNotOfferExceedingTuplesAfterQueueCapacityCheckEnabledAgain ()
    {
        queue.disableCapacityCheck();
        queue.offerTuples( asList( newTuple( 1 ), newTuple( 2 ), newTuple( 3 ), newTuple( 4 ) ) );
        queue.enableCapacityCheck();
        assertFalse( queue.tryOfferTuple( newTuple( 5 ), 1000, MILLISECONDS ) );
    }

    @Test
    public void shouldPollExactNumberOfTuples () throws InterruptedException
    {
        queue.disableCapacityCheck();
        final Thread thread = spawnThread( offerTuples( Thread.currentThread(),
                                                        queue,
                                                        asList( newTuple( 1 ), newTuple( 2 ), newTuple( 3 ), newTuple( 4 ) ) ) );
        final List<Tuple> tuples = queue.pollTuples( 2 );
        assertEquals( 2, tuples.size() );

        thread.join();
    }

    @Test
    public void shouldPollAllTuples () throws InterruptedException
    {
        queue.disableCapacityCheck();
        final Thread thread = spawnThread( offerTuples( Thread.currentThread(),
                                                        queue,
                                                        asList( newTuple( 1 ), newTuple( 2 ), newTuple( 3 ), newTuple( 4 ) ) ) );
        final List<Tuple> tuples = queue.pollTuplesAtLeast( 2 );
        assertEquals( 4, tuples.size() );

        thread.join();
    }

    @Test
    public void shouldPollAllTuples2 () throws InterruptedException
    {
        queue.disableCapacityCheck();
        final Thread thread = spawnThread( offerTuples( Thread.currentThread(),
                                                        queue,
                                                        asList( newTuple( 1 ), newTuple( 2 ), newTuple( 3 ), newTuple( 4 ) ) ) );
        final List<Tuple> tuples = new ArrayList<>();
        queue.pollTuplesAtLeast( 2, tuples );
        assertEquals( 4, tuples.size() );

        thread.join();
    }

    @Test
    public void shouldPollTuplesWithLimit () throws InterruptedException
    {
        queue.disableCapacityCheck();
        final Thread thread = spawnThread( offerTuples( Thread.currentThread(),
                                                        queue,
                                                        asList( newTuple( 1 ), newTuple( 2 ), newTuple( 3 ), newTuple( 4 ) ) ) );
        final List<Tuple> tuples = queue.pollTuplesAtLeast( 2, 3 );
        assertEquals( 3, tuples.size() );

        thread.join();
    }

    @Test
    public void shouldPollTuplesWithLimit2 () throws InterruptedException
    {
        queue.disableCapacityCheck();
        final Thread thread = spawnThread( offerTuples( Thread.currentThread(),
                                                        queue,
                                                        asList( newTuple( 1 ), newTuple( 2 ), newTuple( 3 ), newTuple( 4 ) ) ) );
        final List<Tuple> tuples = new ArrayList<>();
        queue.pollTuplesAtLeast( 2, 3, tuples );
        assertEquals( 3, tuples.size() );

        thread.join();
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

    private Runnable increaseCapacity ( final Thread testThread, final int newCapacity )
    {
        return () ->
        {
            while ( !( testThread.getState() == WAITING || testThread.getState() == TIMED_WAITING ) )
            {
                sleepUninterruptibly( 1, MILLISECONDS );
            }

            queue.ensureCapacity( newCapacity );
        };
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
