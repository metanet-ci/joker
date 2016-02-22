package cs.bilkent.zanza.engine.tuplequeue.impl.queue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static cs.bilkent.zanza.engine.TestUtils.spawnThread;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueue;
import cs.bilkent.zanza.operator.Tuple;
import static java.lang.Thread.State.TIMED_WAITING;
import static java.lang.Thread.State.WAITING;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class BoundedTupleQueueTest
{

    public static final int TIMEOUT_IN_MILLIS = 5000;

    private final TupleQueue queue = new BoundedTupleQueue( 3 );

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
    public void shouldOfferTupleExceedinglyWhenCapacityIncreases ()
    {
        queue.offerTuple( newTuple( 1 ) );
        queue.offerTuple( newTuple( 2 ) );
        queue.offerTuple( newTuple( 3 ) );

        spawnThread( increaseCapacity( Thread.currentThread(), 4 ) );

        queue.offerTuple( newTuple( 4 ) );

        assertQueueContent( 4 );
    }

    @Test
    public void shouldOfferTupleExceedinglyWithTimeoutWhenCapacityIncreases ()
    {
        queue.offerTuple( newTuple( 1 ) );
        queue.offerTuple( newTuple( 2 ) );
        queue.offerTuple( newTuple( 3 ) );

        spawnThread( increaseCapacity( Thread.currentThread(), 4 ) );

        final boolean result = queue.tryOfferTuple( newTuple( 4 ), Integer.MAX_VALUE );

        assertTrue( result );
        assertQueueContent( 4 );
    }

    @Test
    public void shouldNotOfferTupleExceedinglyWithTimeout ()
    {
        queue.offerTuple( newTuple( 1 ) );
        queue.offerTuple( newTuple( 2 ) );
        queue.offerTuple( newTuple( 3 ) );

        final boolean result = queue.tryOfferTuple( newTuple( 4 ), 500 );

        assertFalse( result );
        assertQueueContent( 3 );
    }

    @Test
    public void shouldTryOfferTuples ()
    {
        final int offered = queue.tryOfferTuples( asList( newTuple( 1 ), newTuple( 2 ) ), TIMEOUT_IN_MILLIS );

        assertEquals( 2, offered );
        assertQueueContent( 2 );
    }

    @Test
    public void shouldOfferTuplesPartiallyWithTimeoutWhenCapacityIncreases ()
    {
        queue.offerTuple( newTuple( 1 ) );
        queue.offerTuple( newTuple( 2 ) );
        spawnThread( increaseCapacity( Thread.currentThread(), 4 ) );

        final int offeredCount = queue.tryOfferTuples( asList( newTuple( 3 ), newTuple( 4 ), newTuple( 5 ) ), TIMEOUT_IN_MILLIS );

        assertEquals( 2, offeredCount );
        assertQueueContent( 4 );
    }

    @Test
    public void shouldOfferTuplesCompletelyWithTimeoutWhenCapacityIncreases ()
    {
        queue.offerTuple( newTuple( 1 ) );
        queue.offerTuple( newTuple( 2 ) );
        spawnThread( increaseCapacity( Thread.currentThread(), 5 ) );

        final int offeredCount = queue.tryOfferTuples( asList( newTuple( 3 ), newTuple( 4 ), newTuple( 5 ) ), TIMEOUT_IN_MILLIS );

        assertEquals( 3, offeredCount );
        assertQueueContent( 5 );
    }

    @Test
    public void shouldOfferTuplesWhenCapacityAvailable ()
    {
        queue.offerTuples( asList( newTuple( 1 ), newTuple( 2 ) ) );

        assertQueueContent( 2 );
    }

    @Test
    public void shouldOfferTuplesExceedinglyWhenCapacityIncreases ()
    {
        final Thread testThread = Thread.currentThread();
        spawnThread( increaseCapacity( testThread, 4 ) );

        queue.offerTuples( asList( newTuple( 1 ), newTuple( 2 ), newTuple( 3 ), newTuple( 4 ) ) );

        assertQueueContent( 4 );
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
        assertTrue( queue.awaitMinimumSize( 3, TIMEOUT_IN_MILLIS ) );
    }

    @Test
    public void shouldAwaitSizeWithTimeoutSucceedWhenExpectedSizeIsSatisfiedTuplesAreOfferedAfterwards ()
    {
        spawnThread( offerTuples( Thread.currentThread(), queue, asList( newTuple( 1 ), newTuple( 2 ), newTuple( 3 ) ) ) );
        assertTrue( queue.awaitMinimumSize( 3, TIMEOUT_IN_MILLIS ) );
    }

    private Tuple newTuple ( final int val )
    {
        return new Tuple( "k", val );
    }

    private void assertQueueContent ( final int size )
    {
        final List<Tuple> expected = new ArrayList<>( size );
        for ( int i = 1; i <= size; i++ )
        {
            expected.add( new Tuple( "k", i ) );
        }

        assertEquals( size, queue.size() );
        assertEquals( expected, queue.pollTuples( size ) );
    }

    private Runnable increaseCapacity ( final Thread testThread, final int newCapacity )
    {
        return () -> {
            while ( !( testThread.getState() == WAITING || testThread.getState() == TIMED_WAITING ) )
            {
                sleepUninterruptibly( 1, TimeUnit.MILLISECONDS );
            }

            queue.ensureCapacity( newCapacity );
        };
    }

    public static Runnable offerTuples ( final Thread testThread, final TupleQueue queue, final List<Tuple> tuples )
    {
        return () -> {
            while ( !( testThread.getState() == WAITING || testThread.getState() == TIMED_WAITING ) )
            {
                sleepUninterruptibly( 1, TimeUnit.MILLISECONDS );
            }

            queue.offerTuples( tuples );
        };
    }

}
