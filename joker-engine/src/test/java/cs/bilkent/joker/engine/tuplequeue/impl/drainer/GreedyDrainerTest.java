package cs.bilkent.joker.engine.tuplequeue.impl.drainer;

import org.junit.Test;

import cs.bilkent.joker.engine.tuplequeue.TupleQueue;
import cs.bilkent.joker.engine.tuplequeue.TupleQueueDrainer;
import static cs.bilkent.joker.engine.tuplequeue.impl.drainer.NonBlockingMultiPortDisjunctiveDrainer.newGreedyDrainer;
import cs.bilkent.joker.engine.tuplequeue.impl.queue.SingleThreadedTupleQueue;
import cs.bilkent.joker.operator.Tuple;
import cs.bilkent.joker.operator.impl.TuplesImpl;
import cs.bilkent.joker.test.AbstractJokerTest;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;


public class GreedyDrainerTest extends AbstractJokerTest
{

    @Test( expected = IllegalArgumentException.class )
    public void shouldFailWithNullTupleQueues ()
    {
        final TupleQueueDrainer drainer = newGreedyDrainer( "op1", 1, Integer.MAX_VALUE );
        drainer.drain( null, null, k -> null );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldFailWithEmptyTupleQueues ()
    {
        final TupleQueueDrainer drainer = newGreedyDrainer( "op1", 1, Integer.MAX_VALUE );
        drainer.drain( null, new TupleQueue[] {}, k -> null );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldFailWithNullTupleSupplier ()
    {
        final TupleQueueDrainer drainer = newGreedyDrainer( "op1", 1, Integer.MAX_VALUE );
        drainer.drain( null, new TupleQueue[] { new SingleThreadedTupleQueue( 1 ) }, null );
    }

    @Test
    public void shouldDrainAllTuplesFromSinglePort ()
    {
        final TupleQueue tupleQueue = new SingleThreadedTupleQueue( 1 );
        final Tuple tuple = new Tuple();
        tupleQueue.offer( tuple );

        final TupleQueueDrainer drainer = newGreedyDrainer( "op1", 1, Integer.MAX_VALUE );
        final TuplesImpl tuples = new TuplesImpl( 1 );

        final boolean success = drainer.drain( null, new TupleQueue[] { tupleQueue }, key -> tuples );

        assertTrue( success );
        assertThat( tuples.getNonEmptyPortCount(), equalTo( 1 ) );
        assertThat( tuples.getTupleCount( 0 ), equalTo( 1 ) );
        assertSame( tuple, tuples.getTupleOrFail( 0, 0 ) );
        assertThat( tupleQueue.size(), equalTo( 0 ) );
    }

    @Test
    public void shouldDrainAllTuplesFromMultiplePorts ()
    {
        final TupleQueue tupleQueue1 = new SingleThreadedTupleQueue( 1 );
        final Tuple tuple1 = new Tuple();
        tupleQueue1.offer( tuple1 );
        final TupleQueue tupleQueue2 = new SingleThreadedTupleQueue( 1 );
        final Tuple tuple2 = new Tuple();
        tupleQueue2.offer( tuple2 );

        final TupleQueueDrainer drainer = newGreedyDrainer( "op1", 2, Integer.MAX_VALUE );
        final TuplesImpl tuples = new TuplesImpl( 2 );

        final boolean success = drainer.drain( null, new TupleQueue[] { tupleQueue1, tupleQueue2 }, key -> tuples );

        assertTrue( success );
        assertThat( tuples.getNonEmptyPortCount(), equalTo( 2 ) );
        assertThat( tuples.getTupleCount( 0 ), equalTo( 1 ) );
        assertThat( tuples.getTupleCount( 1 ), equalTo( 1 ) );
        assertSame( tuple1, tuples.getTupleOrFail( 0, 0 ) );
        assertSame( tuple2, tuples.getTupleOrFail( 1, 0 ) );
        assertThat( tupleQueue1.size(), equalTo( 0 ) );
        assertThat( tupleQueue2.size(), equalTo( 0 ) );
    }

}
