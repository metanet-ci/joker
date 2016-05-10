package cs.bilkent.zanza.engine.tuplequeue.impl.drainer;

import org.junit.Test;

import cs.bilkent.zanza.engine.tuplequeue.TupleQueue;
import cs.bilkent.zanza.engine.tuplequeue.impl.queue.SingleThreadedTupleQueue;
import cs.bilkent.zanza.operator.PortsToTuples;
import cs.bilkent.zanza.operator.Tuple;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;


public class GreedyDrainerTest
{

    private final GreedyDrainer greedyDrainer = new GreedyDrainer();

    @Test( expected = IllegalArgumentException.class )
    public void shouldFailWithNullTupleQueues ()
    {
        greedyDrainer.drain( null, null );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldFailWithEmptyTupleQueues ()
    {
        greedyDrainer.drain( null, new TupleQueue[] {} );
    }

    @Test
    public void shouldDrainAllTuplesFromSinglePort ()
    {
        final TupleQueue tupleQueue = new SingleThreadedTupleQueue( 1 );
        final Tuple tuple = new Tuple();
        tupleQueue.offerTuple( tuple );

        greedyDrainer.drain( null, new TupleQueue[] { tupleQueue } );

        final PortsToTuples portsToTuples = greedyDrainer.getResult();
        assertNotNull( portsToTuples );
        assertThat( portsToTuples.getPortCount(), equalTo( 1 ) );
        assertThat( portsToTuples.getTupleCount( 0 ), equalTo( 1 ) );
        assertTrue( tuple == portsToTuples.getTuple( 0, 0 ) );
        assertThat( tupleQueue.size(), equalTo( 0 ) );
    }

    @Test
    public void shouldDrainAllTuplesFromMultiplePorts ()
    {
        final TupleQueue tupleQueue1 = new SingleThreadedTupleQueue( 1 );
        final Tuple tuple1 = new Tuple();
        tupleQueue1.offerTuple( tuple1 );
        final TupleQueue tupleQueue2 = new SingleThreadedTupleQueue( 1 );
        final Tuple tuple2 = new Tuple();
        tupleQueue2.offerTuple( tuple2 );

        greedyDrainer.drain( null, new TupleQueue[] { tupleQueue1, tupleQueue2 } );

        final PortsToTuples portsToTuples = greedyDrainer.getResult();
        assertNotNull( portsToTuples );
        assertThat( portsToTuples.getPortCount(), equalTo( 2 ) );
        assertThat( portsToTuples.getTupleCount( 0 ), equalTo( 1 ) );
        assertThat( portsToTuples.getTupleCount( 1 ), equalTo( 1 ) );
        assertTrue( tuple1 == portsToTuples.getTuple( 0, 0 ) );
        assertTrue( tuple2 == portsToTuples.getTuple( 1, 0 ) );
        assertThat( tupleQueue1.size(), equalTo( 0 ) );
        assertThat( tupleQueue2.size(), equalTo( 0 ) );
    }

}
