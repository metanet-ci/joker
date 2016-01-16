package cs.bilkent.zanza.engine.tuplequeue.impl.consumer;

import org.junit.Test;

import cs.bilkent.zanza.engine.tuplequeue.TupleQueue;
import cs.bilkent.zanza.engine.tuplequeue.impl.queue.SingleThreadedTupleQueue;
import static cs.bilkent.zanza.flow.Port.DEFAULT_PORT_INDEX;
import cs.bilkent.zanza.operator.PortsToTuples;
import cs.bilkent.zanza.operator.Tuple;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;


public class DrainAllAvailableTuplesTest
{

    private final DrainAllAvailableTuples drainAllAvailableTuples = new DrainAllAvailableTuples();

    @Test( expected = IllegalArgumentException.class )
    public void shouldFailWithNullTupleQueues ()
    {
        drainAllAvailableTuples.accept( null );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldFailWithEmptyTupleQueues ()
    {
        drainAllAvailableTuples.accept( new TupleQueue[] {} );
    }

    @Test
    public void shouldDrainAllTuplesFromSinglePort ()
    {
        final TupleQueue tupleQueue = new SingleThreadedTupleQueue( 1 );
        final Tuple tuple = new Tuple();
        tupleQueue.offerTuple( tuple );

        drainAllAvailableTuples.accept( new TupleQueue[] { tupleQueue } );

        final PortsToTuples portsToTuples = drainAllAvailableTuples.getPortsToTuples();
        assertNotNull( portsToTuples );
        assertThat( portsToTuples.getPortCount(), equalTo( 1 ) );
        assertThat( portsToTuples.getTupleCount( DEFAULT_PORT_INDEX ), equalTo( 1 ) );
        assertTrue( tuple == portsToTuples.getTuple( DEFAULT_PORT_INDEX, 0 ) );
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

        drainAllAvailableTuples.accept( new TupleQueue[] { tupleQueue1, tupleQueue2 } );

        final PortsToTuples portsToTuples = drainAllAvailableTuples.getPortsToTuples();
        assertNotNull( portsToTuples );
        assertThat( portsToTuples.getPortCount(), equalTo( 2 ) );
        assertThat( portsToTuples.getTupleCount( DEFAULT_PORT_INDEX ), equalTo( 1 ) );
        assertThat( portsToTuples.getTupleCount( DEFAULT_PORT_INDEX + 1 ), equalTo( 1 ) );
        assertTrue( tuple1 == portsToTuples.getTuple( DEFAULT_PORT_INDEX, 0 ) );
        assertTrue( tuple2 == portsToTuples.getTuple( 1, 0 ) );
        assertThat( tupleQueue1.size(), equalTo( 0 ) );
        assertThat( tupleQueue2.size(), equalTo( 0 ) );
    }

}
