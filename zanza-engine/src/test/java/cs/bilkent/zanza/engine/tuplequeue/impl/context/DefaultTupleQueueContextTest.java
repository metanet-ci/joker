package cs.bilkent.zanza.engine.tuplequeue.impl.context;

import java.util.List;
import java.util.function.Supplier;

import org.junit.Test;

import cs.bilkent.zanza.engine.tuplequeue.TupleQueue;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueueContext;
import cs.bilkent.zanza.engine.tuplequeue.impl.drainer.GreedyDrainer;
import cs.bilkent.zanza.engine.tuplequeue.impl.queue.BoundedTupleQueue;
import cs.bilkent.zanza.engine.tuplequeue.impl.queue.UnboundedTupleQueue;
import cs.bilkent.zanza.operator.PortsToTuples;
import cs.bilkent.zanza.operator.PortsToTuples.PortToTuples;
import cs.bilkent.zanza.operator.Tuple;
import cs.bilkent.zanza.scheduling.ScheduleWhenTuplesAvailable.PortToTupleCount;
import static junit.framework.TestCase.assertNotNull;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertTrue;

public class DefaultTupleQueueContextTest
{

    private static final int TIMEOUT_IN_MILLIS = 2000;

    private static final int UNBOUNDED_TUPLE_QUEUE_SIZE = 2;

    @Test
    public void testAddSinglePortTuplesToUnboundedQueue ()
    {
        testAddTuples( getUnboundedTupleQueueSupplier(), 1, 2 );
    }

    @Test
    public void testAddSinglePortTuplesToBoundedQueue ()
    {
        testAddTuples( getBoundedTupleQueueSupplier( 2 ), 1, 2 );
    }

    @Test
    public void testAddMultiPortTuplesToUnboundedQueue ()
    {
        testAddTuples( getUnboundedTupleQueueSupplier(), 2, 2 );
    }

    @Test
    public void testAddMultiPortTuplesToBoundedQueue ()
    {
        testAddTuples( getBoundedTupleQueueSupplier( 2 ), 2, 2 );
    }

    @Test
    public void testTryAddSinglePortTuplesToUnboundedQueue ()
    {
        testTryAddTuples( getUnboundedTupleQueueSupplier(), 1, 2 );
    }

    @Test
    public void testTryAddSinglePortTuplesToBoundedQueue ()
    {
        testTryAddTuples( getBoundedTupleQueueSupplier( 2 ), 1, 2 );
    }

    @Test
    public void testTryAddMultiPortTuplesToUnboundedQueue ()
    {
        testTryAddTuples( getUnboundedTupleQueueSupplier(), 2, 2 );
    }

    @Test
    public void testTryAddMultiPortTuplesToBoundedQueue ()
    {
        testTryAddTuples( getBoundedTupleQueueSupplier( 2 ), 2, 2 );
    }

    @Test
    public void testPartialTryAdd ()
    {
        final TupleQueueContext context = new DefaultTupleQueueContext( "op1", 2, getBoundedTupleQueueSupplier( 2 ) );
        final PortsToTuples input = addTuples( 2, 2 );
        input.add( 1, new Tuple() );
        input.add( 1, new Tuple() );

        final List<PortToTupleCount> counts = context.tryAdd( input, TIMEOUT_IN_MILLIS );

        assertThat( counts.size(), equalTo( 1 ) );
        final PortToTupleCount portToTupleCount = counts.get( 0 );
        assertThat( portToTupleCount.portIndex, equalTo( 1 ) );
        assertThat( portToTupleCount.tupleCount, equalTo( 2 ) );

    }

    private void testAddTuples ( final Supplier<TupleQueue> tupleQueueSupplier, final int inputPortCount, final int tupleCount )
    {
        final TupleQueueContext context = new DefaultTupleQueueContext( "op1", inputPortCount, tupleQueueSupplier );

        final PortsToTuples input = addTuples( inputPortCount, tupleCount );

        context.add( input );

        final GreedyDrainer drainer = new GreedyDrainer();
        context.drain( drainer );

        assertTuples( inputPortCount, tupleCount, drainer );
    }

    private void testTryAddTuples ( final Supplier<TupleQueue> tupleQueueSupplier, final int inputPortCount, final int tupleCount )
    {
        final TupleQueueContext context = new DefaultTupleQueueContext( "op1", inputPortCount, tupleQueueSupplier );

        final PortsToTuples input = addTuples( inputPortCount, tupleCount );

        final List<PortToTupleCount> counts = context.tryAdd( input, TIMEOUT_IN_MILLIS );

        assertTrue( counts.isEmpty() );

        final GreedyDrainer drainer = new GreedyDrainer();
        context.drain( drainer );

        assertTuples( inputPortCount, tupleCount, drainer );
    }

    private PortsToTuples addTuples ( final int inputPortCount, final int tupleCount )
    {
        final PortsToTuples input = new PortsToTuples();
        for ( int portIndex = 0; portIndex < inputPortCount; portIndex++ )
        {
            for ( int tupleIndex = 1; tupleIndex <= tupleCount; tupleIndex++ )
            {
                final String key = portIndex + "-" + tupleIndex;
                input.add( portIndex, new Tuple( key, key ) );
            }
        }
        return input;
    }

    private void assertTuples ( final int inputPortCount, final int tupleCount, final GreedyDrainer drainer )
    {
        final PortsToTuples output = drainer.getResult();
        assertThat( output.getPortCount(), equalTo( inputPortCount ) );
        for ( PortToTuples eachPort : output.getPortToTuplesList() )
        {
            final List<Tuple> tuples = eachPort.getTuples();
            assertThat( tuples.size(), equalTo( tupleCount ) );
            for ( int tupleIndex = 1; tupleIndex <= tupleCount; tupleIndex++ )
            {
                final Tuple tuple = tuples.get( tupleIndex - 1 );
                assertNotNull( tuple );

                final String key = eachPort.getPortIndex() + "-" + tupleIndex;
                assertThat( tuple, equalTo( new Tuple( key, key ) ) );
            }
        }
    }

    private Supplier<TupleQueue> getUnboundedTupleQueueSupplier ()
    {
        return () -> new UnboundedTupleQueue( UNBOUNDED_TUPLE_QUEUE_SIZE );
    }

    private Supplier<TupleQueue> getBoundedTupleQueueSupplier ( final int queueSize )
    {
        return () -> new BoundedTupleQueue( queueSize );
    }

}
