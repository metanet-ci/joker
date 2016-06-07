package cs.bilkent.zanza.engine.tuplequeue.impl.context;

import java.util.List;
import java.util.function.Function;

import org.junit.Test;

import cs.bilkent.zanza.engine.config.ThreadingPreference;
import static cs.bilkent.zanza.engine.config.ThreadingPreference.MULTI_THREADED;
import static cs.bilkent.zanza.engine.config.ThreadingPreference.SINGLE_THREADED;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueue;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueueContext;
import cs.bilkent.zanza.engine.tuplequeue.impl.drainer.GreedyDrainer;
import cs.bilkent.zanza.engine.tuplequeue.impl.queue.MultiThreadedTupleQueue;
import cs.bilkent.zanza.engine.tuplequeue.impl.queue.SingleThreadedTupleQueue;
import cs.bilkent.zanza.operator.Tuple;
import cs.bilkent.zanza.operator.impl.TuplesImpl;
import static junit.framework.TestCase.assertNotNull;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;

public class DefaultTupleQueueContextTest
{

    private static final int TIMEOUT_IN_MILLIS = 2000;

    private static final int TUPLE_QUEUE_SIZE = 2;

    @Test
    public void testAddSinglePortTuplesToUnboundedQueue ()
    {
        testAddTuples( getSingleThreadedTupleQueueConstructor(), 1, SINGLE_THREADED, 2 );
    }

    @Test
    public void testAddSinglePortTuplesToBoundedQueue ()
    {
        testAddTuples( getMultiThreadedTupleQueueConstructor( 2 ), 1, MULTI_THREADED, 2 );
    }

    @Test
    public void testAddMultiPortTuplesToUnboundedQueue ()
    {
        testAddTuples( getSingleThreadedTupleQueueConstructor(), 2, SINGLE_THREADED, 2 );
    }

    @Test
    public void testAddMultiPortTuplesToBoundedQueue ()
    {
        testAddTuples( getMultiThreadedTupleQueueConstructor( 2 ), 2, MULTI_THREADED, 2 );
    }

    @Test
    public void testTryAddSinglePortTuplesToUnboundedQueue ()
    {
        testTryAddTuples( getSingleThreadedTupleQueueConstructor(), 1, SINGLE_THREADED, 2 );
    }

    @Test
    public void testTryAddSinglePortTuplesToBoundedQueue ()
    {
        testTryAddTuples( getMultiThreadedTupleQueueConstructor( 2 ), 1, MULTI_THREADED, 2 );
    }

    @Test
    public void testTryAddMultiPortTuplesToUnboundedQueue ()
    {
        testTryAddTuples( getSingleThreadedTupleQueueConstructor(), 2, SINGLE_THREADED, 2 );
    }

    @Test
    public void testTryAddMultiPortTuplesToBoundedQueue ()
    {
        testTryAddTuples( getMultiThreadedTupleQueueConstructor( 2 ), 2, MULTI_THREADED, 2 );
    }


    private void testAddTuples ( final Function<Boolean, TupleQueue> tupleQueueConstructor,
                                 final int inputPortCount,
                                 final ThreadingPreference threadingPreference,
                                 final int tupleCount )
    {
        final TupleQueueContext context = new DefaultTupleQueueContext( "op1", inputPortCount, threadingPreference, tupleQueueConstructor );

        final TuplesImpl input = addTuples( inputPortCount, tupleCount );

        for ( int portIndex = 0; portIndex < inputPortCount; portIndex++ )
        {
            context.offer( portIndex, input.getTuples( portIndex ) );
        }

        final GreedyDrainer drainer = new GreedyDrainer( inputPortCount );
        context.drain( drainer );

        assertTuples( inputPortCount, tupleCount, drainer );
    }

    private void testTryAddTuples ( final Function<Boolean, TupleQueue> tupleQueueConstructor,
                                    final int inputPortCount,
                                    final ThreadingPreference threadingPreference,
                                    final int tupleCount )
    {
        final TupleQueueContext context = new DefaultTupleQueueContext( "op1", inputPortCount, threadingPreference, tupleQueueConstructor );

        final TuplesImpl input = addTuples( inputPortCount, tupleCount );

        for ( int portIndex = 0; portIndex < inputPortCount; portIndex++ )
        {
            final List<Tuple> tuples = input.getTuples( portIndex );
            final int count = context.tryOffer( portIndex, tuples, TIMEOUT_IN_MILLIS );
            assertEquals( count, tuples.size() );
        }

        final GreedyDrainer drainer = new GreedyDrainer( inputPortCount );
        context.drain( drainer );

        assertTuples( inputPortCount, tupleCount, drainer );
    }

    private TuplesImpl addTuples ( final int inputPortCount, final int tupleCount )
    {
        final TuplesImpl input = new TuplesImpl( inputPortCount );
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
        final TuplesImpl output = drainer.getResult();
        assertThat( output.getNonEmptyPortCount(), equalTo( inputPortCount ) );

        for ( int portIndex = 0; portIndex < inputPortCount; portIndex++ )
        {
            List<Tuple> tuples = output.getTuplesModifiable( portIndex );
            assertThat( tuples.size(), equalTo( tupleCount ) );

            for ( int tupleIndex = 1; tupleIndex <= tupleCount; tupleIndex++ )
            {
                final Tuple tuple = tuples.get( tupleIndex - 1 );
                assertNotNull( tuple );

                final String key = portIndex + "-" + tupleIndex;
                assertThat( tuple, equalTo( new Tuple( key, key ) ) );
            }
        }
    }

    private Function<Boolean, TupleQueue> getSingleThreadedTupleQueueConstructor ()
    {
        return ( capacityCheckEnabled ) -> new SingleThreadedTupleQueue( TUPLE_QUEUE_SIZE );
    }

    private Function<Boolean, TupleQueue> getMultiThreadedTupleQueueConstructor ( final int queueSize )
    {
        return ( capacityCheckEnabled ) -> new MultiThreadedTupleQueue( queueSize, capacityCheckEnabled );
    }

}