package cs.bilkent.joker.engine.tuplequeue.impl.operator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import cs.bilkent.joker.engine.config.ThreadingPreference;
import static cs.bilkent.joker.engine.config.ThreadingPreference.MULTI_THREADED;
import static cs.bilkent.joker.engine.config.ThreadingPreference.SINGLE_THREADED;
import cs.bilkent.joker.engine.tuplequeue.OperatorTupleQueue;
import cs.bilkent.joker.engine.tuplequeue.TupleQueue;
import cs.bilkent.joker.engine.tuplequeue.impl.drainer.GreedyDrainer;
import cs.bilkent.joker.engine.tuplequeue.impl.drainer.MultiPortDrainer;
import cs.bilkent.joker.engine.tuplequeue.impl.drainer.NonBlockingMultiPortDisjunctiveDrainer;
import cs.bilkent.joker.engine.tuplequeue.impl.queue.MultiThreadedTupleQueue;
import cs.bilkent.joker.engine.tuplequeue.impl.queue.SingleThreadedTupleQueue;
import cs.bilkent.joker.operator.Tuple;
import cs.bilkent.joker.operator.impl.TuplesImpl;
import cs.bilkent.joker.operator.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByCount;
import cs.bilkent.joker.partition.impl.PartitionKey;
import cs.bilkent.joker.test.AbstractJokerTest;
import static java.util.Arrays.asList;
import static junit.framework.TestCase.assertNotNull;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

@RunWith( Parameterized.class )
public class DefaultOperatorTupleQueueTest extends AbstractJokerTest
{

    private static final int TUPLE_QUEUE_SIZE = 2;

    @Parameters( name = "threadingPreference={0}" )
    public static Collection<Object[]> data ()
    {
        return asList( new Object[][] { { SINGLE_THREADED }, { MULTI_THREADED } } );
    }

    private final ThreadingPreference threadingPreference;

    public DefaultOperatorTupleQueueTest ( final ThreadingPreference threadingPreference )
    {
        this.threadingPreference = threadingPreference;
    }

    @Test
    public void shouldOfferSinglePortTuples ()
    {
        testOfferTuples( 1, threadingPreference );
    }

    @Test
    public void shouldOfferMultiPortTuples ()
    {
        testOfferTuples( 2, threadingPreference );
    }

    private void testOfferTuples ( final int inputPortCount, final ThreadingPreference threadingPreference )
    {
        final OperatorTupleQueue operatorTupleQueue = createOperatorTupleQueue( inputPortCount, threadingPreference );
        final TuplesImpl input = createTuples( inputPortCount, TUPLE_QUEUE_SIZE );

        for ( int portIndex = 0; portIndex < inputPortCount; portIndex++ )
        {
            final List<Tuple> tuples = input.getTuples( portIndex );
            final int offered = operatorTupleQueue.offer( portIndex, tuples );
            assertThat( offered, equalTo( tuples.size() ) );
        }

        final GreedyDrainer drainer = new GreedyDrainer( inputPortCount );
        final TuplesImpl result = new TuplesImpl( inputPortCount );
        operatorTupleQueue.drain( drainer, key -> result );

        assertTuples( inputPortCount, TUPLE_QUEUE_SIZE, result );
    }

    @Test
    public void testDrain ()
    {
        final int inputPortCount = 3;
        final OperatorTupleQueue operatorTupleQueue = createOperatorTupleQueue( inputPortCount, threadingPreference );
        final TuplesImpl input = createTuples( inputPortCount, TUPLE_QUEUE_SIZE );
        for ( int portIndex = 0; portIndex < inputPortCount; portIndex++ )
        {
            operatorTupleQueue.offer( portIndex, input.getTuples( portIndex ) );
        }

        final List<TuplesImpl> results = new ArrayList<>();
        final Function<PartitionKey, TuplesImpl> tuplesSupplier = objects -> {
            final TuplesImpl tuples = new TuplesImpl( inputPortCount );
            results.add( tuples );
            return tuples;
        };

        final MultiPortDrainer drainer = new NonBlockingMultiPortDisjunctiveDrainer( inputPortCount, 100 );
        final int requiredTupleCount = TUPLE_QUEUE_SIZE / 2;
        drainer.setParameters( TupleAvailabilityByCount.EXACT,
                               new int[] { 0, 1, 2 },
                               new int[] { requiredTupleCount, requiredTupleCount, requiredTupleCount } );

        operatorTupleQueue.drain( drainer, tuplesSupplier );

        assertThat( results.size(), equalTo( 2 ) );
    }

    private OperatorTupleQueue createOperatorTupleQueue ( final int inputPortCount, final ThreadingPreference threadingPreference )
    {
        final TupleQueue[] tupleQueues = new TupleQueue[ inputPortCount ];
        for ( int portIndex = 0; portIndex < inputPortCount; portIndex++ )
        {
            tupleQueues[ portIndex ] = threadingPreference == SINGLE_THREADED
                                       ? new SingleThreadedTupleQueue( TUPLE_QUEUE_SIZE )
                                       : new MultiThreadedTupleQueue( TUPLE_QUEUE_SIZE );
        }
        return new DefaultOperatorTupleQueue( "op1", inputPortCount, threadingPreference, tupleQueues, Integer.MAX_VALUE );
    }

    private TuplesImpl createTuples ( final int inputPortCount, final int tupleCount )
    {
        final TuplesImpl input = new TuplesImpl( inputPortCount );
        for ( int portIndex = 0; portIndex < inputPortCount; portIndex++ )
        {
            for ( int tupleIndex = 1; tupleIndex <= tupleCount; tupleIndex++ )
            {
                final String key = portIndex + "-" + tupleIndex;
                final Tuple t = new Tuple();
                t.set( key, key );
                input.add( portIndex, t );
            }
        }
        return input;
    }

    private void assertTuples ( final int inputPortCount, final int tupleCount, final TuplesImpl output )
    {
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
                final Tuple expected = new Tuple();
                expected.set( key, key );
                assertThat( tuple, equalTo( expected ) );
            }
        }
    }

}
