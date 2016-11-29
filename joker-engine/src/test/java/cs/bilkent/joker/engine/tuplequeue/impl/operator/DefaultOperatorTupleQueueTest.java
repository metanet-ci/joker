package cs.bilkent.joker.engine.tuplequeue.impl.operator;

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
import cs.bilkent.joker.engine.tuplequeue.impl.queue.MultiThreadedTupleQueue;
import cs.bilkent.joker.engine.tuplequeue.impl.queue.SingleThreadedTupleQueue;
import cs.bilkent.joker.operator.Tuple;
import cs.bilkent.joker.operator.impl.TuplesImpl;
import cs.bilkent.joker.test.AbstractJokerTest;
import static java.util.Arrays.asList;
import static junit.framework.TestCase.assertNotNull;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

@RunWith( Parameterized.class )
public class DefaultOperatorTupleQueueTest extends AbstractJokerTest
{

    private static final int TUPLE_QUEUE_SIZE = 2;

    @Parameters( name = "constructor={0}, threadingPreference={1}" )
    public static Collection<Object[]> data ()
    {
        final Function<Integer, TupleQueue> c0 = ( portIndex ) -> new SingleThreadedTupleQueue( TUPLE_QUEUE_SIZE );
        final Function<Integer, TupleQueue> c1 = ( portIndex ) -> new MultiThreadedTupleQueue( TUPLE_QUEUE_SIZE );
        return asList( new Object[][] { { c0, SINGLE_THREADED }, { c1, MULTI_THREADED } } );
    }

    private final Function<Integer, TupleQueue> tupleQueueConstructor;

    private final ThreadingPreference threadingPreference;

    public DefaultOperatorTupleQueueTest ( final Function<Integer, TupleQueue> tupleQueueConstructor,
                                           final ThreadingPreference threadingPreference )
    {
        this.tupleQueueConstructor = tupleQueueConstructor;
        this.threadingPreference = threadingPreference;
    }

    @Test
    public void shouldOfferSinglePortTuples ()
    {
        testOfferTuples( tupleQueueConstructor, 1, threadingPreference );
    }

    @Test
    public void shouldOfferMultiPortTuples ()
    {
        testOfferTuples( tupleQueueConstructor, 2, threadingPreference );
    }

    private void testOfferTuples ( final Function<Integer, TupleQueue> tupleQueueConstructor,
                                   final int inputPortCount,
                                   final ThreadingPreference threadingPreference )
    {
        final OperatorTupleQueue operatorTupleQueue = new DefaultOperatorTupleQueue( "op1",
                                                                                     inputPortCount,
                                                                                     threadingPreference,
                                                                                     tupleQueueConstructor,
                                                                                     Integer.MAX_VALUE );

        final TuplesImpl input = createTuples( inputPortCount, TUPLE_QUEUE_SIZE );

        for ( int portIndex = 0; portIndex < inputPortCount; portIndex++ )
        {
            final List<Tuple> tuples = input.getTuples( portIndex );
            final int offered = operatorTupleQueue.offer( portIndex, tuples );
            assertThat( offered, equalTo( tuples.size() ) );
        }

        final GreedyDrainer drainer = new GreedyDrainer( inputPortCount );
        operatorTupleQueue.drain( drainer );

        assertTuples( inputPortCount, TUPLE_QUEUE_SIZE, drainer );
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
                final Tuple expected = new Tuple();
                expected.set( key, key );
                assertThat( tuple, equalTo( expected ) );
            }
        }
    }

}
