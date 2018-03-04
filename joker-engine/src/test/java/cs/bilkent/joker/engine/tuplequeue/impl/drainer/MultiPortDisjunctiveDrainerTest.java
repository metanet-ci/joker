package cs.bilkent.joker.engine.tuplequeue.impl.drainer;

import java.util.Collection;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import cs.bilkent.joker.engine.tuplequeue.TupleQueue;
import cs.bilkent.joker.engine.tuplequeue.impl.queue.SingleThreadedTupleQueue;
import cs.bilkent.joker.operator.Tuple;
import cs.bilkent.joker.operator.impl.TuplesImpl;
import static cs.bilkent.joker.operator.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByCount.AT_LEAST;
import static cs.bilkent.joker.operator.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByCount.EXACT;
import cs.bilkent.joker.test.AbstractJokerTest;
import static java.util.Arrays.asList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertTrue;

@RunWith( value = Parameterized.class )
public class MultiPortDisjunctiveDrainerTest extends AbstractJokerTest
{

    private static final int INPUT_PORT_COUNT = 2;

    @Parameters( name = "drainer={0}" )
    public static Collection<Object[]> data ()
    {
        return asList( new Object[][] { { new NonBlockingMultiPortDisjunctiveDrainer( INPUT_PORT_COUNT, Integer.MAX_VALUE ) },
                                        { new BlockingMultiPortDisjunctiveDrainer( INPUT_PORT_COUNT, Integer.MAX_VALUE ) } } );
    }

    private final MultiPortDrainer drainer;

    public MultiPortDisjunctiveDrainerTest ( final MultiPortDrainer drainer )
    {
        this.drainer = drainer;
    }

    @Test
    public void test_TupleAvailabilityByCount_AT_LEAST_allQueuesSatisfy ()
    {
        drainer.setParameters( AT_LEAST, new int[] { 0, 1 }, new int[] { 2, 2 } );
        final TupleQueue tupleQueue0 = new SingleThreadedTupleQueue( 2 );
        final TupleQueue tupleQueue1 = new SingleThreadedTupleQueue( 2 );
        tupleQueue0.offer( new Tuple() );
        tupleQueue0.offer( new Tuple() );
        tupleQueue1.offer( new Tuple() );
        tupleQueue1.offer( new Tuple() );

        final TuplesImpl result = new TuplesImpl( 2 );
        drainer.drain( null, new TupleQueue[] { tupleQueue0, tupleQueue1 }, key -> result );

        assertTrue( result.isNonEmpty() );
        assertThat( result.getNonEmptyPortCount(), equalTo( 2 ) );
        assertThat( result.getTupleCount( 0 ), equalTo( 2 ) );
        assertThat( result.getTupleCount( 1 ), equalTo( 2 ) );
        assertThat( tupleQueue0.size(), equalTo( 0 ) );
        assertThat( tupleQueue1.size(), equalTo( 0 ) );
    }

    @Test
    public void test_TupleAvailabilityByCount_AT_LEAST_allQueuesDoNotSatisfy ()
    {
        drainer.setParameters( AT_LEAST, new int[] { 0, 1 }, new int[] { 2, 2 } );
        final TupleQueue tupleQueue1 = new SingleThreadedTupleQueue( 2 );
        final TupleQueue tupleQueue2 = new SingleThreadedTupleQueue( 2 );
        tupleQueue1.offer( new Tuple() );
        tupleQueue1.offer( new Tuple() );
        tupleQueue2.offer( new Tuple() );

        final TuplesImpl result = new TuplesImpl( 2 );
        drainer.drain( null, new TupleQueue[] { tupleQueue1, tupleQueue2 }, key -> result );

        assertTrue( result.isNonEmpty() );
        assertThat( result.getNonEmptyPortCount(), equalTo( 1 ) );
        assertThat( result.getTupleCount( 0 ), equalTo( 2 ) );
        assertThat( result.getTupleCount( 1 ), equalTo( 0 ) );
        assertThat( tupleQueue1.size(), equalTo( 0 ) );
        assertThat( tupleQueue2.size(), equalTo( 1 ) );
    }

    @Test
    public void test_TupleAvailabilityByCount_EXACT_allQueuesSatisfy ()
    {
        drainer.setParameters( EXACT, new int[] { 0, 1 }, new int[] { 2, 2 } );
        final TupleQueue tupleQueue1 = new SingleThreadedTupleQueue( 2 );
        final TupleQueue tupleQueue2 = new SingleThreadedTupleQueue( 2 );
        tupleQueue1.offer( new Tuple() );
        tupleQueue1.offer( new Tuple() );
        tupleQueue1.offer( new Tuple() );
        tupleQueue2.offer( new Tuple() );
        tupleQueue2.offer( new Tuple() );

        final TuplesImpl result = new TuplesImpl( 2 );
        drainer.drain( null, new TupleQueue[] { tupleQueue1, tupleQueue2 }, key -> result );

        assertTrue( result.isNonEmpty() );
        assertThat( result.getNonEmptyPortCount(), equalTo( 2 ) );
        assertThat( result.getTupleCount( 0 ), equalTo( 2 ) );
        assertThat( result.getTupleCount( 1 ), equalTo( 2 ) );
        assertThat( tupleQueue1.size(), equalTo( 1 ) );
        assertThat( tupleQueue2.size(), equalTo( 0 ) );
    }

    @Test
    public void test_TupleAvailabilityByCount_EXACT_allQueuesDoNotSatisfy ()
    {
        drainer.setParameters( EXACT, new int[] { 0, 1 }, new int[] { 2, 2 } );
        final TupleQueue tupleQueue1 = new SingleThreadedTupleQueue( 2 );
        final TupleQueue tupleQueue2 = new SingleThreadedTupleQueue( 2 );
        tupleQueue1.offer( new Tuple() );
        tupleQueue1.offer( new Tuple() );
        tupleQueue1.offer( new Tuple() );
        tupleQueue2.offer( new Tuple() );

        final TuplesImpl result = new TuplesImpl( 2 );
        drainer.drain( null, new TupleQueue[] { tupleQueue1, tupleQueue2 }, key -> result );

        assertTrue( result.isNonEmpty() );
        assertThat( result.getNonEmptyPortCount(), equalTo( 1 ) );
        assertThat( result.getTupleCount( 0 ), equalTo( 2 ) );
        assertThat( result.getTupleCount( 1 ), equalTo( 0 ) );
        assertThat( tupleQueue1.size(), equalTo( 1 ) );
        assertThat( tupleQueue2.size(), equalTo( 1 ) );
    }

}
