package cs.bilkent.joker.engine.tuplequeue.impl.drainer;

import java.util.Collection;
import java.util.function.Function;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import cs.bilkent.joker.engine.tuplequeue.TupleQueue;
import cs.bilkent.joker.engine.tuplequeue.impl.queue.MultiThreadedTupleQueue;
import static cs.bilkent.joker.flow.Port.DEFAULT_PORT_INDEX;
import cs.bilkent.joker.operator.Tuple;
import cs.bilkent.joker.operator.impl.TuplesImpl;
import cs.bilkent.joker.operator.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByCount;
import static cs.bilkent.joker.operator.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByCount.AT_LEAST;
import static cs.bilkent.joker.operator.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByCount.AT_LEAST_BUT_SAME_ON_ALL_PORTS;
import static cs.bilkent.joker.operator.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByCount.EXACT;
import cs.bilkent.joker.partition.impl.PartitionKey;
import cs.bilkent.joker.test.AbstractJokerTest;
import static java.util.Arrays.asList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


@RunWith( value = Parameterized.class )
public class SinglePortDrainerTest extends AbstractJokerTest
{

    @Parameters( name = "drainer={0}" )
    public static Collection<Object[]> data ()
    {
        return asList( new Object[][] { { new NonBlockingSinglePortDrainer( "", Integer.MAX_VALUE ) },
                                        { new BlockingSinglePortDrainer( "", Integer.MAX_VALUE ) } } );
    }


    private final SinglePortDrainer drainer;

    private final TuplesImpl result = new TuplesImpl( 1 );

    private final Function<PartitionKey, TuplesImpl> tuplesSupplier = key -> result;

    public SinglePortDrainerTest ( final SinglePortDrainer drainer )
    {
        this.drainer = drainer;
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldFailWithNullTupleQueues ()
    {
        drainer.setParameters( AT_LEAST, 1 );
        drainer.drain( null, null, tuplesSupplier );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldFailWithEmptyTupleQueues ()
    {
        drainer.setParameters( AT_LEAST, 1 );
        drainer.drain( null, new TupleQueue[] {}, tuplesSupplier );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldFailWithMultipleTupleQueues ()
    {
        drainer.setParameters( AT_LEAST, 1 );
        final TupleQueue[] tupleQueues = { new MultiThreadedTupleQueue( 1 ), new MultiThreadedTupleQueue( 1 ) };
        drainer.drain( null, tupleQueues, tuplesSupplier );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldFailWithNullTupleSupplier ()
    {
        drainer.setParameters( AT_LEAST, 1 );
        drainer.drain( null, new TupleQueue[] { new MultiThreadedTupleQueue( 1 ) }, null );
    }

    @Test
    public void shouldDrainAllTuplesWithAtLeastTupleAvailabilityByCountSatisfied ()
    {
        final TupleQueue tupleQueue = new MultiThreadedTupleQueue( 2 );
        final Tuple tuple1 = new Tuple();
        tupleQueue.offer( tuple1 );
        final Tuple tuple2 = new Tuple();
        tupleQueue.offer( tuple2 );

        drainer.setParameters( AT_LEAST, 1 );
        final boolean success = drainer.drain( null, new TupleQueue[] { tupleQueue }, tuplesSupplier );

        assertTrue( success );
        assertThat( result.getNonEmptyPortCount(), equalTo( 1 ) );
        assertThat( result.getTupleCount( DEFAULT_PORT_INDEX ), equalTo( 2 ) );
        assertTrue( tuple1 == result.getTupleOrFail( DEFAULT_PORT_INDEX, 0 ) );
        assertTrue( tuple2 == result.getTupleOrFail( DEFAULT_PORT_INDEX, 1 ) );
        assertThat( tupleQueue.size(), equalTo( 0 ) );
    }

    @Test
    public void shouldNotDrainAnyTupleWithAtLeastTupleAvailabilityByCountUnsatisfied ()
    {
        testNoDrain( AT_LEAST );
    }

    @Test
    public void shouldDrainTuplesWithExactButSameOnAllPortsTupleAvailabilityByCountSatisfied ()
    {
        final TupleQueue tupleQueue = new MultiThreadedTupleQueue( 2 );
        final Tuple tuple1 = new Tuple();
        tupleQueue.offer( tuple1 );
        final Tuple tuple2 = new Tuple();
        tupleQueue.offer( tuple2 );

        drainer.setParameters( AT_LEAST_BUT_SAME_ON_ALL_PORTS, 1 );
        final boolean success = drainer.drain( null, new TupleQueue[] { tupleQueue }, tuplesSupplier );

        assertTrue( success );
        assertThat( result.getNonEmptyPortCount(), equalTo( 1 ) );
        assertThat( result.getTupleCount( DEFAULT_PORT_INDEX ), equalTo( 2 ) );
        assertTrue( tuple1 == result.getTupleOrFail( DEFAULT_PORT_INDEX, 0 ) );
        assertTrue( tuple2 == result.getTupleOrFail( DEFAULT_PORT_INDEX, 1 ) );
        assertThat( tupleQueue.size(), equalTo( 0 ) );
    }

    @Test
    public void shouldNotDrainAnyTupleWithExactButSameOnAllPortsTupleAvailabilityByCountUnsatisfied ()
    {
        testNoDrain( AT_LEAST_BUT_SAME_ON_ALL_PORTS );
    }

    @Test
    public void shouldDrainTuplesWithExactTupleAvailabilityByCountSatisfied ()
    {
        final TupleQueue tupleQueue = new MultiThreadedTupleQueue( 2 );
        final Tuple tuple1 = new Tuple();
        tupleQueue.offer( tuple1 );
        tupleQueue.offer( new Tuple() );

        drainer.setParameters( EXACT, 1 );
        final boolean success = drainer.drain( null, new TupleQueue[] { tupleQueue }, tuplesSupplier );

        assertTrue( success );
        assertThat( result.getNonEmptyPortCount(), equalTo( 1 ) );
        assertThat( result.getTupleCount( DEFAULT_PORT_INDEX ), equalTo( 1 ) );
        assertTrue( tuple1 == result.getTupleOrFail( DEFAULT_PORT_INDEX, 0 ) );

        assertThat( tupleQueue.size(), equalTo( 1 ) );
    }

    @Test
    public void shouldNotDrainAnyTupleWithExactTupleAvailabilityByCountUnsatisfied ()
    {
        testNoDrain( EXACT );
    }

    private void testNoDrain ( final TupleAvailabilityByCount tupleAvailabilityByCount )
    {
        final TupleQueue tupleQueue = new MultiThreadedTupleQueue( 2 );

        drainer.setParameters( tupleAvailabilityByCount, 1 );
        final boolean success = drainer.drain( null, new TupleQueue[] { tupleQueue }, tuplesSupplier );

        assertFalse( success );
        assertTrue( result.isEmpty() );
    }

}
