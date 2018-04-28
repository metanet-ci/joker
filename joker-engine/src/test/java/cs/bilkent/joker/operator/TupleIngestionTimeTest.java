package cs.bilkent.joker.operator;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static cs.bilkent.joker.operator.Tuple.INGESTION_TIME_NOT_ASSIGNED;
import static cs.bilkent.joker.operator.Tuple.INGESTION_TIME_UNASSIGNABLE;
import cs.bilkent.joker.operator.utils.Triple;
import cs.bilkent.joker.test.AbstractJokerTest;
import static java.lang.System.nanoTime;
import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.rules.ExpectedException.none;

public class TupleIngestionTimeTest extends AbstractJokerTest
{

    @Rule
    public ExpectedException thrown = none();

    @Test
    public void when_ingestionTimeNotSet_then_nonAvailableIngestionTimeReturns ()
    {
        assertThat( new Tuple().getIngestionTime(), equalTo( INGESTION_TIME_NOT_ASSIGNED ) );
    }

    @Test
    public void when_ingestionTimeSetWithLatencyRecordsTracked_then_setIngestionTimeReturns ()
    {
        final Tuple tuple = new Tuple();
        final long t = nanoTime();
        tuple.setIngestionTime( t, true );
        assertThat( tuple.getIngestionTime(), equalTo( t ) );
        assertNotNull( tuple.getLatencyRecs() );
    }

    @Test
    public void when_ingestionTimeSetWithLatencyRecordsNotTracked_then_setIngestionTimeReturns ()
    {
        final Tuple tuple = new Tuple();
        final long t = nanoTime();
        tuple.setIngestionTime( t, false );
        assertThat( tuple.getIngestionTime(), equalTo( t ) );
        assertNull( tuple.getLatencyRecs() );
    }

    @Test
    public void when_invalidIngestionTimeIsGiven_then_setIngestionTimeFails ()
    {
        thrown.expect( IllegalArgumentException.class );
        new Tuple().setIngestionTime( INGESTION_TIME_NOT_ASSIGNED, true );
    }

    @Test
    public void when_ingestionTimeSetMultipleTimes_then_setIngestionTimeFails ()
    {
        final Tuple tuple = new Tuple();
        tuple.setIngestionTime( System.nanoTime(), true );
        thrown.expect( IllegalStateException.class );
        tuple.setIngestionTime( System.nanoTime(), true );
    }

    @Test
    public void when_ingestionTimeNotSet_then_noQueueLatencyRecordIsAdded ()
    {
        final Tuple tuple = new Tuple();

        tuple.setQueueOfferTime( 1 );
        tuple.recordQueueLatency( "op", 2 );

        assertNull( tuple.getLatencyRecs() );
    }

    @Test
    public void when_ingestionTimeNotSet_then_noInvocationLatencyRecordIsAdded ()
    {
        final Tuple tuple = new Tuple();

        tuple.recordInvocationLatency( "op", 1 );

        assertNull( tuple.getLatencyRecs() );
    }

    @Test
    public void when_attached_then_latencyRecOnSourceIsOverwritten()
    {
        final Tuple source = new Tuple();
        final long ingestionTime = System.nanoTime();
        source.setIngestionTime( ingestionTime, false );

        final Tuple destination = new Tuple();
        destination.setIngestionTime( ingestionTime - 1, true );
        final long invLatency = 100;
        destination.recordInvocationLatency( "op1", invLatency );

        destination.attachTo( source );

        assertThat( destination.getIngestionTime(), equalTo( ingestionTime ) );
        assertNull( destination.getLatencyRecs() );
    }

    @Test
    public void when_ingestionTimeIsSetOnSourceTuple_then_passedToAttachingTuple ()
    {
        final Tuple source = new Tuple();
        final long ingestionTime = System.nanoTime();
        source.setIngestionTime( ingestionTime, true );
        final long invLatency = 100;
        source.recordInvocationLatency( "op1", invLatency );

        final Tuple destination = new Tuple();
        destination.attachTo( source );
        source.recordInvocationLatency( "op2", invLatency );

        assertThat( destination.getIngestionTime(), equalTo( ingestionTime ) );
        assertThat( destination.getLatencyRecs(), equalTo( singletonList( Triple.of( "op1", true, invLatency ) ) ) );
    }

    @Test
    public void when_attachedMultipleTimes_then_theMostRecentIngestionTimeIsPassed ()
    {
        final Tuple source1 = new Tuple();
        final long ingestionTime1 = System.nanoTime();
        source1.setIngestionTime( ingestionTime1, true );
        final long invLatency1 = 100;
        source1.recordInvocationLatency( "op1", invLatency1 );

        final Tuple source2 = new Tuple();
        final long ingestionTime2 = ingestionTime1 + 100;
        source2.setIngestionTime( ingestionTime2, true );
        final long invLatency2 = invLatency1 - 10;
        source2.recordInvocationLatency( "op2", invLatency2 );

        final Tuple destination = new Tuple();
        destination.attachTo( source1 );
        destination.attachTo( source2 );

        assertThat( destination.getIngestionTime(), equalTo( ingestionTime2 ) );
        assertThat( destination.getLatencyRecs(), equalTo( singletonList( Triple.of( "op2", true, invLatency2 ) ) ) );
    }

    @Test
    public void when_ingestionTimeNotSetOnSourceTuple_then_attachingTupleBecomesUnassignable ()
    {
        final Tuple destination = new Tuple();
        destination.attachTo( new Tuple() );

        assertThat( destination.getIngestionTime(), equalTo( INGESTION_TIME_UNASSIGNABLE ) );
        assertNull( destination.getLatencyRecs() );
    }

    @Test
    public void when_ingestionTimeIsUnassignableOnSource_then_noIngestionTimeIsAssignedAfterwards ()
    {
        final Tuple source1 = new Tuple();

        final Tuple source2 = new Tuple();
        source2.setIngestionTime( System.nanoTime(), true );

        final Tuple destination = new Tuple();
        destination.attachTo( source1 );
        destination.attachTo( source2 );

        assertThat( destination.getIngestionTime(), equalTo( INGESTION_TIME_UNASSIGNABLE ) );
    }

    @Test
    public void when_ingestionTimeIsUnassignableOnSource_then_noIngestionTimeIsAssigned ()
    {
        final Tuple source1 = new Tuple();
        source1.setIngestionTime( System.nanoTime(), true );

        final Tuple source2 = new Tuple();

        final Tuple destination = new Tuple();
        destination.attachTo( source1 );
        destination.attachTo( source2 );

        assertThat( destination.getIngestionTime(), equalTo( INGESTION_TIME_UNASSIGNABLE ) );
    }

    @Test
    public void when_ingestionTimeIsEarlierOnSource_then_notPassedToAttachingTuple ()
    {
        final Tuple source = new Tuple();
        final long t1 = System.nanoTime();
        source.setIngestionTime( t1, false );

        final Tuple destination = new Tuple();
        final long t2 = t1 + 100;
        destination.setIngestionTime( t2, true );
        final long invLatency = 100;
        destination.recordInvocationLatency( "op", invLatency );

        destination.attachTo( source );

        assertThat( destination.getIngestionTime(), equalTo( t2 ) );
        assertThat( destination.getLatencyRecs(), equalTo( singletonList( Triple.of( "op", true, invLatency ) ) ) );
    }

    @Test
    public void when_shallowCopied_then_sameInternalStateIsUsed ()
    {
        final Tuple tuple = new Tuple();
        tuple.set( "key", "val1" );
        final long ingestionTime = 10;
        tuple.setIngestionTime( ingestionTime, false );

        final Tuple copy = tuple.shallowCopy();

        assertThat( copy.get( "key" ), equalTo( "val1" ) );
        assertThat( copy.getIngestionTime(), equalTo( ingestionTime ) );

        tuple.set( "key", "val2" );
        assertThat( copy.get( "key" ), equalTo( "val2" ) );
    }

}
