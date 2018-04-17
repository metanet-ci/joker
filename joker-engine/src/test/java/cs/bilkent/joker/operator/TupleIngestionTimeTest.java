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
    public void when_ingestionTimeSet_then_setIngestionTimeReturns ()
    {
        final Tuple tuple = new Tuple();
        final long t = nanoTime();
        tuple.setIngestionTime( t );
        assertThat( tuple.getIngestionTime(), equalTo( t ) );
    }

    @Test
    public void when_invalidIngestionTimeIsGiven_then_setIngestionTimeFails ()
    {
        thrown.expect( IllegalArgumentException.class );
        new Tuple().setIngestionTime( INGESTION_TIME_NOT_ASSIGNED );
    }

    @Test
    public void when_ingestionTimeSetMultipleTimes_then_setIngestionTimeFails ()
    {
        final Tuple tuple = new Tuple();
        tuple.setIngestionTime( System.nanoTime() );
        thrown.expect( IllegalStateException.class );
        tuple.setIngestionTime( System.nanoTime() );
    }

    @Test
    public void when_ingestionTimeIsSetOnSourceTuple_then_passedToAttachingTuple ()
    {
        final Tuple source = new Tuple();
        final long ingestionTime = System.nanoTime();
        source.setIngestionTime( ingestionTime );
        final long invLatency = 100;
        source.recordInvocationLatency( "op1", invLatency );

        final Tuple destination = new Tuple();
        destination.attach( source );
        source.recordInvocationLatency( "op2", invLatency );

        assertThat( destination.getIngestionTime(), equalTo( ingestionTime ) );
        assertThat( destination.getLatencyRecs(), equalTo( singletonList( Triple.of( "op1", true, invLatency ) ) ) );
    }

    @Test
    public void when_attached_then_latencyRecIsPassedToAttachingTuple ()
    {
        final Tuple source = new Tuple();
        final long ingestionTime = System.nanoTime();
        source.setIngestionTime( ingestionTime );

        final Tuple destination = new Tuple();
        destination.setIngestionTime( ingestionTime - 1 );
        final long invLatency = 100;
        destination.recordInvocationLatency( "op1", invLatency );

        destination.attach( source );

        assertThat( destination.getIngestionTime(), equalTo( ingestionTime ) );
        assertNull( destination.getLatencyRecs() );
    }

    @Test
    public void when_attachedMultipleTimes_then_theMostRecentIngestionTimeIsPassed ()
    {
        final Tuple source1 = new Tuple();
        final long ingestionTime1 = System.nanoTime();
        source1.setIngestionTime( ingestionTime1 );
        final long invLatency1 = 100;
        source1.recordInvocationLatency( "op1", invLatency1 );

        final Tuple source2 = new Tuple();
        final long ingestionTime2 = ingestionTime1 + 100;
        source2.setIngestionTime( ingestionTime2 );
        final long invLatency2 = invLatency1 - 10;
        source2.recordInvocationLatency( "op2", invLatency2 );

        final Tuple destination = new Tuple();
        destination.attach( source1 );
        destination.attach( source2 );

        assertThat( destination.getIngestionTime(), equalTo( ingestionTime2 ) );
        assertThat( destination.getLatencyRecs(), equalTo( singletonList( Triple.of( "op2", true, invLatency2 ) ) ) );
    }

    @Test
    public void when_ingestionTimeNotSetOnSourceTuple_then_attachingTupleBecomesUnassignable ()
    {
        final Tuple destination = new Tuple();
        destination.attach( new Tuple() );

        assertThat( destination.getIngestionTime(), equalTo( INGESTION_TIME_UNASSIGNABLE ) );
        assertNull( destination.getLatencyRecs() );
    }

    @Test
    public void when_ingestionTimeIsUnassignable_then_noIngestionTimeIsAssigned ()
    {
        final Tuple source1 = new Tuple();

        final Tuple source2 = new Tuple();
        source2.setIngestionTime( System.nanoTime() );

        final Tuple destination = new Tuple();
        destination.attach( source1 );
        destination.attach( source2 );

        assertThat( destination.getIngestionTime(), equalTo( INGESTION_TIME_UNASSIGNABLE ) );
    }

    @Test
    public void when_ingestionTimeIsEarlierOnSource_then_notPassedToAttachingTuple ()
    {
        final Tuple source = new Tuple();
        final long t1 = System.nanoTime();
        source.setIngestionTime( t1 );

        final Tuple destination = new Tuple();
        final long t2 = t1 + 100;
        destination.setIngestionTime( t2 );
        final long invLatency = 100;
        destination.recordInvocationLatency( "op", invLatency );

        destination.attach( source );

        assertThat( destination.getIngestionTime(), equalTo( t2 ) );
        assertThat( destination.getLatencyRecs(), equalTo( singletonList( Triple.of( "op", true, invLatency ) ) ) );
    }

    @Test
    public void when_ingestionTimeIsLaterOnSource_then_passedToAttachingTuple ()
    {
        final long t1 = System.nanoTime();
        final long t2 = t1 + 100;
        final Tuple source = new Tuple();
        source.setIngestionTime( t2 );
        final long invLatency1 = 100;
        source.recordInvocationLatency( "op", invLatency1 );

        final Tuple destination = new Tuple();
        destination.setIngestionTime( t1 );
        final long invLatency2 = 101;
        destination.recordInvocationLatency( "op", invLatency2 );

        destination.attach( source );

        assertThat( destination.getIngestionTime(), equalTo( t2 ) );
        assertThat( destination.getLatencyRecs(), equalTo( singletonList( Triple.of( "op", true, invLatency1 ) ) ) );
    }

    @Test
    public void when_naIngestionTimeIsProvided_then_ingestionTimeOverwritten ()
    {
        final Tuple tuple = new Tuple();
        final long t = 1;
        tuple.setIngestionTime( t );
        tuple.recordInvocationLatency( "op", 100 );

        tuple.overwriteIngestionTime( new Tuple() );

        assertThat( tuple.getIngestionTime(), equalTo( INGESTION_TIME_NOT_ASSIGNED ) );
        assertNull( tuple.getLatencyRecs() );
    }

    @Test
    public void when_ingestionTimeIsProvided_then_ingestionTimeOverwritten ()
    {
        final long t1 = System.nanoTime();
        final Tuple source = new Tuple();
        source.setIngestionTime( t1 );

        final long t2 = t1 + 100;
        final Tuple target = new Tuple();
        target.setIngestionTime( t2 );

        target.overwriteIngestionTime( source );

        assertThat( target.getIngestionTime(), equalTo( t1 ) );
    }

    @Test
    public void when_copiedForAttachment_then_sameInternalStateIsUsed ()
    {
        final Tuple tuple = new Tuple();
        tuple.set( "key", "val1" );
        final long ingestionTime = 10;
        tuple.setIngestionTime( ingestionTime );

        final Tuple copy = tuple.copyForAttachment();

        assertThat( copy.get( "key" ), equalTo( "val1" ) );
        assertThat( copy.getIngestionTime(), equalTo( ingestionTime ) );

        tuple.set( "key", "val2" );
        assertThat( copy.get( "key" ), equalTo( "val2" ) );
    }

}
