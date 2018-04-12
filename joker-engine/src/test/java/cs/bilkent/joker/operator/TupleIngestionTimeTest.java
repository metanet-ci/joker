package cs.bilkent.joker.operator;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static cs.bilkent.joker.operator.Tuple.INGESTION_TIME_NA;
import cs.bilkent.joker.test.AbstractJokerTest;
import static java.lang.System.nanoTime;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.rules.ExpectedException.none;

public class TupleIngestionTimeTest extends AbstractJokerTest
{

    @Rule
    public ExpectedException thrown = none();

    @Test
    public void when_ingestionTimeNotSet_then_nonAvailableIngestionTimeReturns ()
    {
        assertThat( new Tuple().getIngestionTime(), equalTo( INGESTION_TIME_NA ) );
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
    public void when_invalidIngestionTimeSet_then_fails ()
    {
        thrown.expect( IllegalArgumentException.class );
        new Tuple().setIngestionTime( INGESTION_TIME_NA );
    }

    @Test
    public void when_ingestionTimeSetMultipleTimes_then_fails ()
    {
        final Tuple tuple = new Tuple();
        tuple.setIngestionTime( System.nanoTime() );
        thrown.expect( IllegalStateException.class );
        tuple.setIngestionTime( System.nanoTime() );
    }

    @Test
    public void when_ingestionTimeIsSetOnSourceTuple_then_passedToDerivedTuple ()
    {
        final Tuple source = new Tuple();
        final long t = System.nanoTime();
        source.setIngestionTime( t );

        final Tuple destination = new Tuple();
        destination.attach( source );
        assertThat( destination.getIngestionTime(), equalTo( t ) );
    }

    @Test
    public void when_ingestionTimeNotSetOnSourceTuple_then_notPassedToDerivedTuple ()
    {
        final Tuple destination = new Tuple();
        destination.attach( new Tuple() );
        assertThat( destination.getIngestionTime(), equalTo( INGESTION_TIME_NA ) );
    }

    @Test
    public void when_ingestionTimeIsEarlierOnSource_then_notPassedToDerivedTuple ()
    {
        final Tuple source = new Tuple();
        final long t1 = System.nanoTime();
        source.setIngestionTime( t1 );

        final Tuple destination = new Tuple();
        final long t2 = t1 + 100;
        destination.setIngestionTime( t2 );
        destination.attach( source );
        assertThat( destination.getIngestionTime(), equalTo( t2 ) );
    }

    @Test
    public void when_ingestionTimeIsLaterOnSource_then_passedToDerivedTuple ()
    {
        final long t1 = System.nanoTime();
        final long t2 = t1 + 100;
        final Tuple source = new Tuple();
        source.setIngestionTime( t2 );

        final Tuple destination = new Tuple();
        destination.setIngestionTime( t1 );
        destination.attach( source );
        assertThat( destination.getIngestionTime(), equalTo( t2 ) );
    }

    @Test
    public void when_naIngestionTimeIsProvided_then_ingestionTimeOverwritten ()
    {
        final Tuple tuple = new Tuple();
        final long t = 1;
        tuple.setIngestionTime( t );
        tuple.overwriteIngestionTime( INGESTION_TIME_NA );

        assertThat( tuple.getIngestionTime(), equalTo( INGESTION_TIME_NA ) );
    }

    @Test
    public void when_ingestionTimeIsProvided_then_ingestionTimeOverwritten ()
    {
        final Tuple tuple = new Tuple();
        final long t1 = System.nanoTime();
        final long t2 = t1 + 100;
        tuple.setIngestionTime( t2 );
        tuple.overwriteIngestionTime( t1 );

        assertThat( tuple.getIngestionTime(), equalTo( t1 ) );
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
