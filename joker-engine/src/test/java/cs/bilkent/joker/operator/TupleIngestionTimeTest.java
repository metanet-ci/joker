package cs.bilkent.joker.operator;

import java.util.List;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static cs.bilkent.joker.operator.Tuple.INGESTION_TIME_NOT_ASSIGNED;
import static cs.bilkent.joker.operator.Tuple.INGESTION_TIME_UNASSIGNABLE;
import cs.bilkent.joker.operator.Tuple.LatencyStage;
import static cs.bilkent.joker.operator.Tuple.LatencyStage.LatencyStageType.INTER_ARRIVAL_TIME;
import static cs.bilkent.joker.operator.Tuple.LatencyStage.LatencyStageType.INVOCATION_LATENCY;
import static cs.bilkent.joker.operator.Tuple.LatencyStage.LatencyStageType.QUEUE_LATENCY;
import cs.bilkent.joker.test.AbstractJokerTest;
import static java.lang.System.nanoTime;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
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
    public void when_ingestionTimeSet_then_getIngestionTimeReturnsIngestionTime ()
    {
        final Tuple tuple = new Tuple();
        final long t = nanoTime();
        tuple.setIngestionTime( t );
        assertThat( tuple.getIngestionTime(), equalTo( t ) );
        assertNull( tuple.getLatencyStages() );
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
        tuple.setIngestionTime( nanoTime() );
        thrown.expect( IllegalStateException.class );
        tuple.setIngestionTime( nanoTime() );
    }

    @Test
    public void when_ingestionTimeSet_then_queueLatencyStageIsAdded ()
    {
        final Tuple tuple = new Tuple();

        tuple.setIngestionTime( 1 );

        tuple.setQueueOfferTime( 1 );
        tuple.recordQueueLatency( "op", 3 );

        assertThat( tuple.getIngestionTime(), equalTo( 1L ) );
        final List<LatencyStage> stages = tuple.getLatencyStages();
        assertNotNull( stages );
        assertThat( stages, hasSize( 1 ) );
        final LatencyStage stage = stages.get( 0 );
        assertThat( stage.getDuration(), equalTo( 2L ) );
        assertThat( stage.getOperatorId(), equalTo( "op" ) );
        assertThat( stage.getType(), equalTo( QUEUE_LATENCY ) );
    }

    @Test
    public void when_ingestionTimeNotSet_then_queueLatencyStageIsAdded ()
    {
        final Tuple tuple = new Tuple();

        tuple.setQueueOfferTime( 1 );
        tuple.recordQueueLatency( "op", 3 );

        final List<LatencyStage> stages = tuple.getLatencyStages();
        assertNotNull( stages );
        assertThat( stages, hasSize( 1 ) );
        final LatencyStage stage = stages.get( 0 );
        assertThat( stage.getDuration(), equalTo( 2L ) );
        assertThat( stage.getOperatorId(), equalTo( "op" ) );
        assertThat( stage.getType(), equalTo( QUEUE_LATENCY ) );
    }

    @Test
    public void when_ingestionTimeSet_then_invocationLatencyStageIsAdded ()
    {
        final Tuple tuple = new Tuple();

        tuple.setIngestionTime( 1 );

        tuple.recordInvocationLatency( "op", 3 );

        assertThat( tuple.getIngestionTime(), equalTo( 1L ) );
        final List<LatencyStage> stages = tuple.getLatencyStages();
        assertNotNull( stages );
        assertThat( stages, hasSize( 1 ) );
        final LatencyStage stage = stages.get( 0 );
        assertThat( stage.getDuration(), equalTo( 3L ) );
        assertThat( stage.getOperatorId(), equalTo( "op" ) );
        assertThat( stage.getType(), equalTo( INVOCATION_LATENCY ) );
    }

    @Test
    public void when_ingestionTimeNotSet_then_invocationLatencyStageIsAdded ()
    {
        final Tuple tuple = new Tuple();

        tuple.recordInvocationLatency( "op", 3 );

        final List<LatencyStage> stages = tuple.getLatencyStages();
        assertNotNull( stages );
        assertThat( stages, hasSize( 1 ) );
        final LatencyStage stage = stages.get( 0 );
        assertThat( stage.getDuration(), equalTo( 3L ) );
        assertThat( stage.getOperatorId(), equalTo( "op" ) );
        assertThat( stage.getType(), equalTo( INVOCATION_LATENCY ) );
    }

    @Test
    public void when_ingestionTimeSet_then_interArrivalTimeStageIsAdded ()
    {
        final Tuple tuple = new Tuple();

        tuple.setIngestionTime( 1 );

        tuple.recordInterArrivalTime( "op", 4 );

        assertThat( tuple.getIngestionTime(), equalTo( 1L ) );
        final List<LatencyStage> stages = tuple.getLatencyStages();
        assertNotNull( stages );
        assertThat( stages, hasSize( 1 ) );
        final LatencyStage stage = stages.get( 0 );
        assertThat( stage.getDuration(), equalTo( 4L ) );
        assertThat( stage.getOperatorId(), equalTo( "op" ) );
        assertThat( stage.getType(), equalTo( INTER_ARRIVAL_TIME ) );
    }

    @Test
    public void when_ingestionTimeNotSet_then_interArrivalTimeStageIsAdded ()
    {
        final Tuple tuple = new Tuple();

        tuple.recordInterArrivalTime( "op", 4 );

        final List<LatencyStage> stages = tuple.getLatencyStages();
        assertNotNull( stages );
        assertThat( stages, hasSize( 1 ) );
        final LatencyStage stage = stages.get( 0 );
        assertThat( stage.getDuration(), equalTo( 4L ) );
        assertThat( stage.getOperatorId(), equalTo( "op" ) );
        assertThat( stage.getType(), equalTo( INTER_ARRIVAL_TIME ) );
    }


    @Test
    public void when_attachedToLaterTuple_then_latencyStagesArePassed ()
    {
        final Tuple source = new Tuple();
        final long ingestionTime = System.nanoTime();
        source.setIngestionTime( ingestionTime );
        source.recordInvocationLatency( "op1", 50 );

        final Tuple destination = new Tuple();
        destination.setIngestionTime( ingestionTime - 1 );
        destination.recordInvocationLatency( "op2", 100 );

        destination.attachTo( source );

        assertThat( destination.getIngestionTime(), equalTo( ingestionTime ) );
        final List<LatencyStage> stages = destination.getLatencyStages();
        assertNotNull( stages );
        assertThat( stages, hasSize( 2 ) );
        final LatencyStage stage1 = stages.get( 0 );
        assertThat( stage1.getDuration(), equalTo( 100L ) );
        assertThat( stage1.getOperatorId(), equalTo( "op2" ) );
        assertThat( stage1.getType(), equalTo( INVOCATION_LATENCY ) );
        final LatencyStage stage2 = stages.get( 1 );
        assertThat( stage2.getDuration(), equalTo( 50L ) );
        assertThat( stage2.getOperatorId(), equalTo( "op1" ) );
        assertThat( stage2.getType(), equalTo( INVOCATION_LATENCY ) );
    }

    @Test
    public void when_attachedToIngestionTimeUnknownTuple_then_latencyStagesArePassed ()
    {
        final Tuple source = new Tuple();
        source.recordInvocationLatency( "op1", 50 );

        final Tuple destination = new Tuple();
        final long ingestionTime = System.nanoTime();
        destination.setIngestionTime( ingestionTime );
        destination.recordInvocationLatency( "op2", 100 );

        destination.attachTo( source );

        assertThat( destination.getIngestionTime(), equalTo( INGESTION_TIME_UNASSIGNABLE ) );
        final List<LatencyStage> stages = destination.getLatencyStages();
        assertNotNull( stages );
        assertThat( stages, hasSize( 2 ) );
        final LatencyStage stage1 = stages.get( 0 );
        assertThat( stage1.getDuration(), equalTo( 100L ) );
        assertThat( stage1.getOperatorId(), equalTo( "op2" ) );
        assertThat( stage1.getType(), equalTo( INVOCATION_LATENCY ) );
        final LatencyStage stage2 = stages.get( 1 );
        assertThat( stage2.getDuration(), equalTo( 50L ) );
        assertThat( stage2.getOperatorId(), equalTo( "op1" ) );
        assertThat( stage2.getType(), equalTo( INVOCATION_LATENCY ) );
    }

    @Test
    public void when_attachedToIngestionTimeUnassignableTuple_then_latencyStagesArePassed ()
    {
        final Tuple source = new Tuple();
        source.recordInvocationLatency( "op1", 50 );

        final Tuple destination1 = new Tuple();
        destination1.setIngestionTime( System.nanoTime() );
        destination1.attachTo( source );

        final Tuple destination2 = new Tuple();
        destination2.recordInvocationLatency( "op2", 100 );

        destination2.attachTo( destination1 );

        assertThat( destination2.getIngestionTime(), equalTo( INGESTION_TIME_UNASSIGNABLE ) );
        final List<LatencyStage> stages = destination2.getLatencyStages();
        assertNotNull( stages );
        assertThat( stages, hasSize( 2 ) );
        final LatencyStage stage1 = stages.get( 0 );
        assertThat( stage1.getDuration(), equalTo( 100L ) );
        assertThat( stage1.getOperatorId(), equalTo( "op2" ) );
        assertThat( stage1.getType(), equalTo( INVOCATION_LATENCY ) );
        final LatencyStage stage2 = stages.get( 1 );
        assertThat( stage2.getDuration(), equalTo( 50L ) );
        assertThat( stage2.getOperatorId(), equalTo( "op1" ) );
        assertThat( stage2.getType(), equalTo( INVOCATION_LATENCY ) );
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
        destination.recordInvocationLatency( "op", 20 );

        destination.attachTo( source );

        assertThat( destination.getIngestionTime(), equalTo( t2 ) );
        assertThat( destination.getLatencyStages(), hasSize( 1 ) );
    }

    @Test
    public void when_shallowCopied_then_sameInternalStateIsUsed ()
    {
        final Tuple tuple = new Tuple();
        tuple.set( "key", "val1" );
        final long ingestionTime = 10;
        tuple.setIngestionTime( ingestionTime );
        tuple.recordInvocationLatency( "op", 20 );

        final Tuple copy = tuple.shallowCopy();

        assertThat( copy.get( "key" ), equalTo( "val1" ) );
        assertThat( copy.getIngestionTime(), equalTo( ingestionTime ) );
        assertThat( copy.getLatencyStages(), hasSize( 1 ) );

        tuple.set( "key", "val2" );
        assertThat( copy.get( "key" ), equalTo( "val2" ) );
    }

}
