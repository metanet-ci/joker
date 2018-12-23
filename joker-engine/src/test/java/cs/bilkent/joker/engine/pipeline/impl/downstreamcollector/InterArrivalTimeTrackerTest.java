package cs.bilkent.joker.engine.pipeline.impl.downstreamcollector;

import java.util.List;
import java.util.function.LongSupplier;

import org.junit.Before;
import org.junit.Test;

import cs.bilkent.joker.engine.pipeline.DownstreamCollector;
import cs.bilkent.joker.operator.Tuple;
import cs.bilkent.joker.operator.Tuple.LatencyStage;
import static cs.bilkent.joker.operator.Tuple.LatencyStage.LatencyStageType.INTER_ARRIVAL_TIME;
import cs.bilkent.joker.operator.impl.TuplesImpl;
import cs.bilkent.joker.test.AbstractJokerTest;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class InterArrivalTimeTrackerTest extends AbstractJokerTest
{

    private static final String OPERATOR_ID = "op";


    private LongSupplier timeGenerator = mock( LongSupplier.class );

    private DownstreamCollector downstream = mock( DownstreamCollector.class );

    private DownstreamCollector interArrivalTimeTracker = new InterArrivalTimeTracker( OPERATOR_ID, 5, 3, timeGenerator, downstream );

    private final Tuple tuple = new Tuple();

    private TuplesImpl tuples = new TuplesImpl( 1 );

    @Before
    public void init ()
    {
        tuples.add( tuple );
    }

    @Test
    public void test1 ()
    {
        interArrivalTimeTracker.accept( tuples );

        verify( timeGenerator, never() ).getAsLong();
        assertNull( tuple.getLatencyStages() );
    }

    @Test
    public void test2 ()
    {
        interArrivalTimeTracker.accept( tuples );
        interArrivalTimeTracker.accept( tuples );

        verify( timeGenerator, never() ).getAsLong();
        assertNull( tuple.getLatencyStages() );
    }

    @Test
    public void test3 ()
    {
        interArrivalTimeTracker.accept( tuples );
        interArrivalTimeTracker.accept( tuples );

        interArrivalTimeTracker.accept( tuples );

        verify( timeGenerator ).getAsLong();
        assertNull( tuple.getLatencyStages() );
    }

    @Test
    public void test4 ()
    {
        when( timeGenerator.getAsLong() ).thenReturn( 5L, 15L );

        interArrivalTimeTracker.accept( tuples );
        interArrivalTimeTracker.accept( tuples );

        interArrivalTimeTracker.accept( tuples );
        interArrivalTimeTracker.accept( tuples );

        verify( timeGenerator, times( 2 ) ).getAsLong();
        final List<LatencyStage> stages = tuple.getLatencyStages();
        assertNotNull( stages );
        assertThat( stages, hasSize( 1 ) );
        final LatencyStage stage = stages.get( 0 );
        assertThat( stage.getType(), equalTo( INTER_ARRIVAL_TIME ) );
        assertThat( stage.getOperatorId(), equalTo( OPERATOR_ID ) );
        assertThat( stage.getDuration(), equalTo( 10L ) );
    }

    @Test
    public void test5 ()
    {
        when( timeGenerator.getAsLong() ).thenReturn( 5L, 15L, 30L );

        interArrivalTimeTracker.accept( tuples );
        interArrivalTimeTracker.accept( tuples );

        interArrivalTimeTracker.accept( tuples );
        interArrivalTimeTracker.accept( tuples );
        interArrivalTimeTracker.accept( tuples );

        verify( timeGenerator, times( 3 ) ).getAsLong();
        final List<LatencyStage> stages = tuple.getLatencyStages();
        assertNotNull( stages );
        assertThat( stages, hasSize( 2 ) );
        final LatencyStage stage1 = stages.get( 0 );
        assertThat( stage1.getType(), equalTo( INTER_ARRIVAL_TIME ) );
        assertThat( stage1.getOperatorId(), equalTo( OPERATOR_ID ) );
        assertThat( stage1.getDuration(), equalTo( 10L ) );
        final LatencyStage stage2 = stages.get( 1 );
        assertThat( stage2.getType(), equalTo( INTER_ARRIVAL_TIME ) );
        assertThat( stage2.getOperatorId(), equalTo( OPERATOR_ID ) );
        assertThat( stage2.getDuration(), equalTo( 15L ) );
    }

    @Test
    public void test6 ()
    {
        when( timeGenerator.getAsLong() ).thenReturn( 5L, 15L, 30L, 50L );

        interArrivalTimeTracker.accept( tuples );
        interArrivalTimeTracker.accept( tuples );

        interArrivalTimeTracker.accept( tuples );
        interArrivalTimeTracker.accept( tuples );
        interArrivalTimeTracker.accept( tuples );
        interArrivalTimeTracker.accept( tuples );

        verify( timeGenerator, times( 4 ) ).getAsLong();
        final List<LatencyStage> stages = tuple.getLatencyStages();
        assertNotNull( stages );
        assertThat( stages, hasSize( 3 ) );
        final LatencyStage stage1 = stages.get( 0 );
        assertThat( stage1.getType(), equalTo( INTER_ARRIVAL_TIME ) );
        assertThat( stage1.getOperatorId(), equalTo( OPERATOR_ID ) );
        assertThat( stage1.getDuration(), equalTo( 10L ) );
        final LatencyStage stage2 = stages.get( 1 );
        assertThat( stage2.getType(), equalTo( INTER_ARRIVAL_TIME ) );
        assertThat( stage2.getOperatorId(), equalTo( OPERATOR_ID ) );
        assertThat( stage2.getDuration(), equalTo( 15L ) );
        final LatencyStage stage3 = stages.get( 2 );
        assertThat( stage3.getType(), equalTo( INTER_ARRIVAL_TIME ) );
        assertThat( stage3.getOperatorId(), equalTo( OPERATOR_ID ) );
        assertThat( stage3.getDuration(), equalTo( 20L ) );
    }

    @Test
    public void test7 ()
    {
        when( timeGenerator.getAsLong() ).thenReturn( 5L, 15L, 30L, 50L );

        interArrivalTimeTracker.accept( tuples );
        interArrivalTimeTracker.accept( tuples );

        interArrivalTimeTracker.accept( tuples );
        interArrivalTimeTracker.accept( tuples );
        interArrivalTimeTracker.accept( tuples );
        interArrivalTimeTracker.accept( tuples );

        interArrivalTimeTracker.accept( tuples );

        verify( timeGenerator, times( 4 ) ).getAsLong();
        final List<LatencyStage> stages = tuple.getLatencyStages();
        assertNotNull( stages );
        assertThat( stages, hasSize( 3 ) );
        final LatencyStage stage1 = stages.get( 0 );
        assertThat( stage1.getType(), equalTo( INTER_ARRIVAL_TIME ) );
        assertThat( stage1.getOperatorId(), equalTo( OPERATOR_ID ) );
        assertThat( stage1.getDuration(), equalTo( 10L ) );
        final LatencyStage stage2 = stages.get( 1 );
        assertThat( stage2.getType(), equalTo( INTER_ARRIVAL_TIME ) );
        assertThat( stage2.getOperatorId(), equalTo( OPERATOR_ID ) );
        assertThat( stage2.getDuration(), equalTo( 15L ) );
        final LatencyStage stage3 = stages.get( 2 );
        assertThat( stage3.getType(), equalTo( INTER_ARRIVAL_TIME ) );
        assertThat( stage3.getOperatorId(), equalTo( OPERATOR_ID ) );
        assertThat( stage3.getDuration(), equalTo( 20L ) );
    }

    @Test
    public void test8 ()
    {
        when( timeGenerator.getAsLong() ).thenReturn( 5L, 15L, 30L, 50L, 75L );

        interArrivalTimeTracker.accept( tuples );
        interArrivalTimeTracker.accept( tuples );

        interArrivalTimeTracker.accept( tuples );
        interArrivalTimeTracker.accept( tuples );
        interArrivalTimeTracker.accept( tuples );
        interArrivalTimeTracker.accept( tuples );

        interArrivalTimeTracker.accept( tuples );
        interArrivalTimeTracker.accept( tuples );

        interArrivalTimeTracker.accept( tuples );

        verify( timeGenerator, times( 5 ) ).getAsLong();
        final List<LatencyStage> stages = tuple.getLatencyStages();
        assertNotNull( stages );
        assertThat( stages, hasSize( 3 ) );
        final LatencyStage stage1 = stages.get( 0 );
        assertThat( stage1.getType(), equalTo( INTER_ARRIVAL_TIME ) );
        assertThat( stage1.getOperatorId(), equalTo( OPERATOR_ID ) );
        assertThat( stage1.getDuration(), equalTo( 10L ) );
        final LatencyStage stage2 = stages.get( 1 );
        assertThat( stage2.getType(), equalTo( INTER_ARRIVAL_TIME ) );
        assertThat( stage2.getOperatorId(), equalTo( OPERATOR_ID ) );
        assertThat( stage2.getDuration(), equalTo( 15L ) );
        final LatencyStage stage3 = stages.get( 2 );
        assertThat( stage3.getType(), equalTo( INTER_ARRIVAL_TIME ) );
        assertThat( stage3.getOperatorId(), equalTo( OPERATOR_ID ) );
        assertThat( stage3.getDuration(), equalTo( 20L ) );
    }

    @Test
    public void test9 ()
    {
        when( timeGenerator.getAsLong() ).thenReturn( 5L, 15L, 30L, 50L, 75L, 100L );

        interArrivalTimeTracker.accept( tuples );
        interArrivalTimeTracker.accept( tuples );

        interArrivalTimeTracker.accept( tuples );
        interArrivalTimeTracker.accept( tuples );
        interArrivalTimeTracker.accept( tuples );
        interArrivalTimeTracker.accept( tuples );

        interArrivalTimeTracker.accept( tuples );
        interArrivalTimeTracker.accept( tuples );

        interArrivalTimeTracker.accept( tuples );
        interArrivalTimeTracker.accept( tuples );

        verify( timeGenerator, times( 6 ) ).getAsLong();
        final List<LatencyStage> stages = tuple.getLatencyStages();
        assertNotNull( stages );
        assertThat( stages, hasSize( 4 ) );
        final LatencyStage stage1 = stages.get( 0 );
        assertThat( stage1.getType(), equalTo( INTER_ARRIVAL_TIME ) );
        assertThat( stage1.getOperatorId(), equalTo( OPERATOR_ID ) );
        assertThat( stage1.getDuration(), equalTo( 10L ) );
        final LatencyStage stage2 = stages.get( 1 );
        assertThat( stage2.getType(), equalTo( INTER_ARRIVAL_TIME ) );
        assertThat( stage2.getOperatorId(), equalTo( OPERATOR_ID ) );
        assertThat( stage2.getDuration(), equalTo( 15L ) );
        final LatencyStage stage3 = stages.get( 2 );
        assertThat( stage3.getType(), equalTo( INTER_ARRIVAL_TIME ) );
        assertThat( stage3.getOperatorId(), equalTo( OPERATOR_ID ) );
        assertThat( stage3.getDuration(), equalTo( 20L ) );
        final LatencyStage stage4 = stages.get( 3 );
        assertThat( stage4.getType(), equalTo( INTER_ARRIVAL_TIME ) );
        assertThat( stage4.getOperatorId(), equalTo( OPERATOR_ID ) );
        assertThat( stage4.getDuration(), equalTo( 25L ) );
    }

}
