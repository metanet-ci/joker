package cs.bilkent.joker.engine.metric.impl;

import org.junit.Test;

import cs.bilkent.joker.engine.metric.PipelineMetrics;
import cs.bilkent.joker.engine.metric.PipelineMetricsHistory;
import cs.bilkent.joker.test.AbstractJokerTest;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

public class PipelineMetricsHistoryTest extends AbstractJokerTest
{

    @Test
    public void shouldGetRecentMetricsWith1EmptyHistorySlot ()
    {
        final PipelineMetrics metrics1 = mock( PipelineMetrics.class );
        final PipelineMetrics metrics2 = mock( PipelineMetrics.class );
        final int historySize = 1;

        final PipelineMetricsHistory history1 = new PipelineMetricsHistory( metrics1, historySize );
        final PipelineMetricsHistory history2 = history1.add( metrics2 );

        assertEquals( metrics1, history1.getLatest() );
        assertEquals( 1, history1.getCount() );
        assertEquals( singletonList( metrics1 ), history1.getAll() );

        assertEquals( metrics2, history2.getLatest() );
        assertEquals( 1, history2.getCount() );
        assertEquals( singletonList( metrics2 ), history2.getAll() );
    }

    @Test
    public void shouldGetRecentMetricsWith2EmptyHistorySlots ()
    {
        final PipelineMetrics metrics1 = mock( PipelineMetrics.class );
        final int historySize = 2;

        final PipelineMetricsHistory history = new PipelineMetricsHistory( metrics1, historySize );

        assertEquals( metrics1, history.getLatest() );
        assertEquals( 1, history.getCount() );
        assertEquals( singletonList( metrics1 ), history.getAll() );
    }

    @Test
    public void shouldGetRecentMetricsWithSlidedFullHistory1Slot ()
    {
        final PipelineMetrics metrics1 = mock( PipelineMetrics.class );
        final PipelineMetrics metrics2 = mock( PipelineMetrics.class );
        final PipelineMetrics metrics3 = mock( PipelineMetrics.class );
        final int historySize = 2;
        final PipelineMetricsHistory history1 = new PipelineMetricsHistory( metrics1, historySize );
        final PipelineMetricsHistory history2 = history1.add( metrics2 );
        final PipelineMetricsHistory history3 = history2.add( metrics3 );

        assertEquals( metrics1, history1.getLatest() );
        assertEquals( 1, history1.getCount() );
        assertEquals( singletonList( metrics1 ), history1.getAll() );

        assertEquals( metrics2, history2.getLatest() );
        assertEquals( 2, history2.getCount() );
        assertEquals( asList( metrics2, metrics1 ), history2.getAll() );

        assertEquals( metrics3, history3.getLatest() );
        assertEquals( 2, history3.getCount() );
        assertEquals( asList( metrics3, metrics2 ), history3.getAll() );
    }

}
