package cs.bilkent.joker.engine.metric.impl;

import org.junit.Test;

import cs.bilkent.joker.engine.metric.PipelineMetricsHistory;
import cs.bilkent.joker.engine.metric.PipelineMetricsSnapshot;
import cs.bilkent.joker.test.AbstractJokerTest;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

public class PipelineMetricsHistoryTest extends AbstractJokerTest
{

    @Test
    public void shouldGetRecentSnapshotWith1EmptyHistorySlot ()
    {
        final PipelineMetricsSnapshot snapshot1 = mock( PipelineMetricsSnapshot.class );
        final PipelineMetricsSnapshot snapshot2 = mock( PipelineMetricsSnapshot.class );
        final int historySize = 1;

        final PipelineMetricsHistory history1 = new PipelineMetricsHistory( snapshot1, historySize );
        final PipelineMetricsHistory history2 = history1.add( snapshot2 );

        assertEquals( snapshot1, history1.getLatestSnapshot() );
        assertEquals( 1, history1.getCount() );
        assertEquals( singletonList( snapshot1 ), history1.getSnapshots() );

        assertEquals( snapshot2, history2.getLatestSnapshot() );
        assertEquals( 1, history2.getCount() );
        assertEquals( singletonList( snapshot2 ), history2.getSnapshots() );
    }

    @Test
    public void shouldGetRecentSnapshotWith2EmptyHistorySlots ()
    {
        final PipelineMetricsSnapshot snapshot1 = mock( PipelineMetricsSnapshot.class );
        final int historySize = 2;

        final PipelineMetricsHistory history = new PipelineMetricsHistory( snapshot1, historySize );

        assertEquals( snapshot1, history.getLatestSnapshot() );
        assertEquals( 1, history.getCount() );
        assertEquals( singletonList( snapshot1 ), history.getSnapshots() );
    }

    @Test
    public void shouldGetRecentSnapshotWithSlidedFullHistory1Slot ()
    {
        final PipelineMetricsSnapshot snapshot1 = mock( PipelineMetricsSnapshot.class );
        final PipelineMetricsSnapshot snapshot2 = mock( PipelineMetricsSnapshot.class );
        final PipelineMetricsSnapshot snapshot3 = mock( PipelineMetricsSnapshot.class );
        final int historySize = 2;
        final PipelineMetricsHistory history1 = new PipelineMetricsHistory( snapshot1, historySize );
        final PipelineMetricsHistory history2 = history1.add( snapshot2 );
        final PipelineMetricsHistory history3 = history2.add( snapshot3 );

        assertEquals( snapshot1, history1.getLatestSnapshot() );
        assertEquals( 1, history1.getCount() );
        assertEquals( singletonList( snapshot1 ), history1.getSnapshots() );

        assertEquals( snapshot2, history2.getLatestSnapshot() );
        assertEquals( 2, history2.getCount() );
        assertEquals( asList( snapshot2, snapshot1 ), history2.getSnapshots() );

        assertEquals( snapshot3, history3.getLatestSnapshot() );
        assertEquals( 2, history3.getCount() );
        assertEquals( asList( snapshot3, snapshot2 ), history3.getSnapshots() );
    }

}
