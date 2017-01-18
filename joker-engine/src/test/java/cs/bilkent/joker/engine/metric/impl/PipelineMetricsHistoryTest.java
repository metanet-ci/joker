package cs.bilkent.joker.engine.metric.impl;

import org.junit.Test;

import cs.bilkent.joker.engine.metric.impl.PipelineMetrics.PipelineMetricsHistory;
import cs.bilkent.joker.engine.metric.impl.PipelineMetrics.PipelineMetricsSnapshot;
import cs.bilkent.joker.test.AbstractJokerTest;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

public class PipelineMetricsHistoryTest extends AbstractJokerTest
{


    @Test
    public void shouldNotGetRecentSnapshotWith1EmptyHistorySlot ()
    {
        final PipelineMetricsSnapshot initial = mock( PipelineMetricsSnapshot.class );
        final int historySize = 1;

        final PipelineMetricsHistory history = new PipelineMetricsHistory( initial, historySize );

        assertEquals( initial, history.getRecentSnapshot() );
        assertEquals( 0, history.getCount() );
        assertEquals( emptyList(), history.getSnapshots() );
    }

    @Test
    public void shouldNotGetRecentSnapshotWith2EmptyHistorySlots ()
    {
        final PipelineMetricsSnapshot initial = mock( PipelineMetricsSnapshot.class );
        final int historySize = 2;

        final PipelineMetricsHistory history = new PipelineMetricsHistory( initial, historySize );

        assertEquals( initial, history.getRecentSnapshot() );
        assertEquals( 0, history.getCount() );
        assertEquals( emptyList(), history.getSnapshots() );
    }

    @Test
    public void shouldGetRecentSnapshotWithFullHistory1Slot ()
    {
        final PipelineMetricsSnapshot initial = mock( PipelineMetricsSnapshot.class );
        final PipelineMetricsSnapshot snapshot1 = mock( PipelineMetricsSnapshot.class );
        final int historySize = 1;
        final PipelineMetricsHistory history = new PipelineMetricsHistory( initial, historySize );

        final PipelineMetricsHistory newHistory = history.add( snapshot1 );

        assertEquals( snapshot1, newHistory.getRecentSnapshot() );
        assertEquals( 1, newHistory.getCount() );
        assertEquals( singletonList( snapshot1 ), newHistory.getSnapshots() );
    }

    @Test
    public void shouldGetRecentSnapshotWithSlidedFullHistory1Slot ()
    {
        final PipelineMetricsSnapshot initial = mock( PipelineMetricsSnapshot.class );
        final PipelineMetricsSnapshot snapshot1 = mock( PipelineMetricsSnapshot.class );
        final PipelineMetricsSnapshot snapshot2 = mock( PipelineMetricsSnapshot.class );
        final int historySize = 1;
        final PipelineMetricsHistory history = new PipelineMetricsHistory( initial, historySize );

        final PipelineMetricsHistory newHistory = history.add( snapshot1 ).add( snapshot2 );

        assertEquals( snapshot2, newHistory.getRecentSnapshot() );
        assertEquals( 1, newHistory.getCount() );
        assertEquals( singletonList( snapshot2 ), newHistory.getSnapshots() );
    }

    @Test
    public void shouldGetRecentSnapshotWithNonFullHistory2Slots ()
    {
        final PipelineMetricsSnapshot initial = mock( PipelineMetricsSnapshot.class );
        final PipelineMetricsSnapshot snapshot1 = mock( PipelineMetricsSnapshot.class );
        final int historySize = 2;
        final PipelineMetricsHistory history = new PipelineMetricsHistory( initial, historySize );

        final PipelineMetricsHistory newHistory = history.add( snapshot1 );

        assertEquals( snapshot1, newHistory.getRecentSnapshot() );
        assertEquals( 1, newHistory.getCount() );
        assertEquals( singletonList( snapshot1 ), newHistory.getSnapshots() );
    }

    @Test
    public void shouldGetRecentSnapshotWithFullHistory2Slots ()
    {
        final PipelineMetricsSnapshot initial = mock( PipelineMetricsSnapshot.class );
        final PipelineMetricsSnapshot snapshot1 = mock( PipelineMetricsSnapshot.class );
        final PipelineMetricsSnapshot snapshot2 = mock( PipelineMetricsSnapshot.class );
        final int historySize = 2;
        final PipelineMetricsHistory history = new PipelineMetricsHistory( initial, historySize );

        final PipelineMetricsHistory newHistory = history.add( snapshot1 ).add( snapshot2 );

        assertEquals( snapshot2, newHistory.getRecentSnapshot() );
        assertEquals( 2, newHistory.getCount() );
        assertEquals( asList( snapshot2, snapshot1 ), newHistory.getSnapshots() );
    }

    @Test
    public void shouldGetRecentSnapshotWithSlidedFullHistory2Slots ()
    {
        final PipelineMetricsSnapshot initial = mock( PipelineMetricsSnapshot.class );
        final PipelineMetricsSnapshot snapshot1 = mock( PipelineMetricsSnapshot.class );
        final PipelineMetricsSnapshot snapshot2 = mock( PipelineMetricsSnapshot.class );
        final PipelineMetricsSnapshot snapshot3 = mock( PipelineMetricsSnapshot.class );
        final int historySize = 2;
        final PipelineMetricsHistory history = new PipelineMetricsHistory( initial, historySize );

        final PipelineMetricsHistory newHistory = history.add( snapshot1 ).add( snapshot2 ).add( snapshot3 );

        assertEquals( snapshot3, newHistory.getRecentSnapshot() );
        assertEquals( 2, newHistory.getCount() );
        assertEquals( asList( snapshot3, snapshot2 ), newHistory.getSnapshots() );
    }

}
