package cs.bilkent.joker.engine.metric.impl;

import java.lang.management.ThreadMXBean;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import cs.bilkent.joker.engine.flow.PipelineId;
import cs.bilkent.joker.engine.metric.PipelineMeter;
import static cs.bilkent.joker.engine.metric.PipelineMeter.PIPELINE_EXECUTION_INDEX;
import cs.bilkent.joker.engine.metric.PipelineMetrics;
import cs.bilkent.joker.test.AbstractJokerTest;
import static java.lang.System.arraycopy;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

@RunWith( MockitoJUnitRunner.class )
public class PipelineMetricsContextTest extends AbstractJokerTest
{

    private static final int FLOW_VERSION = 5;

    private static final int REPLICA_COUNT = 2;

    private static final int OPERATOR_COUNT = 3;

    private static final int INPUT_PORT_COUNT = 2;

    private static final PipelineId PIPELINE_ID = new PipelineId( 10, 20 );

    @Mock
    private ThreadMXBean threadMXBean;

    @Mock
    private PipelineMeter meter;

    private PipelineMetricsContext metricsCtx;

    @Before
    public void init ()
    {
        when( meter.getPipelineId() ).thenReturn( PIPELINE_ID );
        when( meter.getReplicaCount() ).thenReturn( REPLICA_COUNT );
        when( meter.getOperatorCount() ).thenReturn( OPERATOR_COUNT );
        when( meter.getPortCount() ).thenReturn( INPUT_PORT_COUNT );

        metricsCtx = new PipelineMetricsContext( FLOW_VERSION, meter );
    }

    @Test
    public void shouldPublishThreadCpuTimes ()
    {
        final long[] initialThreadCpuTimes = { 1, 2 };
        final long[] newThreadCpuTimes = { 2, 4 };
        final long systemTimeDiff = 10;
        doAnswer( invocation -> {
            final long[] arr = (long[]) invocation.getArguments()[ 1 ];
            arraycopy( initialThreadCpuTimes, 0, arr, 0, initialThreadCpuTimes.length );
            return null;
        } ).when( meter ).getThreadCpuTimes( anyObject(), anyObject() );

        metricsCtx.initialize( threadMXBean );

        final PipelineMetrics metrics = metricsCtx.update( newThreadCpuTimes, systemTimeDiff );

        for ( int replicaIndex = 0; replicaIndex < REPLICA_COUNT; replicaIndex++ )
        {
            final double expected =
                    ( ( (double) newThreadCpuTimes[ replicaIndex ] ) - initialThreadCpuTimes[ replicaIndex ] ) / systemTimeDiff;
            assertEquals( "replica index=" + replicaIndex, expected, metrics.getCpuUtilizationRatio( replicaIndex ), 0.01 );
        }
    }

    @Test
    public void shouldPublishThreadCpuTimesMultipleTimes ()
    {
        final long[] initialThreadCpuTimes = { 1, 2 };
        final long[] newThreadCpuTimes = { 2, 4 };
        final long[] newThreadCpuTimes2 = { 6, 10 };
        final long systemTimeDiff = 10;
        doAnswer( invocation -> {
            final long[] arr = (long[]) invocation.getArguments()[ 1 ];
            arraycopy( initialThreadCpuTimes, 0, arr, 0, initialThreadCpuTimes.length );
            return null;
        } ).when( meter ).getThreadCpuTimes( anyObject(), anyObject() );

        metricsCtx.initialize( threadMXBean );

        metricsCtx.update( newThreadCpuTimes, systemTimeDiff );
        final PipelineMetrics metrics = metricsCtx.update( newThreadCpuTimes2, systemTimeDiff );

        for ( int replicaIndex = 0; replicaIndex < REPLICA_COUNT; replicaIndex++ )
        {
            final double expected =
                    ( ( (double) newThreadCpuTimes2[ replicaIndex ] ) - newThreadCpuTimes[ replicaIndex ] ) / systemTimeDiff;
            assertEquals( "replica index=" + replicaIndex, expected, metrics.getCpuUtilizationRatio( replicaIndex ), 0.01 );
        }
    }

    @Test
    public void shouldPublishCosts ()
    {
        when( meter.getCurrentlyExecutingComponentIndex( threadMXBean, 0 ) ).thenReturn( PIPELINE_EXECUTION_INDEX,
                                                                                         PIPELINE_EXECUTION_INDEX,
                                                                                         PIPELINE_EXECUTION_INDEX,
                                                                                         PIPELINE_EXECUTION_INDEX,
                                                                                         0,
                                                                                         0,
                                                                                         0,
                                                                                         1,
                                                                                         1,
                                                                                         2 );

        when( meter.getCurrentlyExecutingComponentIndex( threadMXBean, 1 ) ).thenReturn( PIPELINE_EXECUTION_INDEX,
                                                                                         0,
                                                                                         0,
                                                                                         1,
                                                                                         1,
                                                                                         1,
                                                                                         2,
                                                                                         2,
                                                                                         2,
                                                                                         2 );

        for ( int i = 0; i < 10; i++ )
        {
            metricsCtx.sample( threadMXBean );
        }

        final PipelineMetrics metrics = metricsCtx.update( new long[ REPLICA_COUNT ], 10 );

        assertEquals( 0.4, metrics.getPipelineCost( 0 ), 0.01 );
        assertEquals( 0.3, metrics.getOperatorCost( 0, 0 ), 0.01 );
        assertEquals( 0.2, metrics.getOperatorCost( 0, 1 ), 0.01 );
        assertEquals( 0.1, metrics.getOperatorCost( 0, 2 ), 0.01 );
        assertEquals( 0.1, metrics.getPipelineCost( 1 ), 0.01 );
        assertEquals( 0.2, metrics.getOperatorCost( 1, 0 ), 0.01 );
        assertEquals( 0.3, metrics.getOperatorCost( 1, 1 ), 0.01 );
        assertEquals( 0.4, metrics.getOperatorCost( 1, 2 ), 0.01 );
    }

    @Test
    public void shouldPublishCostsMultipleTimes ()
    {
        when( meter.getCurrentlyExecutingComponentIndex( threadMXBean, 0 ) ).thenReturn( PIPELINE_EXECUTION_INDEX,
                                                                                         PIPELINE_EXECUTION_INDEX,
                                                                                         PIPELINE_EXECUTION_INDEX,
                                                                                         PIPELINE_EXECUTION_INDEX,
                                                                                         0,
                                                                                         0,
                                                                                         0,
                                                                                         1,
                                                                                         1,
                                                                                         2,
                                                                                         PIPELINE_EXECUTION_INDEX,
                                                                                         0,
                                                                                         0,
                                                                                         1,
                                                                                         1,
                                                                                         1,
                                                                                         2,
                                                                                         2,
                                                                                         2,
                                                                                         2 );

        when( meter.getCurrentlyExecutingComponentIndex( threadMXBean, 1 ) ).thenReturn( PIPELINE_EXECUTION_INDEX,
                                                                                         0,
                                                                                         0,
                                                                                         1,
                                                                                         1,
                                                                                         1,
                                                                                         2,
                                                                                         2,
                                                                                         2,
                                                                                         2,
                                                                                         PIPELINE_EXECUTION_INDEX,
                                                                                         PIPELINE_EXECUTION_INDEX,
                                                                                         PIPELINE_EXECUTION_INDEX,
                                                                                         PIPELINE_EXECUTION_INDEX,
                                                                                         0,
                                                                                         0,
                                                                                         0,
                                                                                         1,
                                                                                         1,
                                                                                         2 );

        for ( int i = 0; i < 10; i++ )
        {
            metricsCtx.sample( threadMXBean );
        }

        metricsCtx.update( new long[ REPLICA_COUNT ], 10 );

        for ( int i = 0; i < 10; i++ )
        {
            metricsCtx.sample( threadMXBean );
        }

        final PipelineMetrics metrics = metricsCtx.update( new long[ REPLICA_COUNT ], 10 );

        assertEquals( 0.1, metrics.getPipelineCost( 0 ), 0.01 );
        assertEquals( 0.2, metrics.getOperatorCost( 0, 0 ), 0.01 );
        assertEquals( 0.3, metrics.getOperatorCost( 0, 1 ), 0.01 );
        assertEquals( 0.4, metrics.getOperatorCost( 0, 2 ), 0.01 );
        assertEquals( 0.4, metrics.getPipelineCost( 1 ), 0.01 );
        assertEquals( 0.3, metrics.getOperatorCost( 1, 0 ), 0.01 );
        assertEquals( 0.2, metrics.getOperatorCost( 1, 1 ), 0.01 );
        assertEquals( 0.1, metrics.getOperatorCost( 1, 2 ), 0.01 );
    }

}
