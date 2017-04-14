package cs.bilkent.joker.engine.metric.impl;

import java.lang.management.ThreadMXBean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cs.bilkent.joker.engine.metric.PipelineMeter;
import static cs.bilkent.joker.engine.metric.PipelineMeter.NO_OPERATOR_INDEX;
import static cs.bilkent.joker.engine.metric.PipelineMeter.PIPELINE_EXECUTION_INDEX;
import cs.bilkent.joker.engine.metric.PipelineMetricsSnapshot;
import cs.bilkent.joker.engine.metric.PipelineMetricsSnapshot.PipelineMetricsSnapshotBuilder;
import static java.lang.Math.max;
import static java.lang.System.arraycopy;
import static java.util.Arrays.fill;

class PipelineMetrics
{

    private static final Logger LOGGER = LoggerFactory.getLogger( PipelineMetrics.class );


    private final int flowVersion;

    private final PipelineMeter pipelineMeter;

    // updated and read by metrics thread
    private final long[][] operatorSampleCounts;

    // updated and read by sampler thread, read by metrics thread
    private final long[][] operatorSampleCountsBuffer;

    // updated and read by metrics thread
    private final long[] pipelineSampleCounts;

    // updated by sampler thread, read by metrics thread
    private final long[] pipelineSampleCountsBuffer;

    // updated and read by metrics thread
    private final long[] threadCpuTimes;

    // updated and read by metrics thread
    private final long[][] inboundThroughputs;

    // sample counts buffers can be updated in the sampling thread and read in the metrics thread.
    // this field is used to provide happens-before relationship among these two threads.
    private volatile int sampling;

    PipelineMetrics ( final int flowVersion, final PipelineMeter pipelineMeter )
    {
        this.flowVersion = flowVersion;
        this.pipelineMeter = pipelineMeter;
        this.operatorSampleCounts = new long[ pipelineMeter.getReplicaCount() ][ pipelineMeter.getOperatorCount() ];
        this.operatorSampleCountsBuffer = new long[ pipelineMeter.getReplicaCount() ][ pipelineMeter.getOperatorCount() ];
        this.pipelineSampleCounts = new long[ pipelineMeter.getReplicaCount() ];
        this.pipelineSampleCountsBuffer = new long[ pipelineMeter.getReplicaCount() ];
        this.threadCpuTimes = new long[ pipelineMeter.getReplicaCount() ];
        this.inboundThroughputs = new long[ pipelineMeter.getReplicaCount() ][ pipelineMeter.getInputPortCount() ];
    }

    private PipelineMetricsSnapshotBuilder newPipelineMetricsSnapshotBuilder ()
    {
        final int replicaCount = pipelineMeter.getReplicaCount();
        final int operatorCount = pipelineMeter.getOperatorCount();
        final int inputPortCount = pipelineMeter.getInputPortCount();
        return new PipelineMetricsSnapshotBuilder( pipelineMeter.getPipelineId(),
                                                   flowVersion,
                                                   replicaCount,
                                                   operatorCount,
                                                   inputPortCount );
    }

    // called by sampler thread
    private void afterSampleCountsUpdate ()
    {
        sampling++;
    }

    // called by metrics thread
    private int beforeSampleCountsRead ()
    {
        return sampling;
    }

    // called by metrics thread
    void initialize ( final ThreadMXBean threadMXBean )
    {
        fill( this.pipelineSampleCounts, 0 );
        fill( this.pipelineSampleCountsBuffer, 0 );

        pipelineMeter.getThreadCpuTimes( threadMXBean, this.threadCpuTimes );

        for ( int replicaIndex = 0; replicaIndex < pipelineMeter.getReplicaCount(); replicaIndex++ )
        {
            fill( this.operatorSampleCounts[ replicaIndex ], 0 );
            fill( this.operatorSampleCountsBuffer[ replicaIndex ], 0 );
            fill( this.inboundThroughputs[ replicaIndex ], 0 );

            pipelineMeter.readInboundThroughput( replicaIndex, this.inboundThroughputs[ replicaIndex ] );
        }
    }

    PipelineMeter getPipelineMeter ()
    {
        return pipelineMeter;
    }

    int getFlowVersion ()
    {
        return flowVersion;
    }

    // called by metrics thread
    long[] getThreadCpuTimes ( final ThreadMXBean threadMXBean )
    {
        final long[] threadCpuTimes = new long[ pipelineMeter.getReplicaCount() ];
        pipelineMeter.getThreadCpuTimes( threadMXBean, threadCpuTimes );
        return threadCpuTimes;
    }

    // called by metrics thread
    PipelineMetricsSnapshot update ( final long[] newReplicaCpuTimes, final long systemTimeDiff )
    {
        final PipelineMetricsSnapshotBuilder builder = newPipelineMetricsSnapshotBuilder();
        updateThreadUtilizationRatios( newReplicaCpuTimes, systemTimeDiff, builder );
        updateCosts( builder );
        updateThroughputs( builder );

        return builder.build();
    }

    private void updateThreadUtilizationRatios ( final long[] newReplicaCpuTimes,
                                                 final long systemTimeDiff,
                                                 final PipelineMetricsSnapshotBuilder builder )
    {
        final int replicaCount = pipelineMeter.getReplicaCount();
        for ( int replicaIndex = 0; replicaIndex < replicaCount; replicaIndex++ )
        {
            final long cpuTimeDiff = newReplicaCpuTimes[ replicaIndex ] - this.threadCpuTimes[ replicaIndex ];
            final long threadCpuTimeDiff = max( 0, cpuTimeDiff );
            final double threadUtilizationRatio = ( (double) threadCpuTimeDiff ) / systemTimeDiff;
            builder.setCpuUtilizationRatio( replicaIndex, threadUtilizationRatio );
        }

        arraycopy( newReplicaCpuTimes, 0, this.threadCpuTimes, 0, replicaCount );
    }

    private void updateCosts ( final PipelineMetricsSnapshotBuilder builder )
    {
        beforeSampleCountsRead();

        final int replicaCount = pipelineMeter.getReplicaCount();
        final int operatorCount = pipelineMeter.getOperatorCount();

        for ( int replicaIndex = 0; replicaIndex < replicaCount; replicaIndex++ )
        {
            final long pipelineSampleCountBuffer = this.pipelineSampleCountsBuffer[ replicaIndex ];
            final long pipelineSampleCount = this.pipelineSampleCounts[ replicaIndex ];
            final long[] operatorSampleCountsBuffer = this.operatorSampleCountsBuffer[ replicaIndex ];
            final long[] operatorSampleCounts = this.operatorSampleCounts[ replicaIndex ];
            long sampleCountSum = ( pipelineSampleCountBuffer - pipelineSampleCount );
            for ( int operatorIndex = 0; operatorIndex < operatorCount; operatorIndex++ )
            {
                sampleCountSum += ( operatorSampleCountsBuffer[ operatorIndex ] - operatorSampleCounts[ operatorIndex ] );
            }

            if ( sampleCountSum > 0 )
            {
                final double pipelineCost = ( (double) ( pipelineSampleCountBuffer - pipelineSampleCount ) ) / sampleCountSum;
                builder.setPipelineCost( replicaIndex, pipelineCost );
                for ( int operatorIndex = 0; operatorIndex < operatorCount; operatorIndex++ )
                {
                    final long samplingDiff = operatorSampleCountsBuffer[ operatorIndex ] - operatorSampleCounts[ operatorIndex ];
                    final double operatorCost = ( (double) samplingDiff ) / sampleCountSum;
                    builder.setOperatorCost( replicaIndex, operatorIndex, operatorCost );
                }

                this.pipelineSampleCounts[ replicaIndex ] = pipelineSampleCountBuffer;
                arraycopy( operatorSampleCountsBuffer, 0, operatorSampleCounts, 0, operatorCount );
            }
            else
            {
                builder.setPipelineCost( replicaIndex, 0 );
                for ( int operatorIndex = 0; operatorIndex < operatorCount; operatorIndex++ )
                {
                    builder.setOperatorCost( replicaIndex, operatorIndex, 0 );
                }

                LOGGER.warn( "No sampling data for {}", pipelineMeter.getPipelineReplicaId( replicaIndex ) );
            }
        }
    }

    private void updateThroughputs ( final PipelineMetricsSnapshotBuilder builder )
    {
        final int replicaCount = pipelineMeter.getReplicaCount();
        final int inputPortCount = pipelineMeter.getInputPortCount();

        for ( int replicaIndex = 0; replicaIndex < replicaCount; replicaIndex++ )
        {
            final long[] newInboundThroughputs = new long[ inputPortCount ];

            pipelineMeter.readInboundThroughput( replicaIndex, newInboundThroughputs );

            final long[] currInboundThroughputs = this.inboundThroughputs[ replicaIndex ];

            for ( int portIndex = 0; portIndex < newInboundThroughputs.length; portIndex++ )
            {
                long throughput = newInboundThroughputs[ portIndex ] - currInboundThroughputs[ portIndex ];
                builder.setInboundThroughput( replicaIndex, portIndex, throughput );
            }

            arraycopy( newInboundThroughputs, 0, currInboundThroughputs, 0, inputPortCount );
        }
    }

    // called by sampler thread
    void sample ( final ThreadMXBean threadMXBean )
    {
        for ( int replicaIndex = 0; replicaIndex < pipelineMeter.getReplicaCount(); replicaIndex++ )
        {
            final int index = pipelineMeter.getCurrentlyExecutingComponentIndex( threadMXBean, replicaIndex );

            if ( index == NO_OPERATOR_INDEX )
            {
                continue;
            }

            if ( index == PIPELINE_EXECUTION_INDEX )
            {
                this.pipelineSampleCountsBuffer[ replicaIndex ]++;
            }
            else
            {
                this.operatorSampleCountsBuffer[ replicaIndex ][ index ]++;
            }
        }

        afterSampleCountsUpdate();
    }

}
