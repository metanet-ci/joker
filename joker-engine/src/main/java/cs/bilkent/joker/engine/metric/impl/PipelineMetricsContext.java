package cs.bilkent.joker.engine.metric.impl;

import java.lang.management.ThreadMXBean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cs.bilkent.joker.engine.metric.PipelineMeter;
import static cs.bilkent.joker.engine.metric.PipelineMeter.NO_OPERATOR_INDEX;
import static cs.bilkent.joker.engine.metric.PipelineMeter.PIPELINE_EXECUTION_INDEX;
import cs.bilkent.joker.engine.metric.PipelineMetrics;
import cs.bilkent.joker.engine.metric.PipelineMetrics.PipelineMetricsBuilder;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.System.arraycopy;
import static java.util.Arrays.fill;

class PipelineMetricsContext
{

    private static final Logger LOGGER = LoggerFactory.getLogger( PipelineMetricsContext.class );


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
    private final long[][] throughputs;

    // sample counts buffers can be updated in the sampling thread and read in the metrics thread.
    // this field is used to provide happens-before relationship among these two threads.
    private volatile int sampling;

    PipelineMetricsContext ( final int flowVersion, final PipelineMeter pipelineMeter )
    {
        this.flowVersion = flowVersion;
        this.pipelineMeter = pipelineMeter;
        final int replicaCount = pipelineMeter.getReplicaCount();
        final int operatorCount = pipelineMeter.getOperatorCount();
        this.operatorSampleCounts = new long[ replicaCount ][ operatorCount ];
        this.operatorSampleCountsBuffer = new long[ replicaCount ][ operatorCount ];
        this.pipelineSampleCounts = new long[ replicaCount ];
        this.pipelineSampleCountsBuffer = new long[ replicaCount ];
        this.threadCpuTimes = new long[ replicaCount ];
        this.throughputs = new long[ replicaCount ][ pipelineMeter.getPortCount() ];
    }

    private PipelineMetricsBuilder newPipelineMetricsBuilder ()
    {
        final int replicaCount = pipelineMeter.getReplicaCount();
        final int operatorCount = pipelineMeter.getOperatorCount();
        final int portCount = pipelineMeter.getPortCount();
        return new PipelineMetricsBuilder( pipelineMeter.getPipelineId(), flowVersion, replicaCount, operatorCount, portCount );
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
            fill( this.throughputs[ replicaIndex ], 0 );

            pipelineMeter.readThroughput( replicaIndex, this.throughputs[ replicaIndex ] );
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
    PipelineMetrics update ( final long[] newReplicaCpuTimes, final long systemTimeDiff )
    {
        final PipelineMetricsBuilder builder = newPipelineMetricsBuilder();
        updateThreadUtilizationRatios( newReplicaCpuTimes, systemTimeDiff, builder );
        updateCosts( builder );
        updateThroughputs( builder );

        return builder.build();
    }

    private void updateThreadUtilizationRatios ( final long[] newReplicaCpuTimes,
                                                 final long systemTimeDiff,
                                                 final PipelineMetricsBuilder builder )
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

    private void updateCosts ( final PipelineMetricsBuilder builder )
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
                final double pipelineCost = min( 1d, ( (double) ( pipelineSampleCountBuffer - pipelineSampleCount ) ) / sampleCountSum );
                builder.setPipelineCost( replicaIndex, pipelineCost );
                for ( int operatorIndex = 0; operatorIndex < operatorCount; operatorIndex++ )
                {
                    final long samplingDiff = operatorSampleCountsBuffer[ operatorIndex ] - operatorSampleCounts[ operatorIndex ];
                    final double operatorCost = min( 1d, ( (double) samplingDiff ) / sampleCountSum );
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

    private void updateThroughputs ( final PipelineMetricsBuilder builder )
    {
        final int replicaCount = pipelineMeter.getReplicaCount();
        final int portCount = pipelineMeter.getPortCount();

        for ( int replicaIndex = 0; replicaIndex < replicaCount; replicaIndex++ )
        {
            final long[] newThroughputs = new long[ portCount ];

            pipelineMeter.readThroughput( replicaIndex, newThroughputs );

            final long[] currentThroughputs = this.throughputs[ replicaIndex ];

            for ( int portIndex = 0; portIndex < newThroughputs.length; portIndex++ )
            {
                long throughput = newThroughputs[ portIndex ] - currentThroughputs[ portIndex ];
                builder.setThroughput( replicaIndex, portIndex, throughput );
            }

            arraycopy( newThroughputs, 0, currentThroughputs, 0, portCount );
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
