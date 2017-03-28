package cs.bilkent.joker.engine.metric.impl;

import java.lang.management.ThreadMXBean;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Joiner;

import cs.bilkent.joker.engine.flow.PipelineId;
import cs.bilkent.joker.engine.metric.PipelineMeter;
import static cs.bilkent.joker.engine.metric.PipelineMeter.NO_OPERATOR_INDEX;
import static cs.bilkent.joker.engine.metric.PipelineMeter.PIPELINE_EXECUTION_INDEX;
import cs.bilkent.joker.engine.pipeline.PipelineReplicaId;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.System.arraycopy;
import static java.util.Arrays.asList;
import static java.util.Arrays.fill;
import static java.util.Collections.addAll;

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

    private volatile PipelineMetricsHistory history;

    PipelineMetrics ( final int flowVersion, final PipelineMeter pipelineMeter, final int historySize )
    {
        this.flowVersion = flowVersion;
        this.pipelineMeter = pipelineMeter;
        this.operatorSampleCounts = new long[ pipelineMeter.getReplicaCount() ][ pipelineMeter.getOperatorCount() ];
        this.operatorSampleCountsBuffer = new long[ pipelineMeter.getReplicaCount() ][ pipelineMeter.getOperatorCount() ];
        this.pipelineSampleCounts = new long[ pipelineMeter.getReplicaCount() ];
        this.pipelineSampleCountsBuffer = new long[ pipelineMeter.getReplicaCount() ];
        this.threadCpuTimes = new long[ pipelineMeter.getReplicaCount() ];
        this.inboundThroughputs = new long[ pipelineMeter.getReplicaCount() ][ pipelineMeter.getInputPortCount() ];
        this.history = new PipelineMetricsHistory( newPipelineMetricsSnapshot(), historySize );
    }

    private PipelineMetricsSnapshot newPipelineMetricsSnapshot ()
    {
        final int replicaCount = pipelineMeter.getReplicaCount();
        final int operatorCount = pipelineMeter.getOperatorCount();
        final int inputPortCount = pipelineMeter.getInputPortCount();
        return new PipelineMetricsSnapshot( replicaCount, operatorCount, inputPortCount );
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

        this.history = new PipelineMetricsHistory( newPipelineMetricsSnapshot(), this.history.historySize() );
    }

    void register ( final MetricRegistry metricRegistry )
    {
        final PipelineId pipelineId = pipelineMeter.getPipelineId();

        for ( int replicaIndex = 0; replicaIndex < pipelineMeter.getReplicaCount(); replicaIndex++ )
        {
            final int r = replicaIndex;
            final Supplier<Double> cpuGauge = () -> getRecentSnapshot().getCpuUtilizationRatio( r );
            metricRegistry.register( getMetricName( replicaIndex, "cpu" ), new PipelineGauge<>( pipelineId, cpuGauge ) );

            final Supplier<Double> pipelineCostGauge = () -> getRecentSnapshot().getPipelineCost( r );
            metricRegistry.register( getMetricName( replicaIndex, "cost", "p" ), new PipelineGauge<>( pipelineId, pipelineCostGauge ) );

            for ( int operatorIndex = 0; operatorIndex < pipelineMeter.getOperatorCount(); operatorIndex++ )
            {
                final int o = operatorIndex;
                final Supplier<Double> operatorCostGauge = () -> getRecentSnapshot().getOperatorCost( r, o );
                metricRegistry.register( getMetricName( replicaIndex, "cost", "op", operatorIndex ),
                                         new PipelineGauge<>( pipelineId, operatorCostGauge ) );
            }

            for ( int portIndex = 0; portIndex < pipelineMeter.getInputPortCount(); portIndex++ )
            {
                final int p = portIndex;
                final Supplier<Long> throughputGauge = () -> getRecentSnapshot().getInboundThroughput( r, p );
                metricRegistry.register( getMetricName( replicaIndex, "thr", "cs", portIndex ),
                                         new PipelineGauge<>( pipelineId, throughputGauge ) );
            }
        }
    }

    void deregister ( final MetricRegistry metricRegistry )
    {
        metricRegistry.removeMatching( ( name, metric ) ->
                                       {
                                           if ( metric instanceof PipelineGauge )
                                           {
                                               final PipelineGauge gauge = (PipelineGauge) metric;

                                               return gauge.id.equals( pipelineMeter.getPipelineId() );
                                           }

                                           return false;
                                       } );
    }

    private String getMetricName ( final int replicaIndex, Object... vals )
    {
        final List<Object> parts = new ArrayList<>();
        parts.add( "r" );
        parts.add( pipelineMeter.getPipelineId().getRegionId() );
        parts.add( "p" );
        parts.add( pipelineMeter.getPipelineId().getPipelineStartIndex() );
        parts.add( "r" );
        parts.add( replicaIndex );
        parts.add( "f" );
        parts.add( flowVersion );
        if ( vals != null )
        {
            addAll( parts, vals );
        }

        return Joiner.on( "_" ).join( parts );
    }

    // called by metrics thread
    long[] getThreadCpuTimes ( final ThreadMXBean threadMXBean )
    {
        final long[] threadCpuTimes = new long[ pipelineMeter.getReplicaCount() ];
        pipelineMeter.getThreadCpuTimes( threadMXBean, threadCpuTimes );
        return threadCpuTimes;
    }

    // called by metrics thread
    void update ( final long[] newReplicaCpuTimes, final long systemTimeDiff )
    {
        final PipelineMetricsSnapshot snapshot = newPipelineMetricsSnapshot();
        updateThreadUtilizationRatios( newReplicaCpuTimes, systemTimeDiff, snapshot );
        updateCosts( snapshot );
        updateThroughputs( snapshot );
        final PipelineMetricsHistory history = this.history;
        this.history = history.add( snapshot );
    }

    private void updateThreadUtilizationRatios ( final long[] newReplicaCpuTimes,
                                                 final long systemTimeDiff,
                                                 final PipelineMetricsSnapshot snapshot )
    {
        final int replicaCount = pipelineMeter.getReplicaCount();
        for ( int replicaIndex = 0; replicaIndex < replicaCount; replicaIndex++ )
        {
            final long cpuTimeDiff = newReplicaCpuTimes[ replicaIndex ] - this.threadCpuTimes[ replicaIndex ];
            final long threadCpuTimeDiff = max( 0, cpuTimeDiff );
            final double threadUtilizationRatio = ( (double) threadCpuTimeDiff ) / systemTimeDiff;
            snapshot.cpuUtilizationRatios[ replicaIndex ] = min( threadUtilizationRatio, 1d );
        }

        arraycopy( newReplicaCpuTimes, 0, this.threadCpuTimes, 0, replicaCount );
    }

    private void updateCosts ( final PipelineMetricsSnapshot snapshot )
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
                snapshot.pipelineCosts[ replicaIndex ] = ( (double) ( pipelineSampleCountBuffer - pipelineSampleCount ) ) / sampleCountSum;
                for ( int operatorIndex = 0; operatorIndex < operatorCount; operatorIndex++ )
                {
                    final long samplingDiff = operatorSampleCountsBuffer[ operatorIndex ] - operatorSampleCounts[ operatorIndex ];
                    snapshot.operatorCosts[ replicaIndex ][ operatorIndex ] = ( (double) samplingDiff ) / sampleCountSum;
                }

                this.pipelineSampleCounts[ replicaIndex ] = pipelineSampleCountBuffer;
                arraycopy( operatorSampleCountsBuffer, 0, operatorSampleCounts, 0, operatorCount );
            }
            else
            {
                snapshot.pipelineCosts[ replicaIndex ] = 0;
                for ( int operatorIndex = 0; operatorIndex < operatorCount; operatorIndex++ )
                {
                    snapshot.operatorCosts[ replicaIndex ][ operatorIndex ] = 0;
                }

                LOGGER.warn( "No sampling data for {}", pipelineMeter.getPipelineReplicaId( replicaIndex ) );
            }
        }
    }

    private void updateThroughputs ( final PipelineMetricsSnapshot snapshot )
    {
        final int replicaCount = pipelineMeter.getReplicaCount();
        final int inputPortCount = pipelineMeter.getInputPortCount();

        for ( int replicaIndex = 0; replicaIndex < replicaCount; replicaIndex++ )
        {
            final long[] newInboundThroughputs = new long[ inputPortCount ];

            pipelineMeter.readInboundThroughput( replicaIndex, newInboundThroughputs );

            final long[] currInboundThroughputs = this.inboundThroughputs[ replicaIndex ];

            for ( int i = 0; i < newInboundThroughputs.length; i++ )
            {
                snapshot.inboundThroughputs[ replicaIndex ][ i ] = newInboundThroughputs[ i ] - currInboundThroughputs[ i ];
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

    void visit ( final PipelineMetricsVisitor visitor )
    {
        final PipelineMetricsSnapshot snapshot = getRecentSnapshot();
        for ( int replicaIndex = 0; replicaIndex < pipelineMeter.getReplicaCount(); replicaIndex++ )
        {
            final PipelineReplicaId pipelineReplicaId = pipelineMeter.getPipelineReplicaId( replicaIndex );
            final long[] inboundThroughput = snapshot.inboundThroughputs[ replicaIndex ];
            final double threadUtilizationRatio = snapshot.cpuUtilizationRatios[ replicaIndex ];
            final double pipelineCost = snapshot.pipelineCosts[ replicaIndex ];
            final double[] operatorCosts = snapshot.operatorCosts[ replicaIndex ];

            visitor.handle( pipelineReplicaId, flowVersion, inboundThroughput, threadUtilizationRatio, pipelineCost, operatorCosts );
        }
    }

    PipelineMetricsSnapshot getRecentSnapshot ()
    {
        return history.getRecentSnapshot();
    }

    List<PipelineMetricsSnapshot> getHistory ()
    {
        return history.getSnapshots();
    }

    static class PipelineMetricsSnapshot
    {

        private final int replicaCount;

        private final int operatorCount;

        private final int inputPortCount;

        private final double[] cpuUtilizationRatios;

        private final long[][] inboundThroughputs;

        private final double[][] operatorCosts;

        private final double[] pipelineCosts;

        PipelineMetricsSnapshot ( final int replicaCount, final int operatorCount, final int inputPortCount )
        {
            this.replicaCount = replicaCount;
            this.operatorCount = operatorCount;
            this.inputPortCount = inputPortCount;
            this.cpuUtilizationRatios = new double[ replicaCount ];
            this.inboundThroughputs = new long[ replicaCount ][ inputPortCount ];
            this.operatorCosts = new double[ replicaCount ][ operatorCount ];
            this.pipelineCosts = new double[ replicaCount ];
        }

        int getReplicaCount ()
        {
            return replicaCount;
        }

        int getOperatorCount ()
        {
            return operatorCount;
        }

        int getInputPortCount ()
        {
            return inputPortCount;
        }

        double getPipelineCost ( final int replicaIndex )
        {
            return pipelineCosts[ replicaIndex ];
        }

        double getOperatorCost ( final int replicaIndex, final int operatorIndex )
        {
            return operatorCosts[ replicaIndex ][ operatorIndex ];
        }

        double getCpuUtilizationRatio ( final int replicaIndex )
        {
            return cpuUtilizationRatios[ replicaIndex ];
        }

        long getInboundThroughput ( final int replicaIndex, final int portIndex )
        {
            return inboundThroughputs[ replicaIndex ][ portIndex ];
        }

    }


    interface PipelineMetricsVisitor
    {

        void handle ( PipelineReplicaId pipelineReplicaId, int flowVersion, long[] inboundThroughput,
                      double threadUtilizationRatio,
                      double pipelineCost,
                      double[] operatorCosts );

    }


    static class PipelineGauge<T> implements Gauge<T>
    {

        private final PipelineId id;

        private final Supplier<T> gauge;

        PipelineGauge ( final PipelineId id, final Supplier<T> gauge )
        {
            this.id = id;
            this.gauge = gauge;
        }

        @Override
        public T getValue ()
        {
            return gauge.get();
        }

    }


    public static class PipelineMetricsHistory
    {

        private final PipelineMetricsSnapshot initial;

        private final PipelineMetricsSnapshot[] snapshots;

        private final int count;

        PipelineMetricsHistory ( final PipelineMetricsSnapshot initial, final int historySize )
        {
            this( initial, new PipelineMetricsSnapshot[ historySize ], 0 );
        }

        private PipelineMetricsHistory ( final PipelineMetricsSnapshot initial, final PipelineMetricsSnapshot[] snapshots, final int count )
        {
            this.initial = initial;
            this.snapshots = snapshots;
            this.count = count;
        }

        public PipelineMetricsHistory add ( final PipelineMetricsSnapshot snapshot )
        {
            final PipelineMetricsSnapshot[] snapshots = new PipelineMetricsSnapshot[ this.snapshots.length ];
            snapshots[ 0 ] = snapshot;
            final int length = max( 0, min( this.count, this.snapshots.length - 1 ) );
            arraycopy( this.snapshots, 0, snapshots, 1, length );

            return new PipelineMetricsHistory( initial, snapshots, length + 1 );
        }

        PipelineMetricsSnapshot getRecentSnapshot ()
        {
            return count > 0 ? snapshots[ 0 ] : initial;
        }

        int getCount ()
        {
            return count;
        }

        int historySize ()
        {
            return snapshots.length;
        }

        List<PipelineMetricsSnapshot> getSnapshots ()
        {
            return asList( snapshots ).subList( 0, count );
        }

    }

}
