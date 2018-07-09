package cs.bilkent.joker.engine.metric;

import java.util.Arrays;
import javax.annotation.concurrent.NotThreadSafe;

import com.codahale.metrics.Snapshot;

import cs.bilkent.joker.engine.flow.PipelineId;
import cs.bilkent.joker.engine.pipeline.PipelineReplicaId;
import static cs.bilkent.joker.impl.com.google.common.base.Preconditions.checkArgument;
import static java.lang.Double.NaN;
import static java.lang.Math.min;
import static java.lang.System.arraycopy;
import static java.util.Arrays.stream;
import static java.util.stream.IntStream.range;

public class PipelineMetrics
{

    private final PipelineId pipelineId;

    private final int flowVersion;

    private final int replicaCount;

    private final int operatorCount;

    private final int inputPortCount;

    private final double[] cpuUtilizationRatios;

    private final long[][] inboundThroughputs;

    private final double[][] operatorCosts;

    private final double[] pipelineCosts;

    private final Snapshot[][] inboundThroughputHistograms;

    public PipelineMetrics ( final PipelineId pipelineId,
                             final int flowVersion,
                             final int replicaCount,
                             final int operatorCount,
                             final int inputPortCount )
    {
        this.pipelineId = pipelineId;
        this.flowVersion = flowVersion;
        this.replicaCount = replicaCount;
        this.operatorCount = operatorCount;
        this.inputPortCount = inputPortCount;
        this.cpuUtilizationRatios = new double[ replicaCount ];
        this.inboundThroughputs = new long[ replicaCount ][ inputPortCount ];
        this.operatorCosts = new double[ replicaCount ][ operatorCount ];
        this.pipelineCosts = new double[ replicaCount ];
        this.inboundThroughputHistograms = new Snapshot[ replicaCount ][ inputPortCount ];
    }

    public PipelineId getPipelineId ()
    {
        return pipelineId;
    }

    public int getFlowVersion ()
    {
        return flowVersion;
    }

    public int getReplicaCount ()
    {
        return replicaCount;
    }

    public int getOperatorCount ()
    {
        return operatorCount;
    }

    public int getInputPortCount ()
    {
        return inputPortCount;
    }

    public double getAvgPipelineCost ()
    {
        double cost = 0;
        for ( int replicaIndex = 0; replicaIndex < getReplicaCount(); replicaIndex++ )
        {
            cost += pipelineCosts[ replicaIndex ];
        }

        return cost / getReplicaCount();
    }

    public double getPipelineCost ( final int replicaIndex )
    {
        return pipelineCosts[ replicaIndex ];
    }

    public double[] getOperatorCosts ( final int replicaIndex )
    {
        return operatorCosts[ replicaIndex ];
    }

    public double[] getAvgOperatorCosts ()
    {
        return range( 0, operatorCount ).mapToDouble( this::getAvgOperatorCost ).toArray();
    }

    public double getOperatorCost ( final int replicaIndex, final int operatorIndex )
    {
        return operatorCosts[ replicaIndex ][ operatorIndex ];
    }

    public double getAvgOperatorCost ( final int operatorIndex )
    {
        double cost = 0;
        for ( int replicaIndex = 0; replicaIndex < getReplicaCount(); replicaIndex++ )
        {
            cost += operatorCosts[ replicaIndex ][ operatorIndex ];
        }

        return cost / getReplicaCount();
    }

    public double getCpuUtilizationRatio ( final int replicaIndex )
    {
        return cpuUtilizationRatios[ replicaIndex ];
    }

    public double getAvgCpuUtilizationRatio ()
    {
        return stream( cpuUtilizationRatios ).average().orElse( NaN );
    }

    public long[] getInboundThroughputs ( final int replicaIndex )
    {
        return inboundThroughputs[ replicaIndex ];
    }

    public long[] getTotalInboundThroughputs ()
    {
        final long[] total = new long[ inputPortCount ];
        for ( int portIndex = 0; portIndex < inputPortCount; portIndex++ )
        {
            total[ portIndex ] = getTotalInboundThroughput( portIndex );
        }

        return total;
    }

    public long getInboundThroughput ( final int replicaIndex, final int portIndex )
    {
        return inboundThroughputs[ replicaIndex ][ portIndex ];
    }

    public long getTotalInboundThroughput ( final int portIndex )
    {
        long sum = 0;
        for ( int replicaIndex = 0; replicaIndex < getReplicaCount(); replicaIndex++ )
        {
            sum += inboundThroughputs[ replicaIndex ][ portIndex ];
        }

        return sum;
    }

    public Snapshot[] getInboundThroughputHistograms ( final int replicaIndex )
    {
        return inboundThroughputHistograms[ replicaIndex ];
    }

    public void visit ( final PipelineMetricsVisitor visitor )
    {
        for ( int replicaIndex = 0; replicaIndex < replicaCount; replicaIndex++ )
        {
            final PipelineReplicaId pipelineReplicaId = new PipelineReplicaId( pipelineId, replicaIndex );
            final long[] inboundThroughputs = getInboundThroughputs( replicaIndex );
            final double threadUtilizationRatio = getCpuUtilizationRatio( replicaIndex );
            final double pipelineCost = getPipelineCost( replicaIndex );
            final double[] operatorCosts = getOperatorCosts( replicaIndex );
            final Snapshot[] inboundThroughputHistograms = getInboundThroughputHistograms( replicaIndex );

            visitor.handle( pipelineReplicaId,
                            flowVersion,
                            inboundThroughputs,
                            threadUtilizationRatio,
                            pipelineCost,
                            operatorCosts,
                            inboundThroughputHistograms );
        }
    }

    @Override
    public String toString ()
    {
        return "PipelineMetrics{" + "pipelineId=" + pipelineId + ", flowVersion=" + flowVersion + ", replicaCount=" + replicaCount
               + ", operatorCount=" + operatorCount + ", inputPortCount=" + inputPortCount + ", cpuUtilizationRatios=" + Arrays.toString(
                cpuUtilizationRatios ) + ", inboundThroughputs=" + Arrays.toString( inboundThroughputs ) + ", operatorCosts="
               + Arrays.toString( operatorCosts ) + ", pipelineCosts=" + Arrays.toString( pipelineCosts ) + ", inboundThroughputHistograms="
               + Arrays.toString( inboundThroughputHistograms ) + '}';
    }

    public interface PipelineMetricsVisitor
    {

        void handle ( PipelineReplicaId pipelineReplicaId,
                      int flowVersion,
                      long[] inboundThroughput,
                      double threadUtilizationRatio,
                      double pipelineCost, double[] operatorCosts, Snapshot[] inboundThroughputHistograms );

    }


    @NotThreadSafe
    public static class PipelineMetricsBuilder
    {

        private final PipelineMetrics snapshot;

        private boolean building = true;

        public PipelineMetricsBuilder ( final PipelineId pipelineId,
                                        final int flowVersion,
                                        final int replicaCount,
                                        final int operatorCount,
                                        final int inputPortCount )
        {
            this.snapshot = new PipelineMetrics( pipelineId, flowVersion, replicaCount, operatorCount, inputPortCount );
        }

        public PipelineMetricsBuilder setPipelineCost ( final int replicaIndex, final double pipelineCost )
        {
            checkArgument( building );
            snapshot.pipelineCosts[ replicaIndex ] = min( pipelineCost, 1d );

            return this;
        }

        public PipelineMetricsBuilder setOperatorCost ( final int replicaIndex, final int operatorIndex, final double operatorCost )
        {
            checkArgument( building );
            snapshot.operatorCosts[ replicaIndex ][ operatorIndex ] = min( operatorCost, 1d );

            return this;
        }

        public PipelineMetricsBuilder setCpuUtilizationRatio ( final int replicaIndex, final double cpuUtilizationRatio )
        {
            checkArgument( building );
            snapshot.cpuUtilizationRatios[ replicaIndex ] = min( cpuUtilizationRatio, 1d );

            return this;
        }

        public PipelineMetricsBuilder setInboundThroughput ( final int replicaIndex, final int portIndex, final long throughput )
        {
            checkArgument( building );
            snapshot.inboundThroughputs[ replicaIndex ][ portIndex ] = throughput;

            return this;
        }

        public PipelineMetricsBuilder setInboundThroughputHistogramSnapshots ( final int replicaIndex, final Snapshot[] histograms )
        {
            arraycopy( histograms, 0, snapshot.inboundThroughputHistograms[ replicaIndex ], 0, snapshot.inputPortCount );

            return this;
        }

        public PipelineMetrics build ()
        {
            checkArgument( building );
            building = false;
            return snapshot;
        }

    }

}
