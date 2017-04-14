package cs.bilkent.joker.engine.metric;

import java.util.Arrays;
import java.util.stream.DoubleStream;
import java.util.stream.LongStream;
import javax.annotation.concurrent.NotThreadSafe;

import cs.bilkent.joker.engine.flow.PipelineId;
import cs.bilkent.joker.engine.pipeline.PipelineReplicaId;
import static cs.bilkent.joker.impl.com.google.common.base.Preconditions.checkArgument;
import static java.lang.Double.NaN;
import static java.lang.Math.min;
import static java.util.Arrays.stream;
import static java.util.stream.IntStream.range;

public class PipelineMetricsSnapshot
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

    public PipelineMetricsSnapshot ( final PipelineId pipelineId,
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
    }

    PipelineMetricsSnapshot ( final PipelineId pipelineId,
                              final int flowVersion,
                              final int replicaCount,
                              final int operatorCount,
                              final int inputPortCount,
                              final double[] cpuUtilizationRatios,
                              final long[][] inboundThroughputs,
                              final double[][] operatorCosts,
                              final double[] pipelineCosts )
    {
        this.pipelineId = pipelineId;
        this.flowVersion = flowVersion;
        this.replicaCount = replicaCount;
        this.operatorCount = operatorCount;
        this.inputPortCount = inputPortCount;
        this.cpuUtilizationRatios = cpuUtilizationRatios;
        this.inboundThroughputs = inboundThroughputs;
        this.operatorCosts = operatorCosts;
        this.pipelineCosts = pipelineCosts;
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
        if ( replicaCount == 1 )
        {
            return pipelineCosts[ 0 ];
        }

        return stream( pipelineCosts ).average().orElse( NaN );
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
        if ( replicaCount == 1 )
        {
            return operatorCosts[ 0 ][ operatorIndex ];
        }

        return stream( operatorCosts ).flatMapToDouble( replica -> DoubleStream.of( replica[ operatorIndex ] ) ).average().orElse( NaN );
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

    public long[] getAvgInboundThroughputs ()
    {
        return range( 0, inputPortCount ).mapToLong( this::getAvgInboundThroughput ).toArray();
    }

    public long getInboundThroughput ( final int replicaIndex, final int portIndex )
    {
        return inboundThroughputs[ replicaIndex ][ portIndex ];
    }

    public long getAvgInboundThroughput ( final int portIndex )
    {
        if ( replicaCount == 1 )
        {
            return inboundThroughputs[ 0 ][ portIndex ];
        }

        return (long) stream( inboundThroughputs ).flatMapToLong( replica -> LongStream.of( replica[ portIndex ] ) ).average().orElse( -1 );
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

            visitor.handle( pipelineReplicaId, flowVersion, inboundThroughputs, threadUtilizationRatio, pipelineCost, operatorCosts );
        }
    }

    @Override
    public String toString ()
    {
        return "PipelineMetricsSnapshot{" + "pipelineId=" + pipelineId + ", flowVersion=" + flowVersion + ", cpuUtilizationRatios="
               + Arrays.toString( cpuUtilizationRatios ) + ", inboundThroughputs=" + Arrays.toString( inboundThroughputs )
               + ", operatorCosts=" + Arrays.toString( operatorCosts ) + ", pipelineCosts=" + Arrays.toString( pipelineCosts ) + '}';
    }

    public interface PipelineMetricsVisitor
    {

        void handle ( PipelineReplicaId pipelineReplicaId,
                      int flowVersion,
                      long[] inboundThroughput,
                      double threadUtilizationRatio,
                      double pipelineCost,
                      double[] operatorCosts );

    }


    @NotThreadSafe
    public static class PipelineMetricsSnapshotBuilder
    {

        private final PipelineMetricsSnapshot snapshot;

        private boolean building = true;

        public PipelineMetricsSnapshotBuilder ( final PipelineId pipelineId,
                                                final int flowVersion,
                                                final int replicaCount,
                                                final int operatorCount,
                                                final int inputPortCount )
        {
            this.snapshot = new PipelineMetricsSnapshot( pipelineId, flowVersion, replicaCount, operatorCount, inputPortCount );
        }

        public PipelineMetricsSnapshotBuilder setPipelineCost ( final int replicaIndex, final double pipelineCost )
        {
            checkArgument( building );
            snapshot.pipelineCosts[ replicaIndex ] = min( pipelineCost, 1d );

            return this;
        }

        public PipelineMetricsSnapshotBuilder setOperatorCost ( final int replicaIndex, final int operatorIndex, final double operatorCost )
        {
            checkArgument( building );
            snapshot.operatorCosts[ replicaIndex ][ operatorIndex ] = min( operatorCost, 1d );

            return this;
        }

        public PipelineMetricsSnapshotBuilder setCpuUtilizationRatio ( final int replicaIndex, final double cpuUtilizationRatio )
        {
            checkArgument( building );
            snapshot.cpuUtilizationRatios[ replicaIndex ] = min( cpuUtilizationRatio, 1d );

            return this;
        }

        public PipelineMetricsSnapshotBuilder setInboundThroughput ( final int replicaIndex, final int portIndex, final long throughput )
        {
            checkArgument( building );
            snapshot.inboundThroughputs[ replicaIndex ][ portIndex ] = throughput;

            return this;
        }

        public PipelineMetricsSnapshot build ()
        {
            checkArgument( building );
            building = false;
            return snapshot;
        }

    }

}
