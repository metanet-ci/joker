package cs.bilkent.joker.engine.metric;

import cs.bilkent.joker.engine.flow.PipelineId;
import cs.bilkent.joker.engine.pipeline.PipelineReplicaId;
import static java.lang.Math.min;

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

    public double getPipelineCost ( final int replicaIndex )
    {
        return pipelineCosts[ replicaIndex ];
    }

    public void setPipelineCost ( final int replicaIndex, final double pipelineCost )
    {
        pipelineCosts[ replicaIndex ] = min( pipelineCost, 1d );
    }

    public double[] getOperatorCosts ( final int replicaIndex )
    {
        return operatorCosts[ replicaIndex ];
    }

    public void setOperatorCost ( final int replicaIndex, final int operatorIndex, final double operatorCost )
    {
        operatorCosts[ replicaIndex ][ operatorIndex ] = min( operatorCost, 1d );
    }

    public double getOperatorCost ( final int replicaIndex, final int operatorIndex )
    {
        return operatorCosts[ replicaIndex ][ operatorIndex ];
    }

    public double getCpuUtilizationRatio ( final int replicaIndex )
    {
        return cpuUtilizationRatios[ replicaIndex ];
    }

    public void setCpuUtilizationRatio ( final int replicaIndex, final double cpuUtilizationRatio )
    {
        cpuUtilizationRatios[ replicaIndex ] = min( cpuUtilizationRatio, 1d );
    }

    public long[] getInboundThroughputs ( final int replicaIndex )
    {
        return inboundThroughputs[ replicaIndex ];
    }

    public void setInboundThroughput ( final int replicaIndex, final int portIndex, final long throughput )
    {
        inboundThroughputs[ replicaIndex ][ portIndex ] = throughput;
    }

    public long getInboundThroughput ( final int replicaIndex, final int portIndex )
    {
        return inboundThroughputs[ replicaIndex ][ portIndex ];
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

    public interface PipelineMetricsVisitor
    {

        void handle ( PipelineReplicaId pipelineReplicaId,
                      int flowVersion,
                      long[] inboundThroughput,
                      double threadUtilizationRatio,
                      double pipelineCost,
                      double[] operatorCosts );

    }

}
