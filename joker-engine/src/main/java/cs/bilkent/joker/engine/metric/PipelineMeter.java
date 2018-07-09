package cs.bilkent.joker.engine.metric;

import java.lang.Thread.State;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.ArrayList;
import java.util.List;

import com.codahale.metrics.Snapshot;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import cs.bilkent.joker.engine.flow.PipelineId;
import cs.bilkent.joker.engine.pipeline.PipelineReplicaId;
import cs.bilkent.joker.operator.OperatorDef;
import static java.util.Arrays.copyOf;

public class PipelineMeter
{

    public static final int NO_OPERATOR_INDEX = -2;

    public static final int PIPELINE_EXECUTION_INDEX = -1;


    private final PipelineId pipelineId;

    private final int replicaCount;

    private final long[] threadIds;

    private final PipelineReplicaMeter[] pipelineReplicaMeters;

    private final List<String> operatorIds;

    public PipelineMeter ( final PipelineId pipelineId,
                           final OperatorDef[] operatorDefs,
                           final int replicaCount,
                           final long[] threadIds,
                           final PipelineReplicaMeter[] pipelineReplicaMeters )
    {
        checkArgument( pipelineId != null );
        checkArgument( replicaCount > 0 );
        checkArgument( threadIds != null );
        checkArgument( pipelineReplicaMeters != null );
        checkArgument( replicaCount == threadIds.length );
        checkArgument( replicaCount == pipelineReplicaMeters.length );
        this.pipelineId = pipelineId;
        this.operatorIds = new ArrayList<>( operatorDefs.length );
        for ( OperatorDef operatorDef : operatorDefs )
        {
            this.operatorIds.add( operatorDef.getId() );
        }

        this.replicaCount = replicaCount;
        this.threadIds = copyOf( threadIds, threadIds.length );
        this.pipelineReplicaMeters = copyOf( pipelineReplicaMeters, pipelineReplicaMeters.length );
        for ( PipelineReplicaMeter replicaMeter : pipelineReplicaMeters )
        {
            checkArgument( replicaMeter.getPipelineReplicaId().pipelineId.equals( pipelineId ) );
            checkArgument( replicaMeter.getHeadOperatorId().equals( operatorDefs[ 0 ].getId() ) );
            checkArgument( replicaMeter.getInputPortCount() == operatorDefs[ 0 ].getInputPortCount() );
        }
    }

    public PipelineId getPipelineId ()
    {
        return pipelineId;
    }

    public PipelineReplicaId getPipelineReplicaId ( final int replicaIndex )
    {
        return pipelineReplicaMeters[ replicaIndex ].getPipelineReplicaId();
    }

    public int getOperatorCount ()
    {
        return operatorIds.size();
    }

    public int getReplicaCount ()
    {
        return replicaCount;
    }

    public int getInputPortCount ()
    {
        return pipelineReplicaMeters[ 0 ].getInputPortCount();
    }

    public int getCurrentlyExecutingComponentIndex ( final ThreadMXBean threadMXBean, final int replicaIndex )
    {
        final PipelineReplicaMeter pipelineReplicaMeter = pipelineReplicaMeters[ replicaIndex ];
        final Object component = pipelineReplicaMeter.getCurrentlyExecutingComponent();
        if ( component == null )
        {
            return NO_OPERATOR_INDEX;
        }

        final ThreadInfo threadInfo = threadMXBean.getThreadInfo( threadIds[ replicaIndex ] );

        if ( threadInfo.getThreadState() != State.RUNNABLE )
        {
            return NO_OPERATOR_INDEX;
        }

        if ( pipelineReplicaMeter.getPipelineReplicaId().equals( component ) )
        {
            return PIPELINE_EXECUTION_INDEX;
        }

        final int i = operatorIds.indexOf( component );
        checkState( i >= 0, "operator %s is not in pipeline %s with operators %s", component, pipelineId, operatorIds );
        return i;
    }

    public void getThreadCpuTimes ( final ThreadMXBean threadMXBean, final long[] threadCpuTimes )
    {
        for ( int replicaIndex = 0; replicaIndex < replicaCount; replicaIndex++ )
        {
            threadCpuTimes[ replicaIndex ] = threadMXBean.getThreadCpuTime( threadIds[ replicaIndex ] );
        }
    }

    public void readInboundThroughput ( final int replicaIndex, final long[] inboundThroughput )
    {
        // happens-before
        final PipelineReplicaMeter pipelineReplicaMeter = pipelineReplicaMeters[ replicaIndex ];
        pipelineReplicaMeter.getCurrentlyExecutingComponent();
        pipelineReplicaMeter.readInboundThroughput( inboundThroughput );
    }

    public Snapshot[] getInboundThroughputHistograms ( final int replicaIndex )
    {
        return pipelineReplicaMeters[ replicaIndex ].getInboundThroughputHistograms();
    }

}
