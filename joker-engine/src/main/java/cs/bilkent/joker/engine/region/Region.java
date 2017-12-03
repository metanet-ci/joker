package cs.bilkent.joker.engine.region;

import static com.google.common.base.Preconditions.checkArgument;
import cs.bilkent.joker.engine.flow.PipelineId;
import cs.bilkent.joker.engine.flow.RegionDef;
import cs.bilkent.joker.engine.flow.RegionExecutionPlan;
import cs.bilkent.joker.engine.pipeline.PipelineReplica;
import cs.bilkent.joker.engine.pipeline.UpstreamContext;
import cs.bilkent.joker.operator.scheduling.ScheduleWhenAvailable;
import cs.bilkent.joker.operator.scheduling.ScheduleWhenTuplesAvailable;
import cs.bilkent.joker.operator.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByCount;
import static cs.bilkent.joker.operator.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByCount.AT_LEAST;
import static cs.bilkent.joker.operator.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByCount.AT_LEAST_BUT_SAME_ON_ALL_PORTS;
import cs.bilkent.joker.operator.scheduling.SchedulingStrategy;
import static java.lang.System.arraycopy;
import static java.util.Arrays.copyOf;

public class Region
{

    private final RegionExecutionPlan executionPlan;

    private final SchedulingStrategy[] operatorSchedulingStrategies;

    private final UpstreamContext[] operatorUpstreamContexts;

    private final int[] operatorFusionStartIndices;

    // [pipelineIndex, replicaIndex]
    private final PipelineReplica[][] pipelines;

    public Region ( final RegionExecutionPlan executionPlan,
                    final SchedulingStrategy[] operatorSchedulingStrategies,
                    final UpstreamContext[] operatorUpstreamContexts,
                    final PipelineReplica[][] pipelines )
    {
        this.executionPlan = executionPlan;
        this.operatorSchedulingStrategies = copyOf( operatorSchedulingStrategies, operatorSchedulingStrategies.length );
        this.operatorFusionStartIndices = findOperatorFusionStartIndices( operatorSchedulingStrategies );
        this.operatorUpstreamContexts = copyOf( operatorUpstreamContexts, operatorUpstreamContexts.length );
        this.pipelines = copyOf( pipelines, pipelines.length );
        for ( int i = 0; i < pipelines.length; i++ )
        {
            this.pipelines[ i ] = copyOf( pipelines[ i ], pipelines[ i ].length );
        }
    }

    public int getRegionId ()
    {
        return executionPlan.getRegionId();
    }

    public RegionExecutionPlan getExecutionPlan ()
    {
        return executionPlan;
    }

    public RegionDef getRegionDef ()
    {
        return executionPlan.getRegionDef();
    }

    public PipelineReplica[] getReplicaPipelines ( final int replicaIndex )
    {
        final PipelineReplica[] p = new PipelineReplica[ executionPlan.getPipelineCount() ];
        for ( int i = 0; i < executionPlan.getPipelineCount(); i++ )
        {
            p[ i ] = pipelines[ i ][ replicaIndex ];
        }
        return p;
    }

    public PipelineReplica[] getPipelineReplicas ( final int pipelineIndex )
    {
        return pipelines[ pipelineIndex ];
    }

    public PipelineReplica[] getPipelineReplicasByPipelineId ( final PipelineId pipelineId )
    {
        for ( int i = 0; i < executionPlan.getPipelineCount(); i++ )
        {
            if ( pipelines[ i ][ 0 ].id().pipelineId.equals( pipelineId ) )
            {
                return copyOf( pipelines[ i ], executionPlan.getReplicaCount() );
            }
        }

        throw new IllegalArgumentException( "Invalid pipeline id: " + pipelineId );
    }

    public SchedulingStrategy[] getOperatorSchedulingStrategies ()
    {
        return copyOf( operatorSchedulingStrategies, operatorSchedulingStrategies.length );
    }

    public int[] getOperatorFusionStartIndices ()
    {
        return copyOf( operatorFusionStartIndices, operatorFusionStartIndices.length );
    }

    public SchedulingStrategy[] getOperatorSchedulingStrategies ( final PipelineId pipelineId )
    {
        final int pipelineStartIndex = pipelineId.getPipelineStartIndex();
        final int operatorCount = executionPlan.getOperatorCountByPipelineStartIndex( pipelineStartIndex );

        final SchedulingStrategy[] schedulingStrategies = new SchedulingStrategy[ operatorCount ];
        arraycopy( operatorSchedulingStrategies, pipelineStartIndex, schedulingStrategies, 0, operatorCount );

        return schedulingStrategies;
    }

    public UpstreamContext[] getOperatorUpstreamContexts ()
    {
        return copyOf( operatorUpstreamContexts, operatorUpstreamContexts.length );
    }

    public UpstreamContext[] getOperatorUpstreamContexts ( final PipelineId pipelineId )
    {
        final int pipelineStartIndex = pipelineId.getPipelineStartIndex();
        final int operatorCount = executionPlan.getOperatorCountByPipelineStartIndex( pipelineStartIndex );

        final UpstreamContext[] upstreamContexts = new UpstreamContext[ operatorCount ];
        arraycopy( operatorUpstreamContexts, pipelineStartIndex, upstreamContexts, 0, operatorCount );

        return upstreamContexts;
    }

    public static int[] findOperatorFusionStartIndices ( final SchedulingStrategy[] operatorSchedulingStrategies )
    {
        final int[] indices = new int[ operatorSchedulingStrategies.length ];
        indices[ 0 ] = 0;
        int j = 1;
        for ( int i = 1; i < operatorSchedulingStrategies.length; i++ )
        {
            final SchedulingStrategy strategy = operatorSchedulingStrategies[ i ];
            if ( strategy instanceof ScheduleWhenAvailable )
            {
                indices[ j++ ] = i;
                continue;
            }

            checkArgument( strategy instanceof ScheduleWhenTuplesAvailable );

            final ScheduleWhenTuplesAvailable st = (ScheduleWhenTuplesAvailable) strategy;
            final TupleAvailabilityByCount tupleAvailabilityByCount = st.getTupleAvailabilityByCount();
            if ( !( tupleAvailabilityByCount == AT_LEAST || tupleAvailabilityByCount == AT_LEAST_BUT_SAME_ON_ALL_PORTS ) )
            {
                indices[ j++ ] = i;
                continue;
            }

            for ( int portIndex = 0; portIndex < st.getPortCount(); portIndex++ )
            {
                if ( st.getTupleCount( portIndex ) > 1 )
                {
                    indices[ j++ ] = i;
                    break;
                }
            }
        }

        return copyOf( indices, j );
    }

}
