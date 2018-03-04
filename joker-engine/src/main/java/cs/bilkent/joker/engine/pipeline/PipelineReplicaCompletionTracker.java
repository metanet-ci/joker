package cs.bilkent.joker.engine.pipeline;

import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkState;
import static cs.bilkent.joker.engine.pipeline.OperatorReplicaStatus.COMPLETED;

public class PipelineReplicaCompletionTracker implements OperatorReplicaListener
{

    private static final Logger LOGGER = LoggerFactory.getLogger( PipelineReplicaCompletionTracker.class );


    private final PipelineReplicaId pipelineReplicaId;

    private final int operatorCount;

    private final Set<String> runningOperatorIds = new HashSet<>();

    private int completedOperatorCount;

    PipelineReplicaCompletionTracker ( final PipelineReplicaId pipelineReplicaId, final OperatorReplica[] operators )
    {
        this.pipelineReplicaId = pipelineReplicaId;
        this.operatorCount = operators.length;
        for ( OperatorReplica operator : operators )
        {
            runningOperatorIds.add( operator.getOperatorDef( 0 ).getId() );
        }
    }

    @Override
    public void onStatusChange ( final String operatorId, final OperatorReplicaStatus status )
    {
        if ( status != COMPLETED )
        {
            LOGGER.info( "{}:{} moves to {} status", pipelineReplicaId, operatorId, status );
            return;
        }

        checkState( runningOperatorIds.remove( operatorId ),
                    "Operator: %s is already completed in PipelineReplica %s",
                    operatorId,
                    pipelineReplicaId );
        checkState( completedOperatorCount < operatorCount,
                    "%s moves to %s status although all operators of %s have already %s",
                    operatorId,
                    COMPLETED,
                    pipelineReplicaId,
                    COMPLETED );

        LOGGER.info( "{}:{} is completed", pipelineReplicaId, operatorId );
        if ( ++completedOperatorCount == operatorCount )
        {
            LOGGER.info( "{} will complete as last operator: {} is completed.", pipelineReplicaId, operatorId );
        }
    }

    boolean isPipelineCompleted ()
    {
        return completedOperatorCount == operatorCount;
    }

}
