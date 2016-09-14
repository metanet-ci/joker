package cs.bilkent.joker.engine.pipeline;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static cs.bilkent.joker.engine.pipeline.OperatorReplicaStatus.COMPLETED;

public class PipelineReplicaCompletionTracker implements OperatorReplicaListener
{

    private static final Logger LOGGER = LoggerFactory.getLogger( PipelineReplicaCompletionTracker.class );


    private final PipelineReplicaId pipelineReplicaId;

    private final int operatorCount;

    private int completedOperatorCount;

    public PipelineReplicaCompletionTracker ( final PipelineReplica pipelineReplica )
    {
        this.pipelineReplicaId = pipelineReplica.id();
        this.operatorCount = pipelineReplica.getOperatorCount();
    }

    @Override
    public void onStatusChange ( final String operatorId, final OperatorReplicaStatus status )
    {
        if ( status != COMPLETED )
        {
            LOGGER.info( "{}:{} moves to {} status", pipelineReplicaId, operatorId, status );
            return;
        }

        if ( completedOperatorCount == operatorCount )
        {
            LOGGER.error( "{} moves to {} status although all operators of {} have already moved",
                          operatorId,
                          COMPLETED,
                          pipelineReplicaId );
            return;
        }

        LOGGER.info( "{}:{} is completed", pipelineReplicaId, operatorId );
        if ( ++completedOperatorCount == operatorCount )
        {
            LOGGER.info( "{} will complete as last operator: {} is completed.", pipelineReplicaId, operatorId );
        }
    }

    public boolean isPipelineCompleted ()
    {
        return completedOperatorCount == operatorCount;
    }

}
