package cs.bilkent.zanza.engine.pipeline;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static cs.bilkent.zanza.engine.pipeline.OperatorReplicaStatus.COMPLETED;
import cs.bilkent.zanza.engine.supervisor.Supervisor;

public class SupervisorNotifier implements OperatorReplicaListener
{

    private final static Logger LOGGER = LoggerFactory.getLogger( SupervisorNotifier.class );


    private final Supervisor supervisor;

    private final PipelineReplicaId pipelineReplicaId;

    private final int operatorCount;

    private int completedOperatorCount;

    public SupervisorNotifier ( final Supervisor supervisor, final PipelineReplica pipelineReplica )
    {
        this.supervisor = supervisor;
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
                          operatorId, COMPLETED, pipelineReplicaId );
            return;
        }

        if ( ++completedOperatorCount == operatorCount )
        {
            LOGGER.info( "{} is completed as {} is completed lastly", pipelineReplicaId, operatorId );
            supervisor.notifyPipelineReplicaCompleted( pipelineReplicaId );
        }
        else
        {
            LOGGER.info( "{}:{} is completed", pipelineReplicaId, operatorId );
        }
    }

    public boolean isPipelineCompleted ()
    {
        return completedOperatorCount == operatorCount;
    }

}
