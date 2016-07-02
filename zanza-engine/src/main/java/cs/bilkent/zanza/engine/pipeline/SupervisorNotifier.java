package cs.bilkent.zanza.engine.pipeline;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static cs.bilkent.zanza.engine.pipeline.OperatorInstanceStatus.COMPLETED;
import cs.bilkent.zanza.engine.supervisor.Supervisor;

public class SupervisorNotifier implements OperatorInstanceListener
{

    private final static Logger LOGGER = LoggerFactory.getLogger( SupervisorNotifier.class );


    private final Supervisor supervisor;

    private final PipelineInstanceId pipelineInstanceId;

    private final int operatorCount;

    private int completedOperatorCount;

    public SupervisorNotifier ( final Supervisor supervisor, final PipelineInstance pipelineInstance )
    {
        this.supervisor = supervisor;
        this.pipelineInstanceId = pipelineInstance.id();
        this.operatorCount = pipelineInstance.getOperatorCount();
    }

    @Override
    public void onStatusChange ( final String operatorId, final OperatorInstanceStatus status )
    {
        if ( status != COMPLETED )
        {
            LOGGER.info( "{}:{} moves to {} status", pipelineInstanceId, operatorId, status );
            return;
        }

        if ( completedOperatorCount == operatorCount )
        {
            LOGGER.error( "{} moves to {} status although all operators of {} have already moved",
                          operatorId,
                          COMPLETED,
                          pipelineInstanceId );
            return;
        }

        if ( ++completedOperatorCount == operatorCount )
        {
            LOGGER.info( "{} is completed as {} is completed lastly", pipelineInstanceId, operatorId );
            supervisor.notifyPipelineCompletedRunning( pipelineInstanceId );
        }
        else
        {
            LOGGER.info( "{}:{} is completed", pipelineInstanceId, operatorId );
        }
    }

    public boolean isPipelineCompleted ()
    {
        return completedOperatorCount == operatorCount;
    }

}
