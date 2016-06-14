package cs.bilkent.zanza.engine.pipeline;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static cs.bilkent.zanza.engine.pipeline.OperatorInstanceStatus.COMPLETED;
import cs.bilkent.zanza.engine.supervisor.Supervisor;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueueContext;
import cs.bilkent.zanza.operator.scheduling.ScheduleWhenTuplesAvailable;
import cs.bilkent.zanza.operator.scheduling.SchedulingStrategy;

public class SupervisorNotifier implements OperatorInstanceListener
{

    private final static Logger LOGGER = LoggerFactory.getLogger( SupervisorNotifier.class );


    private final Supervisor supervisor;

    private final PipelineInstanceId pipelineInstanceId;

    private final int operatorCount;

    private final String firstOperatorId;

    private final String lastOperatorId;

    private final TupleQueueContext upstreamTupleQueueContext;

    private int completedOperatorCount;

    private boolean firstOperatorCompleted;

    private boolean lastOperatorCompleted;

    public SupervisorNotifier ( final Supervisor supervisor,
                                final PipelineInstanceId pipelineInstanceId,
                                final int operatorCount,
                                final String firstOperatorId,
                                final String lastOperatorId,
                                final TupleQueueContext upstreamTupleQueueContext )
    {
        this.supervisor = supervisor;
        this.pipelineInstanceId = pipelineInstanceId;
        this.operatorCount = operatorCount;
        this.firstOperatorId = firstOperatorId;
        this.lastOperatorId = lastOperatorId;
        this.upstreamTupleQueueContext = upstreamTupleQueueContext;
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
        else if ( firstOperatorId.equals( operatorId ) )
        {
            if ( firstOperatorCompleted )
            {
                LOGGER.error( "first operator {}:{} is already completed but moves to {} status again",
                              pipelineInstanceId,
                              operatorId,
                              COMPLETED );
            }
            else
            {
                LOGGER.info( "first operator {}:{} is completed", pipelineInstanceId, operatorId );
                firstOperatorCompleted = true;
                supervisor.notifyPipelineStoppedReceivingUpstreamTuples( pipelineInstanceId );
            }
        }
        else if ( lastOperatorId.equals( operatorId ) )
        {
            if ( lastOperatorCompleted )
            {
                LOGGER.error( "last operator {}:{} is already completed but moves to {} status again",
                              pipelineInstanceId,
                              operatorId,
                              COMPLETED );
            }
            else
            {
                LOGGER.info( "last operator {}:{} is completed", pipelineInstanceId, operatorId );
                lastOperatorCompleted = true;
                supervisor.notifyPipelineStoppedSendingDownstreamTuples( pipelineInstanceId );
            }
        }
        else
        {
            LOGGER.info( "{}:{} is completed", pipelineInstanceId, operatorId );
        }
    }

    @Override
    public void onSchedulingStrategyChange ( final String operatorId, final SchedulingStrategy newSchedulingStrategy )
    {
        if ( firstOperatorId.equals( operatorId ) && newSchedulingStrategy instanceof ScheduleWhenTuplesAvailable )
        {
            final ScheduleWhenTuplesAvailable scheduleWhenTuplesAvailable = (ScheduleWhenTuplesAvailable) newSchedulingStrategy;
            for ( int portIndex = 0; portIndex < scheduleWhenTuplesAvailable.getPortCount(); portIndex++ )
            {
                upstreamTupleQueueContext.ensureCapacity( portIndex, scheduleWhenTuplesAvailable.getTupleCount( portIndex ) );
            }
        }
    }

    public boolean isPipelineCompleted ()
    {
        return completedOperatorCount == operatorCount;
    }

}
