package cs.bilkent.zanza.engine.pipeline;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import cs.bilkent.zanza.engine.exception.InitializationException;
import static cs.bilkent.zanza.operator.InvocationContext.InvocationReason.INPUT_PORT_CLOSED;
import cs.bilkent.zanza.operator.InvocationResult;
import cs.bilkent.zanza.operator.PortsToTuples;
import cs.bilkent.zanza.scheduling.ScheduleNever;

public class PipelineInstance
{

    private static Logger LOGGER = LoggerFactory.getLogger( PipelineInstance.class );

    public static final int NO_INVOKABLE_INDEX = -1;

    public final PipelineInstanceId id;

    private final OperatorInstance[] operatorInstances;

    private final int highestInvokableIndex;

    private OperatorInstanceStatus status = OperatorInstanceStatus.NON_INITIALIZED;

    private int currentHighestInvokableIndex;

    public PipelineInstance ( final PipelineInstanceId id, final OperatorInstance[] operatorInstances )
    {
        this.id = id;
        this.operatorInstances = operatorInstances;
        this.highestInvokableIndex = operatorInstances.length - 1;
    }

    public void init ()
    {
        checkState( status == OperatorInstanceStatus.NON_INITIALIZED );
        for ( int i = 0; i < operatorInstances.length; i++ )
        {
            try
            {
                operatorInstances[ i ].init();
            }
            catch ( InitializationException e )
            {
                shutdownOperators( 0, i );
                status = OperatorInstanceStatus.INITIALIZATION_FAILED;
                throw e;
            }
        }

        status = OperatorInstanceStatus.RUNNING;
        this.currentHighestInvokableIndex = operatorInstances.length - 1;
    }

    public PortsToTuples invoke ()
    {
        checkState( status == OperatorInstanceStatus.RUNNING );
        PortsToTuples curr = null;

        for ( int i = 0; i <= currentHighestInvokableIndex; i++ )
        {
            final InvocationResult result = operatorInstances[ i ].invoke( curr );
            curr = result.getOutputTuples();
            if ( result.getSchedulingStrategy() instanceof ScheduleNever )
            {
                LOGGER.info( "{}: operator {} completes its execution.", id, currentHighestInvokableIndex );
                final int j = currentHighestInvokableIndex;
                currentHighestInvokableIndex = i - 1;
                return forceInvoke( i + 1, j, curr );
            }
        }

        return currentHighestInvokableIndex == highestInvokableIndex ? curr : null;
    }

    private PortsToTuples forceInvoke ( final int startIndexInclusive, final int endIndexInclusive, final PortsToTuples input )
    {
        PortsToTuples curr = input;
        for ( int i = startIndexInclusive; i <= endIndexInclusive; i++ )
        {
            final OperatorInstance operator = operatorInstances[ i ];
            curr = operator.forceInvoke( curr, INPUT_PORT_CLOSED );
            operator.shutdown();
        }

        return curr;
    }

    public int getCurrentHighestInvokableIndex ()
    {
        return currentHighestInvokableIndex;
    }

    public boolean isInvokable ()
    {
        return currentHighestInvokableIndex != NO_INVOKABLE_INDEX;
    }

    public void shutdown ()
    {
        if ( status == OperatorInstanceStatus.SHUT_DOWN )
        {
            return;
        }

        checkState( status == OperatorInstanceStatus.RUNNING || status == OperatorInstanceStatus.INITIALIZATION_FAILED );
        shutdownOperators( 0, currentHighestInvokableIndex );
        status = OperatorInstanceStatus.SHUT_DOWN;
    }

    private void shutdownOperators ( final int startIndexInclusive, final int endIndexInclusive )
    {
        checkArgument( startIndexInclusive >= 0 );
        checkArgument( endIndexInclusive < operatorInstances.length );

        for ( int i = startIndexInclusive; i <= endIndexInclusive; i++ )
        {
            operatorInstances[ i ].shutdown();
        }
    }

}
