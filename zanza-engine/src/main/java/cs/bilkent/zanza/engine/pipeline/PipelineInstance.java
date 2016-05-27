package cs.bilkent.zanza.engine.pipeline;

import javax.annotation.concurrent.NotThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import cs.bilkent.zanza.engine.config.ZanzaConfig;
import cs.bilkent.zanza.engine.exception.InitializationException;
import static cs.bilkent.zanza.engine.pipeline.OperatorInstanceStatus.INITIAL;
import static cs.bilkent.zanza.engine.pipeline.OperatorInstanceStatus.INITIALIZATION_FAILED;
import static cs.bilkent.zanza.engine.pipeline.OperatorInstanceStatus.RUNNING;
import static cs.bilkent.zanza.engine.pipeline.OperatorInstanceStatus.SHUT_DOWN;
import cs.bilkent.zanza.engine.region.RegionDefinition;
import cs.bilkent.zanza.operator.InvocationContext.InvocationReason;
import static cs.bilkent.zanza.operator.InvocationContext.InvocationReason.INPUT_PORT_CLOSED;
import cs.bilkent.zanza.operator.impl.TuplesImpl;
import cs.bilkent.zanza.operator.scheduling.ScheduleNever;

/**
 * Manages runtime state of a pipeline defined by the system for a {@link RegionDefinition} and provides methods for operator invocation.
 */
@NotThreadSafe
public class PipelineInstance
{

    private static Logger LOGGER = LoggerFactory.getLogger( PipelineInstance.class );

    static final int NO_INVOKABLE_INDEX = -1;


    private final PipelineInstanceId id;

    private final OperatorInstance[] operators;

    private final int highestInvokableIndex;

    private OperatorInstanceStatus status = INITIAL;

    private int currentHighestInvokableIndex;

    public PipelineInstance ( final PipelineInstanceId id, final OperatorInstance[] operators )
    {
        this.id = id;
        this.operators = operators;
        this.highestInvokableIndex = operators.length - 1;
    }

    public void init ( ZanzaConfig config )
    {
        checkState( status == INITIAL );
        for ( int i = 0; i < operators.length; i++ )
        {
            try
            {
                operators[ i ].init( config );
            }
            catch ( InitializationException e )
            {
                shutdownOperators( 0, i );
                status = INITIALIZATION_FAILED;
                throw e;
            }
        }

        status = RUNNING;
        this.currentHighestInvokableIndex = operators.length - 1;
    }

    public TuplesImpl invoke ()
    {
        checkState( status == RUNNING );

        TuplesImpl tuples = null;
        for ( int i = 0; i <= currentHighestInvokableIndex; i++ )
        {
            tuples = operators[ i ].invoke( tuples );
            if ( operators[ i ].schedulingStrategy() instanceof ScheduleNever )
            {
                LOGGER.info( "{}: operator {} completes its execution.", id, currentHighestInvokableIndex );
                final int j = currentHighestInvokableIndex;
                currentHighestInvokableIndex = i - 1;
                tuples = forceInvoke( i + 1, j, tuples, INPUT_PORT_CLOSED );
                return j == highestInvokableIndex ? tuples : null;
            }
        }

        return currentHighestInvokableIndex == highestInvokableIndex ? tuples : null;
    }

    public TuplesImpl forceInvoke ( final InvocationReason reason )
    {
        checkState( status == RUNNING );

        if ( currentHighestInvokableIndex != NO_INVOKABLE_INDEX )
        {
            final int i = currentHighestInvokableIndex;
            currentHighestInvokableIndex = NO_INVOKABLE_INDEX;
            final TuplesImpl output = forceInvoke( 0, i, null, reason );
            return i == highestInvokableIndex ? output : null;
        }

        return null;
    }

    private TuplesImpl forceInvoke ( final int startIndexInclusive,
                                     final int endIndexInclusive,
                                     final TuplesImpl input,
                                     final InvocationReason reason )
    {
        TuplesImpl tuples = input;
        for ( int i = startIndexInclusive; i <= endIndexInclusive; i++ )
        {
            final OperatorInstance operator = operators[ i ];
            tuples = operator.forceInvoke( tuples, reason );
            operator.shutdown();
        }

        return tuples;
    }

    public void shutdown ()
    {
        if ( status == SHUT_DOWN )
        {
            return;
        }

        checkState( status == RUNNING || status == INITIALIZATION_FAILED );
        shutdownOperators( 0, currentHighestInvokableIndex );
        status = SHUT_DOWN;
    }

    public PipelineInstanceId id ()
    {
        return id;
    }

    public int currentHighestInvokableIndex ()
    {
        return currentHighestInvokableIndex;
    }

    public boolean isInvokable ()
    {
        return currentHighestInvokableIndex != NO_INVOKABLE_INDEX;
    }

    public int operatorCount ()
    {
        return operators.length;
    }

    private void shutdownOperators ( final int startIndexInclusive, final int endIndexInclusive )
    {
        checkArgument( startIndexInclusive >= 0 );
        checkArgument( endIndexInclusive < operators.length );

        for ( int i = startIndexInclusive; i <= endIndexInclusive; i++ )
        {
            operators[ i ].shutdown();
        }
    }

}
