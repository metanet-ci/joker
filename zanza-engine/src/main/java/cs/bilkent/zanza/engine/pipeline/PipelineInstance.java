package cs.bilkent.zanza.engine.pipeline;

import javax.annotation.concurrent.NotThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

/**
 * Manages runtime state of a pipeline defined by the system for a {@link RegionDefinition} and provides methods for operator invocation.
 */
@NotThreadSafe
public class PipelineInstance
{

    private static Logger LOGGER = LoggerFactory.getLogger( PipelineInstance.class );


    private final PipelineInstanceId id;

    private final OperatorInstance[] operators;

    private final int highestInvokableIndex;

    private OperatorInstanceStatus status = INITIAL;

    public PipelineInstance ( final PipelineInstanceId id, final OperatorInstance[] operators )
    {
        this.id = id;
        this.operators = operators;
        this.highestInvokableIndex = operators.length - 1;
    }

    public void init ( ZanzaConfig config )
    {
        checkState( status == INITIAL );
        for ( OperatorInstance operator : operators )
        {
            try
            {
                operator.init( config );
            }
            catch ( InitializationException e )
            {
                shutdownOperators();
                status = INITIALIZATION_FAILED;
                throw e;
            }
        }

        status = RUNNING;
    }

    public TuplesImpl invoke ()
    {
        checkState( status == RUNNING );

        TuplesImpl tuples = null;
        int i = 0;
        for ( ; i <= highestInvokableIndex; i++ )
        {
            final OperatorInstance operator = operators[ i ];
            tuples = operator.invoke( tuples );
            if ( operator.isNonInvokable() )
            {
                break;
            }
        }

        return i < highestInvokableIndex ? forceInvoke( INPUT_PORT_CLOSED, i + 1, tuples ) : tuples;
    }

    public TuplesImpl forceInvoke ( final InvocationReason reason )
    {
        checkState( status == RUNNING );
        return forceInvoke( reason, 0, null );
    }

    private TuplesImpl forceInvoke ( final InvocationReason reason, final int startIndex, TuplesImpl tuples )
    {
        final boolean isProducingDownstreamTuples = isProducingDownstreamTuples();

        for ( int i = startIndex; i <= highestInvokableIndex; i++ )
        {
            final boolean upstreamCompleted = i < 1 || operators[ i - 1 ].isNonInvokable();
            tuples = operators[ i ].forceInvoke( reason, tuples, upstreamCompleted );
        }

        return isProducingDownstreamTuples ? tuples : null;
    }

    public boolean isProducingDownstreamTuples ()
    {
        return operators[ highestInvokableIndex ].isInvokable();
    }

    public boolean isInvokableOperatorAvailable ()
    {
        for ( int i = 0; i <= highestInvokableIndex; i++ )
        {
            if ( operators[ i ].isInvokable() )
            {
                return true;
            }
        }

        return false;
    }

    public void shutdown ()
    {
        if ( status == SHUT_DOWN )
        {
            return;
        }

        checkState( status == RUNNING || status == INITIALIZATION_FAILED );
        shutdownOperators();
        status = SHUT_DOWN;
    }

    private void shutdownOperators ()
    {
        for ( int i = 0; i <= highestInvokableIndex; i++ )
        {
            final OperatorInstance operator = operators[ i ];
            try
            {
                operator.shutdown();
            }
            catch ( Exception e )
            {
                LOGGER.error( "Shutdown of " + operator.getOperatorName() + " failed.", e );
            }
        }
    }

    public PipelineInstanceId id ()
    {
        return id;
    }

}
