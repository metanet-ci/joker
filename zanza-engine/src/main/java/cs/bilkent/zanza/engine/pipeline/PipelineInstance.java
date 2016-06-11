package cs.bilkent.zanza.engine.pipeline;

import javax.annotation.concurrent.NotThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import cs.bilkent.zanza.engine.config.ZanzaConfig;
import cs.bilkent.zanza.engine.exception.InitializationException;
import static cs.bilkent.zanza.engine.pipeline.OperatorInstanceStatus.INITIAL;
import static cs.bilkent.zanza.engine.pipeline.OperatorInstanceStatus.INITIALIZATION_FAILED;
import static cs.bilkent.zanza.engine.pipeline.OperatorInstanceStatus.RUNNING;
import static cs.bilkent.zanza.engine.pipeline.OperatorInstanceStatus.SHUT_DOWN;
import cs.bilkent.zanza.engine.region.RegionDefinition;
import cs.bilkent.zanza.flow.OperatorDefinition;
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

    private final int operatorCount;

    private OperatorInstanceStatus status = INITIAL;

    private UpstreamContext pipelineUpstreamContext;

    public PipelineInstance ( final PipelineInstanceId id, final OperatorInstance[] operators )
    {
        this.id = id;
        this.operators = operators;
        this.operatorCount = operators.length;
    }

    public void init ( final ZanzaConfig config,
                       UpstreamContext upstreamContext,
                       final OperatorInstanceLifecycleListener operatorInstanceLifecycleListener )
    {
        checkState( status == INITIAL );
        checkNotNull( config );
        checkNotNull( upstreamContext );

        this.pipelineUpstreamContext = upstreamContext;
        for ( OperatorInstance operator : operators )
        {
            try
            {
                operator.init( config, upstreamContext, operatorInstanceLifecycleListener );
                upstreamContext = operator.getSelfUpstreamContext();
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


    public void setPipelineUpstreamContext ( final UpstreamContext pipelineUpstreamContext )
    {
        this.pipelineUpstreamContext = pipelineUpstreamContext;
    }

    public UpstreamContext getPipelineUpstreamContext ()
    {
        return pipelineUpstreamContext;
    }

    public TuplesImpl invoke ()
    {
        OperatorInstance operator;
        TuplesImpl tuples = null;
        UpstreamContext upstreamContext = this.pipelineUpstreamContext;

        for ( int i = 0; i < operatorCount; i++ )
        {
            operator = operators[ i ];
            tuples = operator.invoke( tuples, upstreamContext );
            upstreamContext = operator.getSelfUpstreamContext();
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
        shutdownOperators();
        status = SHUT_DOWN;
    }

    private void shutdownOperators ()
    {
        for ( int i = 0; i < operatorCount; i++ )
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

    public int getOperatorCount ()
    {
        return operatorCount;
    }

    public OperatorDefinition getOperatorDefinition ( final int index )
    {
        return operators[ index ].getOperatorDefinition();
    }

}
