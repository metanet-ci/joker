package cs.bilkent.zanza.engine.pipeline;

import java.util.Arrays;
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
import cs.bilkent.zanza.engine.tuplequeue.TupleQueueContext;
import cs.bilkent.zanza.engine.tuplequeue.impl.drainer.BlockingMultiPortDisjunctiveDrainer;
import cs.bilkent.zanza.engine.tuplequeue.impl.drainer.MultiPortDrainer;
import cs.bilkent.zanza.flow.OperatorDefinition;
import cs.bilkent.zanza.operator.impl.TuplesImpl;
import static cs.bilkent.zanza.operator.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByCount.AT_LEAST;

/**
 * Manages runtime state of a pipeline defined by the system for a {@link RegionDefinition} and provides methods for operator invocation.
 */
@NotThreadSafe
public class PipelineInstance
{

    private static Logger LOGGER = LoggerFactory.getLogger( PipelineInstance.class );


    private final PipelineInstanceId id;

    private final OperatorInstance[] operators;

    private final TupleQueueContext upstreamTupleQueueContext;

    private final int operatorCount;

    private final int[] blockingUpstreamTupleCounts;

    private final int[] nonBlockingUpstreamTupleCounts;

    private OperatorInstanceStatus status = INITIAL;

    private MultiPortDrainer upstreamDrainer;

    private UpstreamContext pipelineUpstreamContext;

    private boolean noBlockOnUpstreamTupleQueueContext;

    public PipelineInstance ( final PipelineInstanceId id,
                              final OperatorInstance[] operators,
                              final TupleQueueContext upstreamTupleQueueContext )
    {
        this.id = id;
        this.operators = operators;
        this.operatorCount = operators.length;
        this.upstreamTupleQueueContext = upstreamTupleQueueContext;
        this.blockingUpstreamTupleCounts = new int[ operators[ 0 ].getOperatorDefinition().inputPortCount() ];
        this.nonBlockingUpstreamTupleCounts = new int[ operators[ 0 ].getOperatorDefinition().inputPortCount() ];
    }

    public void init ( final ZanzaConfig config, UpstreamContext upstreamContext, final OperatorInstanceListener operatorInstanceListener )
    {
        checkState( status == INITIAL );
        checkNotNull( config );
        checkNotNull( upstreamContext );

        this.pipelineUpstreamContext = upstreamContext;
        for ( OperatorInstance operator : operators )
        {
            try
            {
                operator.init( config, upstreamContext, operatorInstanceListener );
                upstreamContext = operator.getSelfUpstreamContext();
            }
            catch ( InitializationException e )
            {
                shutdownOperators();
                status = INITIALIZATION_FAILED;
                throw e;
            }
        }

        upstreamDrainer = createUpstreamDrainer( config );

        status = RUNNING;
    }

    private MultiPortDrainer createUpstreamDrainer ( final ZanzaConfig config )
    {
        final int upstreamInputPortCount = operators[ 0 ].getOperatorDefinition().inputPortCount();
        final MultiPortDrainer drainer = new BlockingMultiPortDisjunctiveDrainer( upstreamInputPortCount,
                                                                                  config.getTupleQueueDrainerConfig().getMaxBatchSize(),
                                                                                  config.getTupleQueueDrainerConfig()
                                                                                        .getDrainTimeoutInMillis() );
        Arrays.fill( blockingUpstreamTupleCounts, 1 );
        Arrays.fill( nonBlockingUpstreamTupleCounts, 0 );
        drainer.setParameters( AT_LEAST, nonBlockingUpstreamTupleCounts );
        return drainer;
    }


    public void setPipelineUpstreamContext ( final UpstreamContext pipelineUpstreamContext )
    {
        this.pipelineUpstreamContext = pipelineUpstreamContext;
    }

    public UpstreamContext getPipelineUpstreamContext ()
    {
        return pipelineUpstreamContext;
    }

    public TupleQueueContext getUpstreamTupleQueueContext ()
    {
        return upstreamTupleQueueContext;
    }

    public TuplesImpl invoke ()
    {
        OperatorInstance operator;
        drainUpstreamTupleQueueContext();
        TuplesImpl tuples = upstreamDrainer.getResult();
        UpstreamContext upstreamContext = this.pipelineUpstreamContext;

        this.noBlockOnUpstreamTupleQueueContext = false;
        for ( int i = 0; i < operatorCount; i++ )
        {
            operator = operators[ i ];
            tuples = operator.invoke( tuples, upstreamContext );
            noBlockOnUpstreamTupleQueueContext |= operator.isInvokedOnLastAttempt();
            upstreamContext = operator.getSelfUpstreamContext();
        }

        upstreamDrainer.reset();

        return tuples;
    }

    private void drainUpstreamTupleQueueContext ()
    {
        if ( noBlockOnUpstreamTupleQueueContext )
        {
            upstreamDrainer.setParameters( AT_LEAST, nonBlockingUpstreamTupleCounts );
        }
        else
        {
            upstreamDrainer.setParameters( AT_LEAST, blockingUpstreamTupleCounts );
        }

        upstreamTupleQueueContext.drain( upstreamDrainer );
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
