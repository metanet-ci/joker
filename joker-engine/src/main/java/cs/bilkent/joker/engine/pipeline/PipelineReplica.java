package cs.bilkent.joker.engine.pipeline;

import java.util.Arrays;
import java.util.function.Consumer;
import javax.annotation.concurrent.NotThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import cs.bilkent.joker.engine.config.JokerConfig;
import cs.bilkent.joker.engine.config.TupleQueueDrainerConfig;
import cs.bilkent.joker.engine.exception.InitializationException;
import static cs.bilkent.joker.engine.pipeline.OperatorReplicaStatus.INITIAL;
import static cs.bilkent.joker.engine.pipeline.OperatorReplicaStatus.INITIALIZATION_FAILED;
import static cs.bilkent.joker.engine.pipeline.OperatorReplicaStatus.RUNNING;
import static cs.bilkent.joker.engine.pipeline.OperatorReplicaStatus.SHUT_DOWN;
import static cs.bilkent.joker.engine.pipeline.UpstreamConnectionStatus.ACTIVE;
import cs.bilkent.joker.engine.region.RegionDef;
import cs.bilkent.joker.engine.tuplequeue.TupleQueueContext;
import cs.bilkent.joker.engine.tuplequeue.TupleQueueDrainer;
import cs.bilkent.joker.engine.tuplequeue.impl.context.EmptyTupleQueueContext;
import cs.bilkent.joker.engine.tuplequeue.impl.drainer.BlockingMultiPortDisjunctiveDrainer;
import cs.bilkent.joker.engine.tuplequeue.impl.drainer.BlockingSinglePortDrainer;
import cs.bilkent.joker.engine.tuplequeue.impl.drainer.MultiPortDrainer;
import cs.bilkent.joker.engine.tuplequeue.impl.drainer.NopDrainer;
import cs.bilkent.joker.operator.impl.TuplesImpl;
import cs.bilkent.joker.operator.scheduling.ScheduleWhenTuplesAvailable;
import static cs.bilkent.joker.operator.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByCount.AT_LEAST;
import cs.bilkent.joker.operator.scheduling.SchedulingStrategy;
import static cs.bilkent.joker.operator.spec.OperatorType.PARTITIONED_STATEFUL;
import cs.bilkent.joker.utils.Pair;

/**
 * Manages runtime state of a pipeline defined by the system for a {@link RegionDef} and provides methods for operator invocation.
 */
@NotThreadSafe
public class PipelineReplica
{

    private static final Logger LOGGER = LoggerFactory.getLogger( PipelineReplica.class );


    private final JokerConfig config;

    private final PipelineReplicaId id;

    private final OperatorReplica[] operators;

    private final TupleQueueContext upstreamTupleQueueContext;

    private final int operatorCount;

    private final int upstreamInputPortCount;

    private final int[] upstreamInputPorts;

    private final int[] blockingUpstreamTupleCounts;

    private final int[] nonBlockingUpstreamTupleCounts;

    private final TupleQueueDrainer upstreamDrainer;

    private final Consumer<TupleQueueDrainer> upstreamDrainerParameterSetter;

    private OperatorReplicaStatus status = INITIAL;

    private UpstreamContext pipelineUpstreamContext;

    private boolean noBlockOnUpstreamTupleQueueContext;

    public PipelineReplica ( final JokerConfig config,
                             final PipelineReplicaId id,
                             final OperatorReplica[] operators,
                             final TupleQueueContext upstreamTupleQueueContext )
    {
        this.config = config;
        this.id = id;
        this.operators = Arrays.copyOf( operators, operators.length );
        this.operatorCount = operators.length;
        this.upstreamTupleQueueContext = upstreamTupleQueueContext;
        this.upstreamInputPortCount = operators[ 0 ].getOperatorDef().inputPortCount();
        this.upstreamInputPorts = new int[ upstreamInputPortCount ];
        this.blockingUpstreamTupleCounts = new int[ upstreamInputPortCount ];
        this.nonBlockingUpstreamTupleCounts = new int[ upstreamInputPortCount ];
        this.upstreamDrainer = createUpstreamDrainer();
        this.upstreamDrainerParameterSetter = createUpstreamDrainerParameterSetter();
    }

    private TupleQueueDrainer createUpstreamDrainer ()
    {
        if ( upstreamTupleQueueContext instanceof EmptyTupleQueueContext )
        {
            return new NopDrainer();
        }

        final TupleQueueDrainerConfig tupleQueueDrainerConfig = config.getTupleQueueDrainerConfig();
        if ( upstreamInputPortCount == 1 )
        {
            final BlockingSinglePortDrainer drainer = new BlockingSinglePortDrainer( tupleQueueDrainerConfig.getMaxBatchSize(),
                                                                                     tupleQueueDrainerConfig.getDrainTimeout(),
                                                                                     tupleQueueDrainerConfig.getDrainTimeoutTimeUnit() );
            drainer.setParameters( AT_LEAST, 0 );
            return drainer;
        }

        final MultiPortDrainer drainer = new BlockingMultiPortDisjunctiveDrainer( upstreamInputPortCount,
                                                                                  tupleQueueDrainerConfig.getMaxBatchSize(),
                                                                                  tupleQueueDrainerConfig.getDrainTimeout(),
                                                                                  tupleQueueDrainerConfig.getDrainTimeoutTimeUnit() );
        for ( int i = 0; i < upstreamInputPortCount; i++ )
        {
            upstreamInputPorts[ i ] = i;
        }
        Arrays.fill( blockingUpstreamTupleCounts, 1 );
        Arrays.fill( nonBlockingUpstreamTupleCounts, 0 );
        drainer.setParameters( AT_LEAST, upstreamInputPorts, nonBlockingUpstreamTupleCounts );
        return drainer;
    }

    private Consumer<TupleQueueDrainer> createUpstreamDrainerParameterSetter ()
    {
        if ( upstreamTupleQueueContext instanceof EmptyTupleQueueContext )
        {
            return drainer ->
            {
            };
        }

        if ( upstreamInputPortCount == 1 )
        {
            return drainer ->
            {
                final BlockingSinglePortDrainer b = (BlockingSinglePortDrainer) drainer;
                final int count = noBlockOnUpstreamTupleQueueContext ? 0 : 1;
                b.setParameters( AT_LEAST, count );
            };
        }

        return drainer ->
        {
            final MultiPortDrainer multiPortDrainer = (MultiPortDrainer) upstreamDrainer;
            final int[] tupleCounts = noBlockOnUpstreamTupleQueueContext ? nonBlockingUpstreamTupleCounts : blockingUpstreamTupleCounts;
            multiPortDrainer.setParameters( AT_LEAST, upstreamInputPorts, tupleCounts );
        };
    }

    public SchedulingStrategy[] init ( final UpstreamContext upstreamContext, final OperatorReplicaListener operatorReplicaListener )
    {
        checkState( status == INITIAL, "Cannot initialize PipelineReplica %s as it is in %s state", id, status );
        checkArgument( upstreamContext != null, "Cannot initialize PipelineReplica %s as upstream context is null", id );

        SchedulingStrategy[] schedulingStrategies = new SchedulingStrategy[ operatorCount ];
        UpstreamContext uc = upstreamContext;
        for ( int i = 0; i < operatorCount; i++ )
        {
            try
            {
                final OperatorReplica operator = operators[ i ];
                schedulingStrategies[ i ] = operator.init( uc, operatorReplicaListener );
                uc = operator.getSelfUpstreamContext();
            }
            catch ( InitializationException e )
            {
                shutdownOperators();
                status = INITIALIZATION_FAILED;
                throw e;
            }
        }

        setPipelineUpstreamContext( upstreamContext );

        status = RUNNING;

        return schedulingStrategies;
    }

    public void setPipelineUpstreamContext ( final UpstreamContext pipelineUpstreamContext )
    {
        this.pipelineUpstreamContext = pipelineUpstreamContext;
        if ( upstreamInputPortCount > 1 )
        {
            final OperatorReplica operator = operators[ 0 ];
            final SchedulingStrategy schedulingStrategy = operator.getSchedulingStrategy();
            if ( schedulingStrategy instanceof ScheduleWhenTuplesAvailable )
            {
                updateUpstreamInputPortDrainOrder( operator, (ScheduleWhenTuplesAvailable) schedulingStrategy );
            }
            else
            {
                LOGGER.info( "{} is not updating drainer parameters because {}", id, schedulingStrategy );
            }
        }
    }

    // updates the upstream input port check order such that open ports are checked first
    private void updateUpstreamInputPortDrainOrder ( final OperatorReplica operator, final ScheduleWhenTuplesAvailable schedulingStrategy )
    {
        if ( operator.getStatus() != RUNNING )
        {
            LOGGER.warn( "{} can not update drainer parameters as {} is in {} status",
                         id,
                         operator.getOperatorName(),
                         operator.getStatus() );
            return;
        }

        final Pair<Integer, UpstreamConnectionStatus>[] s = getUpstreamConnectionStatusesSortedByActiveness();
        final int[] tupleCounts = new int[ upstreamInputPortCount ];
        for ( int i = 0; i < upstreamInputPortCount; i++ )
        {
            final int portIndex = s[ i ]._1;
            upstreamInputPorts[ i ] = portIndex;
            tupleCounts[ i ] = schedulingStrategy.getTupleCount( portIndex );
        }

        final MultiPortDrainer drainer = operator.getOperatorDef().operatorType() == PARTITIONED_STATEFUL
                                         ? (MultiPortDrainer) upstreamDrainer
                                         : (MultiPortDrainer) operator.getDrainer();
        LOGGER.info( "{} is updating drainer parameters: {}, input ports: {}, tuple counts: {}",
                     id,
                     schedulingStrategy.getTupleAvailabilityByCount(),
                     upstreamInputPorts,
                     tupleCounts );
        drainer.setParameters( schedulingStrategy.getTupleAvailabilityByCount(), upstreamInputPorts, tupleCounts );
    }

    private Pair<Integer, UpstreamConnectionStatus>[] getUpstreamConnectionStatusesSortedByActiveness ()
    {
        final Pair<Integer, UpstreamConnectionStatus>[] s = new Pair[ upstreamInputPortCount ];
        for ( int i = 0; i < upstreamInputPortCount; i++ )
        {
            s[ i ] = Pair.of( i, pipelineUpstreamContext.getUpstreamConnectionStatus( i ) );
        }
        Arrays.sort( s, ( o1, o2 ) ->
        {
            if ( o1._2 == o2._2 )
            {
                return Integer.compare( o1._1, o2._1 );
            }

            return o1._2 == ACTIVE ? -1 : 1;
        } );
        return s;
    }

    public UpstreamContext getPipelineUpstreamContext ()
    {
        return pipelineUpstreamContext;
    }

    public TupleQueueContext getUpstreamTupleQueueContext ()
    {
        return upstreamTupleQueueContext instanceof EmptyTupleQueueContext ? operators[ 0 ].getQueue() : upstreamTupleQueueContext;
    }

    public TupleQueueContext getSelfUpstreamTupleQueueContext ()
    {
        return upstreamTupleQueueContext;
    }

    public TuplesImpl invoke ()
    {
        UpstreamContext upstreamContext = this.pipelineUpstreamContext;
        drainUpstreamTupleQueueContext();
        TuplesImpl tuples = upstreamDrainer.getResult();

        OperatorReplica operator;
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
        upstreamDrainerParameterSetter.accept( upstreamDrainer );
        upstreamTupleQueueContext.drain( upstreamDrainer );
    }

    public void shutdown ()
    {
        if ( status == SHUT_DOWN )
        {
            return;
        }

        checkState( status == RUNNING || status == INITIALIZATION_FAILED,
                    "Cannot shutdown PipelineReplica %s as it is in %s state",
                    id,
                    status );
        shutdownOperators();
        status = SHUT_DOWN;
    }

    private void shutdownOperators ()
    {
        for ( int i = 0; i < operatorCount; i++ )
        {
            final OperatorReplica operator = operators[ i ];
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

    public PipelineReplicaId id ()
    {
        return id;
    }

    public int getOperatorCount ()
    {
        return operatorCount;
    }

    public OperatorReplica getOperator ( final int index )
    {
        return operators[ index ];
    }

}
