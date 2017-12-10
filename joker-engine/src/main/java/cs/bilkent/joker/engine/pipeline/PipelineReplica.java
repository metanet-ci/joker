package cs.bilkent.joker.engine.pipeline;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import javax.annotation.concurrent.NotThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import cs.bilkent.joker.engine.config.JokerConfig;
import cs.bilkent.joker.engine.exception.InitializationException;
import cs.bilkent.joker.engine.flow.RegionDef;
import cs.bilkent.joker.engine.metric.PipelineReplicaMeter;
import static cs.bilkent.joker.engine.pipeline.OperatorReplicaStatus.INITIAL;
import static cs.bilkent.joker.engine.pipeline.OperatorReplicaStatus.RUNNING;
import static cs.bilkent.joker.engine.pipeline.OperatorReplicaStatus.SHUT_DOWN;
import cs.bilkent.joker.engine.tuplequeue.OperatorTupleQueue;
import cs.bilkent.joker.engine.tuplequeue.TupleQueueDrainer;
import cs.bilkent.joker.engine.tuplequeue.impl.drainer.BlockingGreedyDrainer;
import cs.bilkent.joker.engine.tuplequeue.impl.drainer.NopDrainer;
import cs.bilkent.joker.engine.tuplequeue.impl.operator.EmptyOperatorTupleQueue;
import cs.bilkent.joker.operator.OperatorDef;
import cs.bilkent.joker.operator.impl.TuplesImpl;
import cs.bilkent.joker.operator.scheduling.SchedulingStrategy;
import cs.bilkent.joker.partition.impl.PartitionKey;
import static java.util.stream.Collectors.toList;

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

    private final OperatorTupleQueue pipelineTupleQueue;

    private final PipelineReplicaMeter meter;

    private final int operatorCount;

    private final int pipelineInputPortCount;

    private final PipelineInputTuplesSupplier pipelineInputTuplesSupplier;

    private final PipelineReplicaCompletionTracker pipelineReplicaCompletionTracker;

    private TupleQueueDrainer upstreamDrainer;

    private OperatorReplicaStatus status = INITIAL;

    private UpstreamContext pipelineUpstreamContext;

    private boolean drainerMaySkipBlocking = true;

    public PipelineReplica ( final JokerConfig config,
                             final PipelineReplicaId id,
                             final OperatorReplica[] operators,
                             final OperatorTupleQueue pipelineTupleQueue,
                             final PipelineReplicaMeter meter )
    {
        this.config = config;
        this.id = id;
        this.operators = Arrays.copyOf( operators, operators.length );
        this.operatorCount = operators.length;
        this.pipelineTupleQueue = pipelineTupleQueue;
        this.meter = meter;
        this.pipelineInputPortCount = operators[ 0 ].getOperatorDef().getInputPortCount();
        this.pipelineInputTuplesSupplier = new PipelineInputTuplesSupplier( pipelineInputPortCount );
        this.pipelineReplicaCompletionTracker = new PipelineReplicaCompletionTracker( id, operators );
        for ( OperatorReplica operator : operators )
        {
            operator.setOperatorReplicaListener( this.pipelineReplicaCompletionTracker );
        }
    }

    public PipelineReplica ( final JokerConfig config,
                             final PipelineReplicaId id,
                             final OperatorReplica[] operators,
                             final OperatorTupleQueue pipelineTupleQueue,
                             final PipelineReplicaMeter meter,
                             final UpstreamContext upstreamContext )
    {
        this( config, id, operators, pipelineTupleQueue, meter );
        initUpstreamDrainer();
        this.status = RUNNING;
        setPipelineUpstreamContext( upstreamContext );
    }

    // constructor for pipeline replica duplication
    private PipelineReplica ( final JokerConfig config,
                              final PipelineReplicaId id,
                              final OperatorReplica[] operators,
                              final OperatorTupleQueue pipelineTupleQueue,
                              final PipelineReplicaMeter meter,
                              final OperatorReplicaStatus status,
                              final UpstreamContext pipelineUpstreamContext,
                              final TupleQueueDrainer upstreamDrainer )
    {
        this( config, id, operators, pipelineTupleQueue, meter );
        this.status = status;
        this.pipelineUpstreamContext = pipelineUpstreamContext;
        this.upstreamDrainer = upstreamDrainer;
    }

    public void init ( final SchedulingStrategy[] schedulingStrategies, final UpstreamContext[] upstreamContexts )
    {
        checkState( status == INITIAL, "Cannot initialize PipelineReplica %s as it is in %s state", id, status );
        checkArgument( schedulingStrategies != null && schedulingStrategies.length == operatorCount,
                       "Cannot initialize PipelineReplica %s because of invalid scheduling strategies",
                       id,
                       schedulingStrategies );
        checkArgument( upstreamContexts != null && upstreamContexts.length == operatorCount,
                       "Cannot initialize PipelineReplica %s because of invalid upstream contexts",
                       id,
                       upstreamContexts );

        initUpstreamDrainer();

        for ( int i = 0; i < operatorCount; i++ )
        {
            try
            {
                final OperatorReplica operator = operators[ i ];
                final UpstreamContext selfUpstreamContext = ( i < operatorCount - 1 ) ? upstreamContexts[ i + 1 ] : null;
                operator.init( upstreamContexts[ i ], selfUpstreamContext );
            }
            catch ( InitializationException e )
            {
                shutdownOperators();
                status = SHUT_DOWN;
                throw e;
            }
        }

        setPipelineUpstreamContext( upstreamContexts[ 0 ] );

        status = RUNNING;
    }

    private void initUpstreamDrainer ()
    {
        checkState( this.upstreamDrainer == null, "upstream drainer already initialized for %s", id );
        this.upstreamDrainer = ( pipelineTupleQueue instanceof EmptyOperatorTupleQueue )
                               ? new NopDrainer()
                               : new BlockingGreedyDrainer( pipelineInputPortCount );
    }

    void setPipelineUpstreamContext ( final UpstreamContext pipelineUpstreamContext )
    {
        if ( pipelineUpstreamContext == this.pipelineUpstreamContext )
        {
            return;
        }

        this.pipelineUpstreamContext = pipelineUpstreamContext;
        LOGGER.info( "{} handled new {}", id, pipelineUpstreamContext );
    }

    public UpstreamContext getPipelineUpstreamContext ()
    {
        return pipelineUpstreamContext;
    }

    public OperatorTupleQueue getPipelineTupleQueue ()
    {
        return pipelineTupleQueue instanceof EmptyOperatorTupleQueue ? operators[ 0 ].getQueue() : pipelineTupleQueue;
    }

    public OperatorTupleQueue getSelfPipelineTupleQueue ()
    {
        return pipelineTupleQueue;
    }

    public TuplesImpl invoke ()
    {
        meter.tick();
        pipelineInputTuplesSupplier.reset();
        pipelineTupleQueue.drain( drainerMaySkipBlocking, upstreamDrainer, pipelineInputTuplesSupplier );

        TuplesImpl tuples = operators[ 0 ].invoke( drainerMaySkipBlocking,
                                                   pipelineInputTuplesSupplier.getTuples(),
                                                   pipelineUpstreamContext );
        boolean invoked = ( tuples != null );
        for ( int i = 1; i < operatorCount; i++ )
        {
            tuples = operators[ i ].invoke( drainerMaySkipBlocking, tuples, operators[ i - 1 ].getSelfUpstreamContext() );
            invoked |= ( tuples != null );
        }

        drainerMaySkipBlocking = invoked;

        return tuples;
    }

    public void shutdown ()
    {
        if ( status == SHUT_DOWN )
        {
            return;
        }

        checkState( status == RUNNING, "Cannot shutdown PipelineReplica %s as it is in %s status", id, status );
        shutdownOperators();
        status = SHUT_DOWN;
        LOGGER.info( "Pipeline Replica {} is shut down", id );
    }

    private void shutdownOperators ()
    {
        for ( int i = 0; i < operatorCount; i++ )
        {
            final OperatorReplica operator = operators[ i ];
            try
            {
                if ( operator.getStatus() != INITIAL )
                {
                    operator.shutdown();
                }
            }
            catch ( Exception e )
            {
                LOGGER.error( "Shutdown of " + operator.getOperatorName() + " failed.", e );
            }
        }
    }

    public PipelineReplica duplicate ( final PipelineReplicaMeter meter, final OperatorReplica[] operators )
    {
        checkState( this.status == RUNNING, "Cannot duplicate pipeline replica %s because in %s status", this.id, this.status );

        final PipelineReplica duplicate = new PipelineReplica( this.config,
                                                               this.id,
                                                               operators,
                                                               this.pipelineTupleQueue,
                                                               meter,
                                                               this.status,
                                                               this.pipelineUpstreamContext,
                                                               this.upstreamDrainer );

        final List<String> operatorNames = Arrays.stream( operators ).map( o -> o.getOperatorDef().getId() ).collect( toList() );

        LOGGER.debug( "Pipeline {} is duplicated with {} operators: {}", this.id, operators.length, operatorNames );

        return duplicate;
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

    public OperatorDef getOperatorDef ( final int index )
    {
        return getOperator( index ).getOperatorDef();
    }

    public SchedulingStrategy getSchedulingStrategy ( final int index )
    {
        return getOperator( index ).getSchedulingStrategy();
    }

    public OperatorReplica[] getOperators ()
    {
        return Arrays.copyOf( operators, operators.length );
    }

    public boolean isCompleted ()
    {
        return pipelineReplicaCompletionTracker.isPipelineCompleted();
    }

    public OperatorReplicaStatus getStatus ()
    {
        return status;
    }

    public PipelineReplicaMeter getMeter ()
    {
        return meter;
    }

    PipelineReplicaCompletionTracker getPipelineReplicaCompletionTracker ()
    {
        return pipelineReplicaCompletionTracker;
    }

    @Override
    public String toString ()
    {
        return "PipelineReplica{" + "id=" + id + ", status=" + status + ", pipelineTupleQueue=" + pipelineTupleQueue.getClass()
                                                                                                                    .getSimpleName()
               + ", upstreamDrainer=" + ( upstreamDrainer != null ? upstreamDrainer.getClass().getSimpleName() : null )
               + ", pipelineUpstreamContext=" + pipelineUpstreamContext + ", operators=" + Arrays.toString( operators ) + '}';
    }

    private static class PipelineInputTuplesSupplier implements Function<PartitionKey, TuplesImpl>
    {

        private final TuplesImpl tuples;

        private boolean applied;

        PipelineInputTuplesSupplier ( final int portCount )
        {
            this.tuples = new TuplesImpl( portCount );
        }

        @Override
        public TuplesImpl apply ( final PartitionKey key )
        {
            checkState( !applied );
            applied = true;
            return tuples;
        }

        public TuplesImpl getTuples ()
        {
            return applied ? tuples : null;
        }

        public void reset ()
        {
            if ( applied )
            {
                tuples.clear();
                applied = false;
            }
        }

    }

}
