package cs.bilkent.joker.engine.pipeline;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.IntStream;
import javax.annotation.concurrent.NotThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import cs.bilkent.joker.engine.exception.InitializationException;
import cs.bilkent.joker.engine.flow.RegionDef;
import cs.bilkent.joker.engine.metric.PipelineReplicaMeter;
import static cs.bilkent.joker.engine.pipeline.OperatorReplicaStatus.INITIAL;
import static cs.bilkent.joker.engine.pipeline.OperatorReplicaStatus.RUNNING;
import static cs.bilkent.joker.engine.pipeline.OperatorReplicaStatus.SHUT_DOWN;
import cs.bilkent.joker.engine.tuplequeue.OperatorQueue;
import cs.bilkent.joker.engine.tuplequeue.TupleQueueDrainer;
import cs.bilkent.joker.engine.tuplequeue.impl.drainer.BlockingGreedyDrainer;
import cs.bilkent.joker.engine.tuplequeue.impl.drainer.NopDrainer;
import cs.bilkent.joker.engine.tuplequeue.impl.operator.EmptyOperatorQueue;
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


    private final PipelineReplicaId id;

    private final OperatorReplica[] operators;

    private final OperatorQueue queue;

    private final PipelineReplicaMeter meter;

    private final int pipelineInputPortCount;

    private final PipelineInputTuplesSupplier inputTuplesSupplier;

    private final PipelineReplicaCompletionTracker completionTracker;

    private TupleQueueDrainer drainer;

    private OperatorReplicaStatus status = INITIAL;

    private UpstreamCtx upstreamCtx;

    private boolean drainerMaySkipBlocking = true;

    public PipelineReplica ( final PipelineReplicaId id, final OperatorReplica[] operators, final OperatorQueue queue,
                             final PipelineReplicaMeter meter )
    {
        this.id = id;
        this.operators = Arrays.copyOf( operators, operators.length );
        this.queue = queue;
        this.meter = meter;
        this.pipelineInputPortCount = operators[ 0 ].getOperatorDef( 0 ).getInputPortCount();
        this.inputTuplesSupplier = new PipelineInputTuplesSupplier( pipelineInputPortCount );
        this.completionTracker = new PipelineReplicaCompletionTracker( id, operators );
        for ( OperatorReplica operator : operators )
        {
            operator.setOperatorReplicaListener( this.completionTracker );
        }
    }

    private PipelineReplica ( final PipelineReplicaId id,
                              final OperatorReplica[] operators,
                              final OperatorQueue queue, final PipelineReplicaMeter meter, final UpstreamCtx upstreamCtx )
    {
        this( id, operators, queue, meter );
        initUpstreamDrainer();
        this.status = RUNNING;
        setUpstreamCtx( upstreamCtx );
    }

    // constructor for pipeline replica duplication
    private PipelineReplica ( final PipelineReplicaId id,
                              final OperatorReplica[] operators,
                              final OperatorQueue queue,
                              final PipelineReplicaMeter meter,
                              final OperatorReplicaStatus status,
                              final UpstreamCtx upstreamCtx,
                              final TupleQueueDrainer drainer )
    {
        this( id, operators, queue, meter );
        this.status = status;
        this.upstreamCtx = upstreamCtx;
        this.drainer = drainer;
    }

    public void init ( final SchedulingStrategy[][] schedulingStrategies, final UpstreamCtx[][] upstreamCtxes )
    {
        checkState( status == INITIAL, "Cannot initialize PipelineReplica %s as it is %s", id, status );
        checkArgument( schedulingStrategies != null && schedulingStrategies.length == operators.length,
                       "Cannot initialize PipelineReplica %s because of invalid scheduling strategies",
                       id,
                       schedulingStrategies );
        checkArgument( upstreamCtxes != null && upstreamCtxes.length == operators.length,
                       "Cannot initialize PipelineReplica %s because of invalid upstream contexts", id, upstreamCtxes );

        initUpstreamDrainer();

        for ( int i = 0; i < operators.length; i++ )
        {
            try
            {
                final OperatorReplica operator = operators[ i ];
                final UpstreamCtx downstreamCtx = ( i < operators.length - 1 ) ? upstreamCtxes[ i + 1 ][ 0 ] : null;
                final SchedulingStrategy[] operatorSchedulingStrategies = operator.init( upstreamCtxes[ i ], downstreamCtx );
                checkState( Arrays.equals( schedulingStrategies[ i ], operatorSchedulingStrategies ) );
            }
            catch ( InitializationException e )
            {
                shutdownOperators();
                status = SHUT_DOWN;
                throw e;
            }
        }

        setUpstreamCtx( upstreamCtxes[ 0 ][ 0 ] );

        status = RUNNING;
    }

    private void initUpstreamDrainer ()
    {
        checkState( this.drainer == null, "upstream drainer already initialized for %s", id );
        // TODO consider open / closed ports...
        this.drainer = ( queue instanceof EmptyOperatorQueue ) ? new NopDrainer() : new BlockingGreedyDrainer( pipelineInputPortCount );
    }

    void setUpstreamCtx ( final UpstreamCtx upstreamCtx )
    {
        if ( upstreamCtx == this.upstreamCtx )
        {
            return;
        }

        this.upstreamCtx = upstreamCtx;
        LOGGER.info( "{} handled new {}", id, upstreamCtx );
    }

    public UpstreamCtx getUpstreamCtx ()
    {
        return upstreamCtx;
    }

    public OperatorQueue getQueue ()
    {
        return queue;
    }

    public OperatorQueue getEffectiveQueue ()
    {
        return queue instanceof EmptyOperatorQueue ? operators[ 0 ].getQueue() : queue;
    }

    public TuplesImpl invoke ()
    {
        meter.tryTick();
        inputTuplesSupplier.reset();
        queue.drain( drainerMaySkipBlocking, drainer, inputTuplesSupplier );

        TuplesImpl tuples = operators[ 0 ].invoke( drainerMaySkipBlocking, inputTuplesSupplier.getTuples(), upstreamCtx );
        boolean invoked = ( tuples != null );
        for ( int i = 1; i < operators.length; i++ )
        {
            tuples = operators[ i ].invoke( drainerMaySkipBlocking, tuples, operators[ i - 1 ].getDownstreamCtx() );
            invoked |= ( tuples != null );
        }

        drainerMaySkipBlocking = invoked;

        return tuples;
    }

    public boolean isInvoked ()
    {
        return drainerMaySkipBlocking;
    }

    public void shutdown ()
    {
        if ( status == SHUT_DOWN )
        {
            return;
        }

        checkState( status == RUNNING, "Cannot shutdown PipelineReplica %s as it is %s", id, status );
        shutdownOperators();
        status = SHUT_DOWN;
        LOGGER.info( "Pipeline Replica {} is shut down", id );
    }

    private void shutdownOperators ()
    {
        for ( final OperatorReplica operator : operators )
        {
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

    // used during parallelisation changes
    public PipelineReplica duplicate ( final PipelineReplicaMeter meter, final OperatorReplica[] operators )
    {
        checkState( this.status == RUNNING, "Cannot duplicate pipeline replica %s because in %s status", this.id, this.status );

        final PipelineReplica duplicate = new PipelineReplica( this.id,
                                                               operators,
                                                               this.queue,
                                                               meter, this.status, this.upstreamCtx,
                                                               this.drainer );

        final List<String> operatorNames = Arrays.stream( operators )
                                                 .flatMap( operatorReplica -> IntStream.range( 0, operatorReplica.getOperatorCount() )
                                                                                       .mapToObj( operatorReplica::getOperatorDef ) )
                                                 .map( OperatorDef::getId )
                                                 .collect( toList() );

        LOGGER.debug( "Pipeline {} is duplicated with {} operators: {}", this.id, operators.length, operatorNames );

        return duplicate;
    }

    public static PipelineReplica running ( final PipelineReplicaId id,
                                            final OperatorReplica[] operators,
                                            final OperatorQueue pipelineQueue,
                                            final PipelineReplicaMeter meter,
                                            final UpstreamCtx upstreamCtx )
    {
        return new PipelineReplica( id, operators, pipelineQueue, meter, upstreamCtx );
    }

    public PipelineReplicaId id ()
    {
        return id;
    }

    // used during parallelisation changes
    public int getOperatorCount ()
    {
        return Arrays.stream( operators ).mapToInt( OperatorReplica::getOperatorCount ).sum();
    }

    public int getOperatorReplicaCount ()
    {
        return operators.length;
    }

    // used during parallelisation changes
    public OperatorReplica getOperatorReplica ( final int index )
    {
        return operators[ index ];
    }

    // used during parallelisation changes
    public OperatorReplica[] getOperators ()
    {
        return Arrays.copyOf( operators, operators.length );
    }

    public boolean isCompleted ()
    {
        return completionTracker.isPipelineCompleted();
    }

    public OperatorReplicaStatus getStatus ()
    {
        return status;
    }

    public PipelineReplicaMeter getMeter ()
    {
        return meter;
    }

    PipelineReplicaCompletionTracker getCompletionTracker ()
    {
        return completionTracker;
    }

    @Override
    public String toString ()
    {
        return "PipelineReplica{" + "id=" + id + ", status=" + status + ", queue=" + queue.getClass().getSimpleName() + ", drainer=" + (
                drainer != null ? drainer.getClass().getSimpleName() : null ) + ", upstreamCtx=" + upstreamCtx + ", operators="
               + Arrays.toString( operators ) + '}';
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
