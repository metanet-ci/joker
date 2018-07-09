package cs.bilkent.joker.engine.metric;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.SlidingTimeWindowReservoir;
import com.codahale.metrics.Snapshot;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import cs.bilkent.joker.engine.pipeline.PipelineReplicaId;
import cs.bilkent.joker.operator.OperatorDef;
import cs.bilkent.joker.operator.Tuples;
import cs.bilkent.joker.operator.impl.TuplesImpl;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class PipelineReplicaMeter
{

    private final AtomicReference<Object> currentlyInvokedOperator = new AtomicReference<>();

    private final Ticker ticker;

    private final PipelineReplicaId pipelineReplicaId;

    private final String headOperatorId;

    private final int inputPortCount;

    private final long[] inboundThroughput;

    private final Histogram[] inboundThroughputHistograms;

    public PipelineReplicaMeter ( final long tickMask, final PipelineReplicaId pipelineReplicaId, final OperatorDef headOperatorDef )
    {
        this.ticker = new Ticker( tickMask );
        this.pipelineReplicaId = pipelineReplicaId;
        this.headOperatorId = headOperatorDef.getId();
        this.inputPortCount = headOperatorDef.getInputPortCount();
        this.inboundThroughput = new long[ inputPortCount ];
        this.inboundThroughputHistograms = new Histogram[ inputPortCount ];
        for ( int i = 0; i < inputPortCount; i++ )
        {
            this.inboundThroughputHistograms[ i ] = new Histogram( new SlidingTimeWindowReservoir( 1000, MILLISECONDS ) );
        }
    }

    public PipelineReplicaId getPipelineReplicaId ()
    {
        return pipelineReplicaId;
    }

    public void tryTick ()
    {
        if ( ticker.isTicked() )
        {
            casOrFail( pipelineReplicaId, null );
        }

        if ( ticker.tryTick() )
        {
            casOrFail( null, pipelineReplicaId );
        }
    }

    public String getHeadOperatorId ()
    {
        return headOperatorId;
    }

    public int getInputPortCount ()
    {
        return inputPortCount;
    }

    public void readInboundThroughput ( final long[] inboundThroughput )
    {
        checkArgument( inboundThroughput.length == inputPortCount );

        for ( int i = 0; i < inputPortCount; i++ )
        {
            inboundThroughput[ i ] = this.inboundThroughput[ i ];
        }
    }

    public boolean isTicked ()
    {
        return ticker.isTicked();
    }

    public void onInvocationStart ( final String operatorId )
    {
        checkNotNull( operatorId );

        if ( ticker.isTicked() )
        {
            casOrFail( pipelineReplicaId, operatorId );
        }
    }

    public void count ( final String operatorId, final TuplesImpl tuples )
    {
        checkNotNull( operatorId );
        checkNotNull( tuples );

        if ( !headOperatorId.equals( operatorId ) )
        {
            return;
        }

        for ( int i = 0; i < inputPortCount; i++ )
        {
            final int tupleCount = tuples.getTupleCount( i );
            inboundThroughputHistograms[ i ].update( tupleCount );
            inboundThroughput[ i ] += tupleCount;
        }
    }

    public void count ( final String operatorId, final List<TuplesImpl> tuplesList, final int count )
    {
        checkNotNull( operatorId );
        checkNotNull( tuplesList );

        if ( !headOperatorId.equals( operatorId ) )
        {
            return;
        }

        for ( int i = 0; i < count; i++ )
        {
            final Tuples tuples = tuplesList.get( i );
            for ( int j = 0; j < inputPortCount; j++ )
            {
                final int tupleCount = tuples.getTupleCount( j );
                inboundThroughputHistograms[ j ].update( tupleCount );
                inboundThroughput[ j ] += tupleCount;
            }
        }
    }

    public void onInvocationComplete ( final String operatorId )
    {
        checkNotNull( operatorId );

        if ( ticker.isTicked() )
        {
            casOrFail( operatorId, pipelineReplicaId );
        }
    }

    Object getCurrentlyExecutingComponent ()
    {
        return currentlyInvokedOperator.get();
    }

    Snapshot[] getInboundThroughputHistograms ()
    {
        return Arrays.stream( inboundThroughputHistograms ).map( Histogram::getSnapshot ).toArray( Snapshot[]::new );
    }

    private void casOrFail ( final Object currentVal, final Object nextVal )
    {
        final boolean success = currentlyInvokedOperator.compareAndSet( currentVal, nextVal );
        checkState( success, "cannot set ref from %s to %s in pipeline replica meter of %s", currentVal, nextVal, pipelineReplicaId );
    }

    static class Ticker
    {

        private final long tickMask;

        private long count;

        private boolean ticked;

        Ticker ( final long tickMask )
        {
            this.tickMask = tickMask;
        }

        boolean tryTick ()
        {
            return ( this.ticked = ( ( ++count & tickMask ) == 0 ) );
        }

        boolean isTicked ()
        {
            return ticked;
        }

        long getCount ()
        {
            return count;
        }

        void reset ()
        {
            count = 0;
            ticked = false;
        }

    }

}
