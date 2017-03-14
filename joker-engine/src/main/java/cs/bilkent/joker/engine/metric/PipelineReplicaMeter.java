package cs.bilkent.joker.engine.metric;

import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import cs.bilkent.joker.engine.pipeline.PipelineReplicaId;
import cs.bilkent.joker.operator.OperatorDef;
import cs.bilkent.joker.operator.Tuples;

public class PipelineReplicaMeter
{

    private final AtomicReference<Object> currentlyInvokedOperator = new AtomicReference<>();

    private final Ticker ticker;

    private final PipelineReplicaId pipelineReplicaId;

    private final String headOperatorId;

    private final int consumedPortCount;

    private final long[] consumedTupleCounts;

    public PipelineReplicaMeter ( final long tickMask,
                                  final PipelineReplicaId pipelineReplicaId, final OperatorDef headOperatorDef )
    {
        this.ticker = new Ticker( tickMask );
        this.pipelineReplicaId = pipelineReplicaId;
        this.headOperatorId = headOperatorDef.getId();
        this.consumedPortCount = headOperatorDef.getInputPortCount();
        this.consumedTupleCounts = new long[ consumedPortCount ];
    }

    public PipelineReplicaId getPipelineReplicaId ()
    {
        return pipelineReplicaId;
    }

    public void tick ()
    {
        if ( ticker.isTicked() )
        {
            casOrFail( pipelineReplicaId, null );
        }

        if ( ticker.tick() )
        {
            casOrFail( null, pipelineReplicaId );
        }
    }

    public void addConsumedTuples ( final String operatorId, final Tuples tuples )
    {
        if ( !headOperatorId.equals( operatorId ) )
        {
            return;
        }

        for ( int i = 0; i < consumedPortCount; i++ )
        {
            consumedTupleCounts[ i ] += tuples.getTupleCount( i );
        }
    }

    public String getHeadOperatorId ()
    {
        return headOperatorId;
    }

    public int getConsumedPortCount ()
    {
        return consumedPortCount;
    }

    public void getConsumedTupleCounts ( final long[] consumedTupleCounts )
    {
        checkArgument( consumedTupleCounts.length == consumedPortCount );

        for ( int i = 0; i < consumedPortCount; i++ )
        {
            consumedTupleCounts[ i ] = this.consumedTupleCounts[ i ];
        }
    }

    public boolean isTicked ()
    {
        return ticker.isTicked();
    }

    public void startOperatorInvocation ( final String operatorId )
    {
        checkNotNull( operatorId );
        if ( ticker.isTicked() )
        {
            casOrFail( pipelineReplicaId, operatorId );
        }
    }

    public void completeOperatorInvocation ( final String operatorId )
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

        boolean tick ()
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
