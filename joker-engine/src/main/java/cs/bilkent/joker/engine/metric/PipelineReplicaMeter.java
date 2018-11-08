package cs.bilkent.joker.engine.metric;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import cs.bilkent.joker.engine.pipeline.PipelineReplicaId;
import cs.bilkent.joker.operator.OperatorDef;
import cs.bilkent.joker.operator.impl.TuplesImpl;

public class PipelineReplicaMeter
{

    private static final Logger LOGGER = LoggerFactory.getLogger( PipelineReplicaMeter.class );


    private final PipelineReplicaId pipelineReplicaId;

    private final Ticker ticker;

    private final String headOperatorId;

    private final int inputPortCount;

    private final long[] inboundThroughput;

    private final Map<String, AvgCalculator> invocationTupleCounts = new HashMap<>();

    private volatile Object currentlyInvokedOperator;

    private long lastOperatorInputTuplesReportTime = System.nanoTime();

    public PipelineReplicaMeter ( final long tickMask, final PipelineReplicaId pipelineReplicaId, final OperatorDef headOperatorDef )
    {
        this.pipelineReplicaId = pipelineReplicaId;
        this.ticker = new Ticker( tickMask );
        this.headOperatorId = headOperatorDef.getId();
        this.inputPortCount = headOperatorDef.getInputPortCount();
        this.inboundThroughput = new long[ inputPortCount ];
    }

    public PipelineReplicaId getPipelineReplicaId ()
    {
        return pipelineReplicaId;
    }

    public boolean tryTick ()
    {
        if ( ticker.isTicked() )
        {
            currentlyInvokedOperator = null;
        }

        if ( ticker.tryTick() )
        {
            currentlyInvokedOperator = pipelineReplicaId;
            reportOperatorInputTupleCounts();

            return true;
        }

        return false;
    }

    private void reportOperatorInputTupleCounts ()
    {
        final long now = System.nanoTime();
        if ( ( now - lastOperatorInputTuplesReportTime ) >= TimeUnit.SECONDS.toNanos( 1 ) )
        {
            lastOperatorInputTuplesReportTime = now;

            for ( Entry<String, AvgCalculator> e : invocationTupleCounts.entrySet() )
            {
                final AvgCalculator avgCalculator = e.getValue();
                LOGGER.info( "{} => INPUT TUPLE COUNTS operator: {} -> average: {}",
                             pipelineReplicaId,
                             e.getKey(),
                             avgCalculator.getAvg() );
            }

            invocationTupleCounts.values().forEach( AvgCalculator::reset );
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

    public boolean isTicked ()
    {
        return ticker.isTicked();
    }

    public boolean isTicked ( final long tickMask )
    {
        return ticker.isTicked( tickMask );
    }

    public void onInvocationStart ( final String operatorId )
    {
        checkNotNull( operatorId );

        if ( ticker.isTicked() )
        {
            currentlyInvokedOperator = operatorId;
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

        recordInvocationInputTupleCount( tuples );

        for ( int i = 0; i < inputPortCount; i++ )
        {
            final int tupleCount = tuples.getTupleCount( i );
            inboundThroughput[ i ] += tupleCount;
        }
    }

    private void recordInvocationInputTupleCount ( final TuplesImpl tuples )
    {
        // TODO works for only port = 0 ???
        if ( ticker.isTicked() && inputPortCount > 0 )
        {
            final AvgCalculator avgCalculator = invocationTupleCounts.computeIfAbsent( headOperatorId, op -> new AvgCalculator() );
            avgCalculator.record( tuples.getTupleCount( 0 ) );
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
            final TuplesImpl tuples = tuplesList.get( i );
            recordInvocationInputTupleCount( tuples );

            for ( int j = 0; j < inputPortCount; j++ )
            {
                final int tupleCount = tuples.getTupleCount( j );
                inboundThroughput[ j ] += tupleCount;
            }
        }
    }

    public void onInvocationComplete ( final String operatorId )
    {
        checkNotNull( operatorId );

        if ( ticker.isTicked() )
        {
            currentlyInvokedOperator = pipelineReplicaId;
        }
    }

    void readInboundThroughput ( final long[] inboundThroughput )
    {
        checkArgument( inboundThroughput.length == inputPortCount );

        for ( int i = 0; i < inputPortCount; i++ )
        {
            inboundThroughput[ i ] = this.inboundThroughput[ i ];
        }
    }

    Object getCurrentlyExecutingComponent ()
    {
        return currentlyInvokedOperator;
    }

    public static class Ticker
    {

        private final long tickMask;

        private long count;

        private boolean ticked;

        public Ticker ( final long tickMask )
        {
            this.tickMask = tickMask;
        }

        public boolean tryTick ()
        {
            return ( this.ticked = ( ( ++count & tickMask ) == 0 ) );
        }

        public boolean isTicked ()
        {
            return ticked;
        }

        public boolean isTicked ( final long tickMask )
        {
            return ( count & tickMask ) == 0;
        }

        public long getCount ()
        {
            return count;
        }

        public void reset ()
        {
            count = 0;
            ticked = false;
        }

    }


    private static class AvgCalculator
    {

        private int count;
        private long sum;

        void record ( final long value )
        {
            count++;
            sum += value;
        }

        double getAvg ()
        {
            return count > 0 ? ( (double) sum ) / count : 0;
        }

        private void reset ()
        {
            count = 0;
            sum = 0;
        }

    }
}
