package cs.bilkent.joker.engine.metric;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private final boolean samplingEnabled;

    private final AvgCalculator invocationTupleCounts = new AvgCalculator();

    private volatile Object currentlyInvokedOperator;

    private long lastOperatorInputTuplesReportTime = System.nanoTime();

    public PipelineReplicaMeter ( final long tickMask, final PipelineReplicaId pipelineReplicaId, final OperatorDef headOperatorDef )
    {
        this.pipelineReplicaId = pipelineReplicaId;
        this.ticker = new Ticker( tickMask );
        this.headOperatorId = headOperatorDef.getId();
        this.inputPortCount = headOperatorDef.getInputPortCount();
        this.inboundThroughput = new long[ inputPortCount ];
        this.samplingEnabled = ( inputPortCount > 0 );
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

        final boolean ticked = ticker.tryTick();
        if ( ticked )
        {
            currentlyInvokedOperator = pipelineReplicaId;
            if ( inputPortCount > 0 )
            {
                reportOperatorInputTupleCounts();
            }
        }

        return ticked;
    }

    private void reportOperatorInputTupleCounts ()
    {
        final long now = System.nanoTime();
        if ( ( now - lastOperatorInputTuplesReportTime ) >= TimeUnit.SECONDS.toNanos( 1 ) )
        {
            lastOperatorInputTuplesReportTime = now;

            LOGGER.info( "{} => INPUT TUPLE COUNTS operator: {} -> average: {}",
                         pipelineReplicaId,
                         headOperatorId,
                         invocationTupleCounts.getAvg() );
            invocationTupleCounts.reset();
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
        assert operatorId != null;

        if ( ticker.isTicked() )
        {
            currentlyInvokedOperator = operatorId;
        }
    }

    public int onInvocationComplete ( final String operatorId,
                                      final List<TuplesImpl> inputs,
                                      final int inputCount,
                                      final boolean countAnyway )
    {
        assert operatorId != null;

        if ( ticker.isTicked() )
        {
            currentlyInvokedOperator = pipelineReplicaId;
        }

        assert inputs != null;

        if ( !headOperatorId.equals( operatorId ) )
        {
            int total = 0;
            if ( countAnyway )
            {
                for ( int i = 0; i < inputCount; i++ )
                {
                    final TuplesImpl tuples = inputs.get( i );
                    for ( int j = 0; j < tuples.getPortCount(); j++ )
                    {
                        total += tuples.getTupleCount( j );
                    }
                }
            }

            return total;
        }

        int total = 0;

        for ( int i = 0; i < inputCount; i++ )
        {
            final TuplesImpl tuples = inputs.get( i );

            for ( int j = 0; j < inputPortCount; j++ )
            {
                final int tupleCount = tuples.getTupleCount( j );
                inboundThroughput[ j ] += tupleCount;
                total += tupleCount;
            }
        }

        if ( samplingEnabled && ticker.isTicked() )
        {
            invocationTupleCounts.record( total );
        }

        return total;
    }

    void readInboundThroughput ( final long[] inboundThroughput )
    {
        assert inboundThroughput.length == inputPortCount;

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
