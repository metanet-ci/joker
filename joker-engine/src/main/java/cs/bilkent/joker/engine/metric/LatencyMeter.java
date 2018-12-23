package cs.bilkent.joker.engine.metric;

import java.util.AbstractMap.SimpleEntry;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

import org.HdrHistogram.IntCountsHistogram;

import cs.bilkent.joker.engine.metric.LatencyMetrics.LatencyRecord;
import cs.bilkent.joker.operator.utils.Pair;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toMap;

public class LatencyMeter
{

    private final String sinkOperatorId;

    private final int replicaIndex;

    private IntCountsHistogram tupleLatency;

    private final Map<String, IntCountsHistogram> invocationLatencies = new HashMap<>();

    private final Map<String, IntCountsHistogram> queueLatencies = new HashMap<>();

    private final Map<String, IntCountsHistogram> interArrivalTimes = new HashMap<>();

    private final AtomicReference<LatencyMetrics> ref = new AtomicReference<>();

    public LatencyMeter ( final String sinkOperatorId, final int replicaIndex, final Set<String> operatorIds )
    {
        this.sinkOperatorId = sinkOperatorId;
        this.replicaIndex = replicaIndex;
        this.tupleLatency = newHistogram();
        for ( String operatorId : operatorIds )
        {
            invocationLatencies.put( operatorId, newHistogram() );
            queueLatencies.put( operatorId, newHistogram() );
            interArrivalTimes.put( operatorId, newHistogram() );
        }
    }

    private IntCountsHistogram newHistogram ()
    {
        return new IntCountsHistogram( SECONDS.toNanos( 10 ), 4 );
    }

    public String getSinkOperatorId ()
    {
        return sinkOperatorId;
    }

    public int getReplicaIndex ()
    {
        return replicaIndex;
    }

    public Pair<String, Integer> getKey ()
    {
        return Pair.of( sinkOperatorId, replicaIndex );
    }

    public void recordTuple ( final long latency )
    {
        if ( latency > 0 )
        {
            tupleLatency.recordValue( latency );
        }
    }

    public void recordInvocation ( final String operatorId, final long latency )
    {
        if ( latency > 0 )
        {
            invocationLatencies.get( operatorId ).recordValue( latency );
        }
    }

    public void recordQueue ( final String operatorId, final long latency )
    {
        if ( latency > 0 )
        {
            queueLatencies.get( operatorId ).recordValue( latency );
        }
    }

    public void recordInterArrivalTime ( final String operatorId, final long duration )
    {
        if ( duration > 0 )
        {
            interArrivalTimes.get( operatorId ).recordValue( duration );
        }
    }

    public boolean publish ()
    {
        final LatencyRecord tupleLatency = toLatencyRecord( new SimpleEntry<>( null, this.tupleLatency ) ).getValue();
        final Map<String, LatencyRecord> invocationLatencies = this.invocationLatencies.entrySet()
                                                                                       .stream()
                                                                                       .map( this::toLatencyRecord )
                                                                                       .collect( toMap( Entry::getKey, Entry::getValue ) );

        final Map<String, LatencyRecord> queueLatencies = this.queueLatencies.entrySet()
                                                                             .stream()
                                                                             .map( this::toLatencyRecord )
                                                                             .collect( toMap( Entry::getKey, Entry::getValue ) );

        final Map<String, LatencyRecord> interArrivalTimes = this.interArrivalTimes.entrySet()
                                                                                   .stream()
                                                                                   .map( this::toLatencyRecord )
                                                                                   .collect( toMap( Entry::getKey, Entry::getValue ) );

        final LatencyMetrics m = new LatencyMetrics( sinkOperatorId,
                                                     replicaIndex,
                                                     0,
                                                     tupleLatency,
                                                     invocationLatencies,
                                                     queueLatencies,
                                                     interArrivalTimes );

        final Set<String> operatorIds = invocationLatencies.keySet();
        this.tupleLatency = newHistogram();
        operatorIds.forEach( operatorId -> {
            this.invocationLatencies.put( operatorId, newHistogram() );
            this.queueLatencies.put( operatorId, newHistogram() );
            this.interArrivalTimes.put( operatorId, newHistogram() );
        } );

        return ref.getAndSet( m ) != null;
    }

    public LatencyMetrics toLatencyMetrics ( final int flowVersion )
    {
        LatencyMetrics m;
        while ( ( m = ref.getAndSet( null ) ) == null )
        {
            LockSupport.parkNanos( 1 );
        }

        return new LatencyMetrics( sinkOperatorId,
                                   replicaIndex,
                                   flowVersion,
                                   m.getTupleLatency(),
                                   m.getInvocationLatencies(),
                                   m.getQueueLatencies(),
                                   m.getInterArrivalTimes() );
    }

    private Entry<String, LatencyRecord> toLatencyRecord ( Entry<String, IntCountsHistogram> e )
    {
        final IntCountsHistogram histogram = e.getValue();
        return new SimpleEntry<>( e.getKey(),
                                  new LatencyRecord( (long) histogram.getMean(),
                                                     (long) histogram.getStdDeviation(),
                                                     histogram.getValueAtPercentile( 50 ),
                                                     histogram.getMinValue(),
                                                     histogram.getMaxValue(),
                                                     histogram.getValueAtPercentile( 75 ),
                                                     histogram.getValueAtPercentile( 95 ),
                                                     histogram.getValueAtPercentile( 99 ) ) );
    }


    @Override
    public String toString ()
    {
        return "LatencyMeter{" + "sinkOperatorId='" + sinkOperatorId + '\'' + ", replicaIndex=" + replicaIndex + ", tupleLatency="
               + tupleLatency + ", invocationLatencies=" + invocationLatencies + ", queueLatencies=" + queueLatencies
               + ", interArrivalTimes=" + interArrivalTimes + '}';
    }
}
