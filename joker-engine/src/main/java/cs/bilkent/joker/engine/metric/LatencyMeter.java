package cs.bilkent.joker.engine.metric;

import java.util.AbstractMap.SimpleEntry;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

import org.HdrHistogram.IntCountsHistogram;

import cs.bilkent.joker.engine.metric.LatencyMetrics.LatencyRec;
import cs.bilkent.joker.operator.utils.Pair;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toMap;

public class LatencyMeter
{

    private final String sinkOperatorId;

    private final int replicaIndex;

    private IntCountsHistogram tupleLatency;

    private final Map<String, IntCountsHistogram> serviceTimes = new HashMap<>();

    private final Map<String, IntCountsHistogram> queueWaitingTimes = new HashMap<>();

    private final Map<String, IntCountsHistogram> interArrivalTimes = new HashMap<>();

    private final AtomicReference<LatencyMetrics> ref = new AtomicReference<>();

    public LatencyMeter ( final String sinkOperatorId, final int replicaIndex, final Set<String> operatorIds )
    {
        this.sinkOperatorId = sinkOperatorId;
        this.replicaIndex = replicaIndex;
        this.tupleLatency = newWideHistogram();
        for ( String operatorId : operatorIds )
        {
            serviceTimes.put( operatorId, newNarrowHistogram() );
            queueWaitingTimes.put( operatorId, newWideHistogram() );
            interArrivalTimes.put( operatorId, newNarrowHistogram() );
        }
    }

    private IntCountsHistogram newWideHistogram ()
    {
        return new IntCountsHistogram( SECONDS.toNanos( 30 ), 1 );
    }

    private IntCountsHistogram newNarrowHistogram ()
    {
        return new IntCountsHistogram( SECONDS.toNanos( 5 ), 1 );
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

    public void recordServiceTime ( final String operatorId, final long latency )
    {
        serviceTimes.get( operatorId ).recordValue( latency );
    }

    public void recordQueueWaitingTime ( final String operatorId, final long latency )
    {
        queueWaitingTimes.get( operatorId ).recordValue( latency );
    }

    public void recordInterArrivalTime ( final String operatorId, final long duration, final int times )
    {
        for ( int i = 0; i < times; i++ )
        {
            interArrivalTimes.get( operatorId ).recordValue( duration );
        }
    }

    public boolean publish ()
    {
        final LatencyRec tupleLatency = toLatencyRecord( new SimpleEntry<>( null, this.tupleLatency ) ).getValue();
        final Map<String, LatencyRec> serviceTimes = this.serviceTimes.entrySet()
                                                                      .stream()
                                                                      .map( this::toLatencyRecord )
                                                                      .collect( toMap( Entry::getKey, Entry::getValue ) );

        final Map<String, LatencyRec> queueWaitingTimes = this.queueWaitingTimes.entrySet()
                                                                                .stream()
                                                                                .map( this::toLatencyRecord )
                                                                                .collect( toMap( Entry::getKey, Entry::getValue ) );

        final Map<String, LatencyRec> interArrivalTimes = this.interArrivalTimes.entrySet()
                                                                                .stream()
                                                                                .map( this::toLatencyRecord )
                                                                                .collect( toMap( Entry::getKey, Entry::getValue ) );

        final LatencyMetrics m = new LatencyMetrics( sinkOperatorId,
                                                     replicaIndex,
                                                     0,
                                                     tupleLatency, serviceTimes, queueWaitingTimes,
                                                     interArrivalTimes );

        final Set<String> operatorIds = serviceTimes.keySet();
        this.tupleLatency = newWideHistogram();
        operatorIds.forEach( operatorId -> {
            this.serviceTimes.put( operatorId, newNarrowHistogram() );
            this.queueWaitingTimes.put( operatorId, newWideHistogram() );
            this.interArrivalTimes.put( operatorId, newNarrowHistogram() );
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
                                   m.getTupleLatency(), m.getServiceTimes(), m.getQueueWaitingTimes(),
                                   m.getInterArrivalTimes() );
    }

    private Entry<String, LatencyRec> toLatencyRecord ( Entry<String, IntCountsHistogram> e )
    {
        final IntCountsHistogram histogram = e.getValue();
        return new SimpleEntry<>( e.getKey(),
                                  new LatencyRec( (long) histogram.getMean(),
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
               + tupleLatency + ", serviceTimes=" + serviceTimes + ", queueWaitingTimes=" + queueWaitingTimes + ", interArrivalTimes="
               + interArrivalTimes + '}';
    }
}
