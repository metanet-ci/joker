package cs.bilkent.joker.engine.metric;

import java.util.AbstractMap.SimpleEntry;
import java.util.Map;
import java.util.Map.Entry;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Snapshot;

import cs.bilkent.joker.engine.metric.LatencyMetrics.LatencyRecord;
import cs.bilkent.joker.operator.utils.Pair;
import static java.util.Collections.unmodifiableMap;
import static java.util.stream.Collectors.toMap;

public class LatencyMeter
{

    private final String sinkOperatorId;

    private final int replicaIndex;

    private final Map<String, Histogram> invocationLatencies;

    private final Map<String, Histogram> queueLatencies;

    private final Histogram tupleLatency;

    public LatencyMeter ( final String sinkOperatorId,
                          final int replicaIndex,
                          final Map<String, Histogram> invocationLatencies,
                          final Map<String, Histogram> queueLatencies,
                          final Histogram tupleLatency )
    {
        this.sinkOperatorId = sinkOperatorId;
        this.replicaIndex = replicaIndex;
        this.invocationLatencies = invocationLatencies;
        this.queueLatencies = queueLatencies;
        this.tupleLatency = tupleLatency;
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
            tupleLatency.update( latency );
        }
    }

    public void recordInvocation ( final String operatorId, final long latency )
    {
        if ( latency > 0 )
        {
            invocationLatencies.get( operatorId ).update( latency );
        }
    }

    public void recordQueue ( final String operatorId, final long latency )
    {
        if ( latency > 0 )
        {
            queueLatencies.get( operatorId ).update( latency );
        }
    }

    public Histogram getTupleLatency ()
    {
        return tupleLatency;
    }

    public Map<String, Histogram> getInvocationLatencies ()
    {
        return unmodifiableMap( invocationLatencies );
    }

    public Map<String, Histogram> getQueueLatencies ()
    {
        return unmodifiableMap( queueLatencies );
    }

    public LatencyMetrics toLatencyMetrics ( final int flowVersion )
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

        return new LatencyMetrics( sinkOperatorId, replicaIndex, flowVersion, tupleLatency, invocationLatencies, queueLatencies );
    }

    private Entry<String, LatencyRecord> toLatencyRecord ( Entry<String, Histogram> e )
    {
        final Histogram histogram = e.getValue();
        final Snapshot snapshot = histogram.getSnapshot();
        return new SimpleEntry<>( e.getKey(),
                                  new LatencyRecord( (long) snapshot.getMean(),
                                                     (long) snapshot.getStdDev(),
                                                     (long) snapshot.getMedian(),
                                                     snapshot.getMin(),
                                                     snapshot.getMax(),
                                                     (long) snapshot.get75thPercentile(),
                                                     (long) snapshot.get95thPercentile(),
                                                     (long) snapshot.get98thPercentile(),
                                                     (long) snapshot.get99thPercentile(),
                                                     (long) snapshot.get999thPercentile() ) );
    }


    @Override
    public String toString ()
    {
        return "LatencyMeter{" + "sinkOperatorId='" + sinkOperatorId + '\'' + ", replicaIndex=" + replicaIndex + ", invocationLatencies="
               + invocationLatencies + ", queueLatencies=" + queueLatencies + ", tupleLatency=" + tupleLatency + '}';
    }

}
