package cs.bilkent.joker.engine.metric;

import java.util.Map;

import com.codahale.metrics.Histogram;

import cs.bilkent.joker.operator.utils.Pair;
import static java.util.Collections.unmodifiableMap;

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
        tupleLatency.update( latency );
    }

    public void recordInvocation ( final String operatorId, final long latency )
    {
        invocationLatencies.get( operatorId ).update( latency );
    }

    public void recordQueue ( final String operatorId, final long latency )
    {
        queueLatencies.get( operatorId ).update( latency );
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

    @Override
    public String toString ()
    {
        return "LatencyMeter{" + "sinkOperatorId='" + sinkOperatorId + '\'' + ", replicaIndex=" + replicaIndex + ", invocationLatencies="
               + invocationLatencies + ", queueLatencies=" + queueLatencies + ", tupleLatency=" + tupleLatency + '}';
    }

}
