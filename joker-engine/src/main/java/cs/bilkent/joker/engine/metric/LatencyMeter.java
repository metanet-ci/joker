package cs.bilkent.joker.engine.metric;

import com.codahale.metrics.Histogram;

import cs.bilkent.joker.utils.Pair;

public class LatencyMeter
{

    private final String operatorId;

    private final int replicaIndex;

    private final Histogram histogram;

    public LatencyMeter ( final String operatorId, final int replicaIndex, final Histogram histogram )
    {
        this.operatorId = operatorId;
        this.replicaIndex = replicaIndex;
        this.histogram = histogram;
    }

    public String getOperatorId ()
    {
        return operatorId;
    }

    public int getReplicaIndex ()
    {
        return replicaIndex;
    }

    public Pair<String, Integer> getKey ()
    {
        return Pair.of( operatorId, replicaIndex );
    }

    public void record ( final long latency )
    {
        histogram.update( latency );
    }

    public Histogram getHistogram ()
    {
        return histogram;
    }

    @Override
    public String toString ()
    {
        return "LatencyMeter{" + "operatorId='" + operatorId + '\'' + ", replicaIndex=" + replicaIndex + ", histogram=" + histogram + '}';
    }

}
