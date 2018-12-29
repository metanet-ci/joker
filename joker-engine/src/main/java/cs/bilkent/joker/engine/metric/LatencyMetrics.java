package cs.bilkent.joker.engine.metric;

import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.unmodifiableMap;

public class LatencyMetrics
{

    private final String sinkOperatorId;

    private final int replicaIndex;

    private final int flowVersion;

    private final LatencyRec tupleLatency;

    private final Map<String, LatencyRec> serviceTimes;

    private final Map<String, LatencyRec> queueWaitingTimes;

    private final Map<String, LatencyRec> interArrivalTimes;

    public LatencyMetrics ( final String sinkOperatorId,
                            final int replicaIndex,
                            final int flowVersion,
                            final LatencyRec tupleLatency,
                            final Map<String, LatencyRec> serviceTimes,
                            final Map<String, LatencyRec> queueWaitingTimes,
                            final Map<String, LatencyRec> interArrivalTimes )
    {
        this.sinkOperatorId = sinkOperatorId;
        this.replicaIndex = replicaIndex;
        this.flowVersion = flowVersion;
        this.tupleLatency = tupleLatency;
        this.serviceTimes = unmodifiableMap( new HashMap<>( serviceTimes ) );
        this.queueWaitingTimes = unmodifiableMap( new HashMap<>( queueWaitingTimes ) );
        this.interArrivalTimes = unmodifiableMap( new HashMap<>( interArrivalTimes ) );
    }

    public String getSinkOperatorId ()
    {
        return sinkOperatorId;
    }

    public int getReplicaIndex ()
    {
        return replicaIndex;
    }

    public int getFlowVersion ()
    {
        return flowVersion;
    }

    public LatencyRec getTupleLatency ()
    {
        return tupleLatency;
    }

    public long getTupleLatencyMean ()
    {
        return tupleLatency.mean;
    }

    public long getTupleLatencyStdDev ()
    {
        return tupleLatency.stdDev;
    }

    public long getTupleLatencyVariance ()
    {
        final long stdDev = getTupleLatencyStdDev();
        return stdDev * stdDev;
    }

    public Map<String, LatencyRec> getServiceTimes ()
    {
        return serviceTimes;
    }

    public Map<String, LatencyRec> getQueueWaitingTimes ()
    {
        return queueWaitingTimes;
    }

    public Map<String, LatencyRec> getInterArrivalTimes ()
    {
        return interArrivalTimes;
    }

    public LatencyRec getServiceTime ( final String operatorId )
    {
        return serviceTimes.get( operatorId );
    }

    public long getServiceTimeMean ( final String operatorId )
    {
        return getServiceTime( operatorId ).mean;
    }

    public long getServiceTimeStdDev ( final String operatorId )
    {
        return getServiceTime( operatorId ).stdDev;
    }

    public long getServiceTimeVar ( final String operatorId )
    {
        final long stdDev = getServiceTimeStdDev( operatorId );
        return stdDev * stdDev;
    }

    public LatencyRec getQueueWaitingTime ( final String operatorId )
    {
        return queueWaitingTimes.get( operatorId );
    }

    public long getQueueWaitingTimeMean ( final String operatorId )
    {
        return getQueueWaitingTime( operatorId ).mean;
    }

    public long getQueueWaitingTimeStdDev ( final String operatorId )
    {
        return getQueueWaitingTime( operatorId ).stdDev;
    }

    public long getQueueWaitingTimeVar ( final String operatorId )
    {
        final long stdDev = getQueueWaitingTimeStdDev( operatorId );
        return stdDev * stdDev;
    }

    public LatencyRec getInterArrivalTime ( final String operatorId )
    {
        return interArrivalTimes.get( operatorId );
    }

    public long getInterArrivalTimeMean ( final String operatorId )
    {
        return getInterArrivalTime( operatorId ).mean;
    }

    public long getInterArrivalTimeStdDev ( final String operatorId )
    {
        return getInterArrivalTime( operatorId ).stdDev;
    }

    public long getInterArrivalTimeVar ( final String operatorId )
    {
        final long stdDev = getInterArrivalTimeStdDev( operatorId );
        return stdDev * stdDev;
    }

    @Override
    public String toString ()
    {
        return "LatencyMetrics{" + "sinkOperatorId='" + sinkOperatorId + '\'' + ", replicaIndex=" + replicaIndex + ", flowVersion="
               + flowVersion + ", tupleLatency=" + tupleLatency + ", serviceTimes=" + serviceTimes + ", queueWaitingTimes="
               + queueWaitingTimes + ", interArrivalTimes=" + interArrivalTimes + '}';
    }

    public static class LatencyRec
    {

        private final long mean;

        private final long stdDev;

        private final long median;

        private final long min;

        private final long max;

        private final long percentile75;

        private final long percentile95;

        private final long percentile99;

        public LatencyRec ( final long mean,
                            final long stdDev,
                            final long median,
                            final long min,
                            final long max,
                            final long percentile75,
                            final long percentile95,
                            final long percentile99 )
        {
            this.mean = mean;
            this.stdDev = stdDev;
            this.median = median;
            this.min = min;
            this.max = max;
            this.percentile75 = percentile75;
            this.percentile95 = percentile95;
            this.percentile99 = percentile99;
        }

        public long getMean ()
        {
            return mean;
        }

        public long getStdDev ()
        {
            return stdDev;
        }

        public long getMedian ()
        {
            return median;
        }

        public long getMin ()
        {
            return min;
        }

        public long getMax ()
        {
            return max;
        }

        public long getPercentile75 ()
        {
            return percentile75;
        }

        public long getPercentile95 ()
        {
            return percentile95;
        }

        public long getPercentile99 ()
        {
            return percentile99;
        }

        @Override
        public String toString ()
        {
            return "LatencyRec{" + "mean=" + mean + ", stdDev=" + stdDev + ", median=" + median + ", min=" + min + ", max=" + max
                   + ", percentile75=" + percentile75 + ", percentile95=" + percentile95 + ", percentile99=" + percentile99 + '}';
        }
    }

}
