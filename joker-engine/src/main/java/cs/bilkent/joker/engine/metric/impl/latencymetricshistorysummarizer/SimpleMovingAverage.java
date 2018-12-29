package cs.bilkent.joker.engine.metric.impl.latencymetricshistorysummarizer;

import java.util.List;
import java.util.Map;

import cs.bilkent.joker.engine.metric.LatencyMetrics;
import cs.bilkent.joker.engine.metric.LatencyMetrics.LatencyRec;
import cs.bilkent.joker.engine.metric.LatencyMetricsHistory;
import cs.bilkent.joker.engine.metric.LatencyMetricsHistorySummarizer;
import cs.bilkent.joker.operator.utils.Pair;
import static java.lang.Math.ceil;
import static java.lang.Math.sqrt;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

public class SimpleMovingAverage implements LatencyMetricsHistorySummarizer
{
    @Override
    public LatencyMetrics summarize ( final List<LatencyMetricsHistory> histories )
    {
        final List<LatencyMetrics> l = histories.stream().map( LatencyMetricsHistory::getAll ).map( this::avg ).collect( toList() );
        return avg( l );
    }

    private LatencyMetrics avg ( final List<LatencyMetrics> metrics )
    {
        final double avgTupleLat = metrics.stream().mapToLong( LatencyMetrics::getTupleLatencyMean ).average().orElse( 0 );

        final double avgTupleLatVariance = metrics.stream().mapToLong( LatencyMetrics::getTupleLatencyVariance ).average().orElse( 0 );

        final Map<String, LatencyRec> avgServiceTimes = metrics.get( 0 ).getServiceTimes().keySet().stream().map( operatorId -> {

            final double avgMean = metrics.stream().mapToLong( m -> m.getServiceTimeMean( operatorId ) ).average().orElse( 0 );

            final double avgVariance = metrics.stream().mapToLong( m -> m.getServiceTimeVar( operatorId ) ).average().orElse( 0 );

            return Pair.of( operatorId, newLatencyRec( (long) ceil( avgMean ), (long) sqrt( avgVariance ) ) );
        } ).collect( toMap( p -> p._1, p -> p._2 ) );

        final Map<String, LatencyRec> avgQueueWaits = metrics.get( 0 ).getQueueWaitingTimes().keySet().stream().map( operatorId -> {

            final double avgMean = metrics.stream().mapToLong( m -> m.getQueueWaitingTimeMean( operatorId ) ).average().orElse( 0 );

            final double avgVariance = metrics.stream().mapToLong( m -> m.getQueueWaitingTimeVar( operatorId ) ).average().orElse( 0 );

            return Pair.of( operatorId, newLatencyRec( (long) ceil( avgMean ), (long) sqrt( avgVariance ) ) );
        } ).collect( toMap( p -> p._1, p -> p._2 ) );

        final Map<String, LatencyRec> avgInterArvTimes = metrics.get( 0 ).getInterArrivalTimes().keySet().stream().map( operatorId -> {

            final double avgMean = metrics.stream().mapToLong( m -> m.getInterArrivalTimeMean( operatorId ) ).average().orElse( 0 );

            final double avgVariance = metrics.stream().mapToLong( m -> m.getInterArrivalTimeVar( operatorId ) ).average().orElse( 0 );

            return Pair.of( operatorId, newLatencyRec( (long) ceil( avgMean ), (long) sqrt( avgVariance ) ) );
        } ).collect( toMap( p -> p._1, p -> p._2 ) );

        return new LatencyMetrics( metrics.get( 0 ).getSinkOperatorId(),
                                   metrics.get( 0 ).getReplicaIndex(),
                                   metrics.get( 0 ).getFlowVersion(),
                                   newLatencyRec( (long) ceil( avgTupleLat ), (long) sqrt( avgTupleLatVariance ) ),
                                   avgServiceTimes,
                                   avgQueueWaits,
                                   avgInterArvTimes );
    }

    private LatencyRec newLatencyRec ( final long mean, final long stdDev )
    {
        return new LatencyRec( mean, stdDev, 0, 0, 0, 0, 0, 0 );
    }

}
