package cs.bilkent.joker.engine.metric.impl.latencymetricshistorysummarizer;

import java.util.List;
import java.util.Map;

import cs.bilkent.joker.engine.metric.LatencyMetrics;
import cs.bilkent.joker.engine.metric.LatencyMetrics.LatencyRecord;
import cs.bilkent.joker.engine.metric.LatencyMetricsHistory;
import cs.bilkent.joker.engine.metric.LatencyMetricsHistorySummarizer;
import cs.bilkent.joker.operator.utils.Pair;
import static java.lang.Math.ceil;
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
        final double avgTupleLat = metrics.stream()
                                          .map( LatencyMetrics::getTupleLatency )
                                          .mapToLong( LatencyRecord::getMean )
                                          .average()
                                          .orElse( 0 );

        final Map<String, LatencyRecord> avgInvocationLats = metrics.get( 0 )
                                                                    .getInvocationLatencies()
                                                                    .keySet()
                                                                    .stream()
                                                                    .map( operatorId -> {

                                                                        final double avg = metrics.stream()
                                                                                                  .map( m -> m.getInvocationLatency(
                                                                                                          operatorId ) )
                                                                                                  .mapToLong( LatencyRecord::getMean )
                                                                                                  .average()
                                                                                                  .orElse( 0 );

                                                                        return Pair.of( operatorId,
                                                                                        newLatencyRecord( (long) ceil( avg ) ) );
                                                                    } )
                                                                    .collect( toMap( p -> p._1, p -> p._2 ) );

        final Map<String, LatencyRecord> avgQueueLats = metrics.get( 0 ).getQueueLatencies().keySet().stream().map( operatorId -> {

            final double avg = metrics.stream()
                                      .map( m -> m.getQueueLatency( operatorId ) )
                                      .mapToLong( LatencyRecord::getMean )
                                      .average()
                                      .orElse( 0 );

            return Pair.of( operatorId, newLatencyRecord( (long) ceil( avg ) ) );
        } ).collect( toMap( p -> p._1, p -> p._2 ) );

        final Map<String, LatencyRecord> avgInterArvTimes = metrics.get( 0 ).getInterArrivalTimes().keySet().stream().map( operatorId -> {

            final double avg = metrics.stream()
                                      .map( m -> m.getInterArrivalTime( operatorId ) )
                                      .mapToLong( LatencyRecord::getMean )
                                      .average()
                                      .orElse( 0 );

            return Pair.of( operatorId, newLatencyRecord( (long) ceil( avg ) ) );
        } ).collect( toMap( p -> p._1, p -> p._2 ) );

        return new LatencyMetrics( metrics.get( 0 ).getSinkOperatorId(),
                                   metrics.get( 0 ).getReplicaIndex(),
                                   metrics.get( 0 ).getFlowVersion(),
                                   newLatencyRecord( (long) ceil( avgTupleLat ) ),
                                   avgInvocationLats,
                                   avgQueueLats,
                                   avgInterArvTimes );
    }

    private LatencyRecord newLatencyRecord ( long mean )
    {
        return new LatencyRecord( mean, 0, 0, 0, 0, 0, 0, 0 );
    }

}
