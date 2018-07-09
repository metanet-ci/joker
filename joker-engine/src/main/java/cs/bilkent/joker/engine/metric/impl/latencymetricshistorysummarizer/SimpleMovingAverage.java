package cs.bilkent.joker.engine.metric.impl.latencymetricshistorysummarizer;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

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
        final LatencyRecord avgTupleLatency = avg( metrics.stream().map( LatencyMetrics::getTupleLatency ), metrics.size() );

        final Map<String, LatencyRecord> avgInvocationLatencies = metrics.get( 0 )
                                                                         .getInvocationLatencies()
                                                                         .keySet()
                                                                         .stream()
                                                                         .map( operatorId -> {
                                                                             final Stream<LatencyRecord> recs = metrics.stream()
                                                                                                                       .map( m -> m.getInvocationLatency(
                                                                                                                               operatorId
                                                                                                                       ) );
                                                                             return Pair.of( operatorId, avg( recs, metrics.size() ) );
                                                                         } )
                                                                         .collect( toMap( p -> p._1, p -> p._2 ) );

        final Map<String, LatencyRecord> avgQueueLatencies = metrics.get( 0 ).getQueueLatencies().keySet().stream().map( operatorId -> {
            final Stream<LatencyRecord> recs = metrics.stream().map( m -> m.getQueueLatency( operatorId ) );
            return Pair.of( operatorId, avg( recs, metrics.size() ) );
        } ).collect( toMap( p -> p._1, p -> p._2 ) );

        return new LatencyMetrics( metrics.get( 0 ).getSinkOperatorId(),
                                   metrics.get( 0 ).getReplicaIndex(),
                                   metrics.get( 0 ).getFlowVersion(),
                                   avgTupleLatency,
                                   avgInvocationLatencies,
                                   avgQueueLatencies );
    }

    private LatencyRecord avg ( final Stream<LatencyRecord> stream, final int count )
    {
        final LatencyRecord sum = stream.reduce( new LatencyRecord( 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 ),
                                                 ( r1, r2 ) -> new LatencyRecord( r1.getMean() + r2.getMean(),
                                                                                  0,
                                                                                  0,
                                                                                  0,
                                                                                  0,
                                                                                  0,
                                                                                  0,
                                                                                  0,
                                                                                  0,
                                                                                  0 ) );

        return new LatencyRecord( (long) ceil( ( (double) sum.getMean() ) / count ), 0, 0, 0, 0, 0, 0, 0, 0, 0 );
    }
}
