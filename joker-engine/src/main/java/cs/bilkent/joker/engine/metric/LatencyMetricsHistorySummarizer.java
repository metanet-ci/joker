package cs.bilkent.joker.engine.metric;

import java.util.List;

@FunctionalInterface
public interface LatencyMetricsHistorySummarizer
{

    LatencyMetrics summarize ( List<LatencyMetricsHistory> histories );

}
