package cs.bilkent.joker.engine.config;

import com.typesafe.config.Config;

public class MetricManagerConfig
{

    public static final String CONFIG_NAME = "metricManager";

    public static final String TICK_MASK = "tickMask";

    public static final String WARMUP_ITERATIONS = "warmupIterations";

    public static final String PIPELINE_METRICS_SCANNING_PERIOD_IN_MILLIS = "pipelineMetricsScanningPeriodInMillis";

    public static final String OPERATOR_INVOCATION_SAMPLING_IN_MICROS = "operatorInvocationSamplingPeriodInMicros";


    private final long tickMask;

    private final int warmupIterations;

    private final long pipelineMetricsScanningPeriodInMillis;

    private final long operatorInvocationSamplingPeriodInMicros;

    MetricManagerConfig ( final Config parentConfig )
    {
        final Config config = parentConfig.getConfig( CONFIG_NAME );
        this.tickMask = config.getLong( TICK_MASK );
        this.warmupIterations = config.getInt( WARMUP_ITERATIONS );
        this.pipelineMetricsScanningPeriodInMillis = config.getLong( PIPELINE_METRICS_SCANNING_PERIOD_IN_MILLIS );
        this.operatorInvocationSamplingPeriodInMicros = config.getLong( OPERATOR_INVOCATION_SAMPLING_IN_MICROS );
    }

    public long getTickMask ()
    {
        return tickMask;
    }

    public int getWarmupIterations ()
    {
        return warmupIterations;
    }

    public long getPipelineMetricsScanningPeriodInMillis ()
    {
        return pipelineMetricsScanningPeriodInMillis;
    }

    public long getOperatorInvocationSamplingPeriodInMicros ()
    {
        return operatorInvocationSamplingPeriodInMicros;
    }

}
