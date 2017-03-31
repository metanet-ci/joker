package cs.bilkent.joker.engine.config;

import java.io.File;

import com.typesafe.config.Config;

public class MetricManagerConfig
{

    public static final String CONFIG_NAME = "metricManager";

    public static final String TICK_MASK = "tickMask";

    public static final String WARMUP_ITERATIONS = "warmupIterations";

    public static final String PIPELINE_METRICS_SCANNING_PERIOD_IN_MILLIS = "pipelineMetricsScanningPeriodInMillis";

    public static final String OPERATOR_INVOCATION_SAMPLING_IN_MICROS = "operatorInvocationSamplingPeriodInMicros";

    public static final String HISTORY_SIZE = "historySize";

    public static final String PERIOD_SKEW_TOLERANCE_RATIO = "periodSkewToleranceRatio";

    public static final String CSV_REPORT_ENABLED = "csvReportEnabled";

    public static final String CSV_REPORT_PERIOD_IN_MILLIS = "csvReportPeriodInMillis";

    public static final String CSV_REPORT_BASE_DIR = "csvReportBaseDir";


    private final long tickMask;

    private final int warmupIterations;

    private final long pipelineMetricsScanningPeriodInMillis;

    private final long operatorInvocationSamplingPeriodInMicros;

    private final int historySize;

    private final double periodSkewToleranceRatio;

    private final boolean csvReportEnabled;

    private final long csvReportPeriodInMillis;

    private final String csvReportBaseDir;

    MetricManagerConfig ( final Config parentConfig )
    {
        final Config config = parentConfig.getConfig( CONFIG_NAME );
        this.tickMask = config.getLong( TICK_MASK );
        this.warmupIterations = config.getInt( WARMUP_ITERATIONS );
        this.pipelineMetricsScanningPeriodInMillis = config.getLong( PIPELINE_METRICS_SCANNING_PERIOD_IN_MILLIS );
        this.operatorInvocationSamplingPeriodInMicros = config.getLong( OPERATOR_INVOCATION_SAMPLING_IN_MICROS );
        this.historySize = config.getInt( HISTORY_SIZE );
        this.periodSkewToleranceRatio = config.getDouble( PERIOD_SKEW_TOLERANCE_RATIO );
        this.csvReportEnabled = config.getBoolean( CSV_REPORT_ENABLED );
        this.csvReportPeriodInMillis = config.getLong( CSV_REPORT_PERIOD_IN_MILLIS );
        final String csvReportBaseDir = config.getString( CSV_REPORT_BASE_DIR );
        this.csvReportBaseDir = csvReportBaseDir.endsWith( File.separator ) ? csvReportBaseDir : csvReportBaseDir + File.separator;
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

    public int getHistorySize ()
    {
        return historySize;
    }

    public double getPeriodSkewToleranceRatio ()
    {
        return periodSkewToleranceRatio;
    }

    public boolean isCsvReportEnabled ()
    {
        return csvReportEnabled;
    }

    public long getCsvReportPeriodInMillis ()
    {
        return csvReportPeriodInMillis;
    }

    public String getCsvReportBaseDir ()
    {
        return csvReportBaseDir;
    }

}
