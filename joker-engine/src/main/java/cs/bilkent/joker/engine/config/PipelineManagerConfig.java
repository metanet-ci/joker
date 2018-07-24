package cs.bilkent.joker.engine.config;

import com.typesafe.config.Config;

public class PipelineManagerConfig
{

    static final String CONFIG_NAME = "pipelineManager";

    static final String RUNNER_COMMAND_TIMEOUT = "runnerCommandTimeoutInMillis";

    static final String LATENCY_TISK_MASK = "latencyTickMask";

    static final String LATENCY_COMPONENT_TISK_MASK = "latencyComponentTickMask";


    private final long runnerCommandTimeoutInMillis;

    private final long latencyTickMask;

    private final long latencyComponentTickMask;


    PipelineManagerConfig ( final Config parentConfig )
    {
        final Config config = parentConfig.getConfig( CONFIG_NAME );
        this.runnerCommandTimeoutInMillis = config.getLong( RUNNER_COMMAND_TIMEOUT );
        this.latencyTickMask = config.getLong( LATENCY_TISK_MASK );
        this.latencyComponentTickMask = config.getLong( LATENCY_COMPONENT_TISK_MASK );
    }

    public long getRunnerCommandTimeoutInMillis ()
    {
        return runnerCommandTimeoutInMillis;
    }

    public long getLatencyTickMask ()
    {
        return latencyTickMask;
    }

    public long getLatencyComponentTickMask ()
    {
        return latencyComponentTickMask;
    }

    @Override
    public String toString ()
    {
        return "PipelineManagerConfig{" + "runnerCommandTimeoutInMillis=" + runnerCommandTimeoutInMillis + ", latencyTickMask="
               + latencyTickMask + ", latencyComponentTickMask=" + latencyComponentTickMask + '}';
    }
}
