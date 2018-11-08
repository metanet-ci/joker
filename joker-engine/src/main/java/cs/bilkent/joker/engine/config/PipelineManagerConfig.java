package cs.bilkent.joker.engine.config;

import com.typesafe.config.Config;

public class PipelineManagerConfig
{

    static final String CONFIG_NAME = "pipelineManager";

    static final String RUNNER_COMMAND_TIMEOUT = "runnerCommandTimeoutInMillis";

    static final String LATENCY_TICK_MASK = "latencyTickMask";

    static final String LATENCY_STAGE_TICK_MASK = "latencyStageTickMask";


    private final long runnerCommandTimeoutInMillis;

    private final long latencyTickMask;

    private final long latencyStageTickMask;


    PipelineManagerConfig ( final Config parentConfig )
    {
        final Config config = parentConfig.getConfig( CONFIG_NAME );
        this.runnerCommandTimeoutInMillis = config.getLong( RUNNER_COMMAND_TIMEOUT );
        this.latencyTickMask = config.getLong( LATENCY_TICK_MASK );
        this.latencyStageTickMask = config.getLong( LATENCY_STAGE_TICK_MASK );
    }

    public long getRunnerCommandTimeoutInMillis ()
    {
        return runnerCommandTimeoutInMillis;
    }

    public long getLatencyTickMask ()
    {
        return latencyTickMask;
    }

    public long getLatencyStageTickMask ()
    {
        return latencyStageTickMask;
    }

    @Override
    public String toString ()
    {
        return "PipelineManagerConfig{" + "runnerCommandTimeoutInMillis=" + runnerCommandTimeoutInMillis + ", latencyTickMask="
               + latencyTickMask + ", latencyStageTickMask=" + latencyStageTickMask + '}';
    }
}
