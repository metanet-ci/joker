package cs.bilkent.joker.engine.config;

import com.typesafe.config.Config;

public class PipelineManagerConfig
{

    static final String CONFIG_NAME = "pipelineManager";

    static final String RUNNER_COMMAND_TIMEOUT = "runnerCommandTimeoutInMillis";

    static final String LATENCY_RECORDER_POOL_SIZE = "latencyRecorderPoolSize";


    private final long runnerCommandTimeoutInMillis;

    private final int latencyRecorderPoolSize;

    PipelineManagerConfig ( final Config parentConfig )
    {
        final Config config = parentConfig.getConfig( CONFIG_NAME );
        this.runnerCommandTimeoutInMillis = config.getLong( RUNNER_COMMAND_TIMEOUT );
        this.latencyRecorderPoolSize = config.getInt( LATENCY_RECORDER_POOL_SIZE );
    }

    public long getRunnerCommandTimeoutInMillis ()
    {
        return runnerCommandTimeoutInMillis;
    }

    public int getLatencyRecorderPoolSize ()
    {
        return latencyRecorderPoolSize;
    }

    @Override
    public String toString ()
    {
        return "PipelineManagerConfig{" + "runnerCommandTimeoutInMillis=" + runnerCommandTimeoutInMillis + ", latencyRecorderPoolSize="
               + latencyRecorderPoolSize + '}';
    }

}
