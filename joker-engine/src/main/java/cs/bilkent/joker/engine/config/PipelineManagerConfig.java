package cs.bilkent.joker.engine.config;

import com.typesafe.config.Config;

public class PipelineManagerConfig
{

    static final String CONFIG_NAME = "pipelineManager";

    static final String RUNNER_COMMAND_TIMEOUT = "runnerCommandTimeoutInMillis";


    private final long runnerCommandTimeoutInMillis;


    PipelineManagerConfig ( final Config parentConfig )
    {
        final Config config = parentConfig.getConfig( CONFIG_NAME );
        this.runnerCommandTimeoutInMillis = config.getLong( RUNNER_COMMAND_TIMEOUT );
    }

    public long getRunnerCommandTimeoutInMillis ()
    {
        return runnerCommandTimeoutInMillis;
    }

    @Override
    public String toString ()
    {
        return "PipelineManagerConfig{" + "runnerCommandTimeoutInMillis=" + runnerCommandTimeoutInMillis + '}';
    }

}
