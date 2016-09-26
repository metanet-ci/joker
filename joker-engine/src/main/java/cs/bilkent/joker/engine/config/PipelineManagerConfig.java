package cs.bilkent.joker.engine.config;

import com.typesafe.config.Config;

public class PipelineManagerConfig
{

    static final String CONFIG_NAME = "pipelineManager";

    static final String RUNNER_STOP_TIMEOUT = "runnerStopTimeoutInMillis";


    private final long runnerStopTimeoutInMillis;

    PipelineManagerConfig ( final Config parentConfig )
    {
        final Config config = parentConfig.getConfig( CONFIG_NAME );
        this.runnerStopTimeoutInMillis = config.getLong( RUNNER_STOP_TIMEOUT );
    }

    public long getRunnerStopTimeoutInMillis ()
    {
        return runnerStopTimeoutInMillis;
    }

}
