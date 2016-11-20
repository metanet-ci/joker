package cs.bilkent.joker.engine.config;

import com.typesafe.config.Config;

public class PipelineReplicaRunnerConfig
{

    public static final String CONFIG_NAME = "pipelineReplicaRunner";

    public static final String RUNNER_WAIT_TIMEOUT = "runnerWaitTimeoutInMillis";


    private final long waitTimeoutInMillis;

    PipelineReplicaRunnerConfig ( final Config parentConfig )
    {
        final Config config = parentConfig.getConfig( CONFIG_NAME );
        this.waitTimeoutInMillis = config.getLong( RUNNER_WAIT_TIMEOUT );
    }

    public long getWaitTimeoutInMillis ()
    {
        return waitTimeoutInMillis;
    }

}
