package cs.bilkent.joker.engine.config;

import com.typesafe.config.Config;

public class PipelineReplicaRunnerConfig
{

    static final String CONFIG_NAME = "pipelineReplicaRunner";

    static final String RUNNER_WAIT_TIMEOUT = "runnerWaitTimeoutInMillis";


    private final long waitTimeoutInMillis;


    PipelineReplicaRunnerConfig ( Config parentConfig )
    {
        final Config config = parentConfig.getConfig( CONFIG_NAME );
        this.waitTimeoutInMillis = config.getLong( RUNNER_WAIT_TIMEOUT );
    }

    public long getWaitTimeoutInMillis ()
    {
        return waitTimeoutInMillis;
    }

}
