package cs.bilkent.joker.engine.config;

import com.typesafe.config.Config;

public class PipelineReplicaRunnerConfig
{

    static final String CONFIG_NAME = "pipelineReplicaRunner";

    static final String RUNNER_WAIT_TIME_IN_MILLIS = "runnerWaitTimeInMillis";


    public final long waitTimeoutInMillis;

    PipelineReplicaRunnerConfig ( Config parentConfig )
    {
        final Config config = parentConfig.getConfig( CONFIG_NAME );
        waitTimeoutInMillis = config.getLong( RUNNER_WAIT_TIME_IN_MILLIS );
    }

    public long getWaitTimeoutInMillis ()
    {
        return waitTimeoutInMillis;
    }

}
