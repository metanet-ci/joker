package cs.bilkent.joker.engine.config;

import java.util.concurrent.TimeUnit;

import com.typesafe.config.Config;

public class PipelineReplicaRunnerConfig
{

    static final String CONFIG_NAME = "pipelineReplicaRunner";

    static final String RUNNER_WAIT_TIMEOUT = "runnerWaitTimeout";

    static final String RUNNER_WAIT_TIMEOUT_UNIT = "runnerWaitTimeoutUnit";


    private final long waitTimeout;

    private final TimeUnit waitTimeoutUnit;

    PipelineReplicaRunnerConfig ( Config parentConfig )
    {
        final Config config = parentConfig.getConfig( CONFIG_NAME );
        this.waitTimeout = config.getLong( RUNNER_WAIT_TIMEOUT );
        this.waitTimeoutUnit = TimeUnit.valueOf( config.getString( RUNNER_WAIT_TIMEOUT_UNIT ) );
    }

    public long getWaitTimeout ()
    {
        return waitTimeout;
    }

    public TimeUnit getWaitTimeoutUnit ()
    {
        return waitTimeoutUnit;
    }

}
