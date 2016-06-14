package cs.bilkent.zanza.engine.config;

import com.typesafe.config.Config;

public class PipelineInstanceRunnerConfig
{

    static final String CONFIG_NAME = "pipelineInstanceRunner";

    static final String RUNNER_WAIT_TIME_IN_MILLIS = "runnerWaitTimeInMillis";


    public final long waitTimeoutInMillis;

    PipelineInstanceRunnerConfig ( Config parentConfig )
    {
        final Config config = parentConfig.getConfig( CONFIG_NAME );
        waitTimeoutInMillis = config.getLong( RUNNER_WAIT_TIME_IN_MILLIS );
    }

    public long getWaitTimeoutInMillis ()
    {
        return waitTimeoutInMillis;
    }

}
