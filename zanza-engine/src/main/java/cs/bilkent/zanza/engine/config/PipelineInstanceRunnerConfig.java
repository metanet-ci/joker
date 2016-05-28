package cs.bilkent.zanza.engine.config;

import com.typesafe.config.Config;

public class PipelineInstanceRunnerConfig
{

    public static final String CONFIG_NAME = "pipelineInstanceRunner";

    public static final String RUNNER_WAIT_TIME_IN_MILLIS = "runnerWaitTimeInMillis";


    public final long waitTimeoutInMillis;

    public PipelineInstanceRunnerConfig ( Config parentConfig )
    {
        final Config config = parentConfig.getConfig( CONFIG_NAME );
        waitTimeoutInMillis = config.getLong( RUNNER_WAIT_TIME_IN_MILLIS );
    }

    public long getWaitTimeoutInMillis ()
    {
        return waitTimeoutInMillis;
    }

}
