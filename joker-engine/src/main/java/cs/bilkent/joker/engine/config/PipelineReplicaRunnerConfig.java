package cs.bilkent.joker.engine.config;

import com.typesafe.config.Config;

public class PipelineReplicaRunnerConfig
{

    static final String CONFIG_NAME = "pipelineReplicaRunner";

    static final String RUNNER_WAIT_TIMEOUT = "runnerWaitTimeoutInMillis";

    static final String ENFORCE_THREAD_AFFINITY = "enforceThreadAffinity";


    private final long runnerWaitTimeoutInMillis;

    private final boolean enforceThreadAffinity;

    PipelineReplicaRunnerConfig ( final Config parentConfig )
    {
        final Config config = parentConfig.getConfig( CONFIG_NAME );
        this.runnerWaitTimeoutInMillis = config.getLong( RUNNER_WAIT_TIMEOUT );
        this.enforceThreadAffinity = config.getBoolean( ENFORCE_THREAD_AFFINITY );
    }

    public long getRunnerWaitTimeoutInMillis ()
    {
        return runnerWaitTimeoutInMillis;
    }

    public boolean shouldEnforceThreadAffinity ()
    {
        return enforceThreadAffinity;
    }

    @Override
    public String toString ()
    {
        return "PipelineReplicaRunnerConfig{" + "runnerWaitTimeoutInMillis=" + runnerWaitTimeoutInMillis + ", enforceThreadAffinity="
               + enforceThreadAffinity + '}';
    }

}
