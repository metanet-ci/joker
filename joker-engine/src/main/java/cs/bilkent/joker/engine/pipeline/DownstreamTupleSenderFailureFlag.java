package cs.bilkent.joker.engine.pipeline;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Singleton;

@Singleton
@ThreadSafe
public class DownstreamTupleSenderFailureFlag
{

    private volatile boolean failed;

    public DownstreamTupleSenderFailureFlag ()
    {
    }

    public boolean isFailed ()
    {
        return failed;
    }

    public void setFailed ()
    {
        failed = true;
    }

}
